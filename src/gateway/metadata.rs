//! Exact bundled-driver SQL shapes over shared source definitions. Parameters
//! can only replace designated string predicates; client SQL is never executed
//! by these handlers, including strings containing SQL or catalog names.
mod metabase;
mod redash;
mod types;
use super::FrontendProfile;
use crate::discovery::template::{capture, tokens};
use crate::{
    backend::result::{ColumnType, ExecutionError, ExecutionResult, ResultColumn, ResultEvent},
    catalog::{Column, RelationKind, Snapshot},
    config::GatewaySettings,
};
use futures::{Sink, SinkExt, channel::oneshot};
use sqlparser::{
    dialect::{Dialect, PostgreSqlDialect, RedshiftSqlDialect},
    tokenizer::Token,
};
use std::sync::OnceLock;
use tokio::time::timeout;
use types::FrontendType;

#[derive(Clone, Copy)]
enum Kind {
    Schemas,
    Tables,
    Columns,
    Database,
    MetabaseTables,
    MetabaseColumns,
    RedashColumns,
}
struct Template {
    kind: Kind,
    tokens: Vec<Token>,
}
pub(super) struct Request {
    kind: Kind,
    schema: String,
    table: String,
    column: String,
    schemas: Vec<String>,
    tables: Vec<String>,
}

impl Request {
    /// Capture templates come from JDBC 42.7.12 and Redshift JDBC 2.2.5. A new
    /// query shape requires a reviewed handler and a client-level fixture.
    pub(super) fn recognize(sql: &str, profile: FrontendProfile) -> Option<Self> {
        static PG: OnceLock<Vec<Template>> = OnceLock::new();
        static RS: OnceLock<Vec<Template>> = OnceLock::new();
        let (templates, dialect): (_, &dyn Dialect) = match profile {
            FrontendProfile::Postgres => {
                (PG.get_or_init(|| templates(false)), &PostgreSqlDialect {})
            }
            FrontendProfile::Redshift => {
                (RS.get_or_init(|| templates(true)), &RedshiftSqlDialect {})
            }
        };
        let tokens = tokens(sql, dialect)?;
        for template in templates {
            if let Some(captures) = capture(&tokens, &template.tokens) {
                let scalar = |key| captures.get(key).map(|v| v[0]).unwrap_or("").to_owned();
                let list = |key| {
                    captures
                        .get(key)
                        .into_iter()
                        .flatten()
                        .map(|v| (*v).to_owned())
                        .collect()
                };
                return Some(Self {
                    kind: template.kind,
                    schema: scalar("__YASP_SCHEMA__"),
                    table: scalar("__YASP_TABLE__"),
                    column: scalar("__YASP_COLUMN__"),
                    schemas: list("__YASP_SCHEMAS__"),
                    tables: list("__YASP_TABLES__"),
                });
            }
        }
        None
    }

    pub(super) async fn execute<S>(
        &self,
        snapshot: &Snapshot,
        profile: FrontendProfile,
        output: &mut S,
        settings: &GatewaySettings,
        mut cancel: oneshot::Receiver<()>,
    ) -> ExecutionResult<()>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Send + Unpin,
    {
        let operation = async {
            let columns = self.columns(profile);
            send_event(output, ResultEvent::Columns(columns), settings).await?;
            let mut count = 0;
            match self.kind {
                Kind::MetabaseTables | Kind::MetabaseColumns => {
                    count = metabase::serve(self, snapshot, output, settings).await?;
                }
                Kind::RedashColumns => {
                    count = redash::serve(self, snapshot, output, settings).await?;
                }
                Kind::Schemas => {
                    for (database, schema) in &snapshot.schemas {
                        self.send(output, vec![some(schema), some(database)], settings)
                            .await?;
                        count += 1;
                    }
                }
                Kind::Database => {
                    let databases: std::collections::BTreeSet<_> =
                        snapshot.schemas.iter().map(|(d, _)| d).collect();
                    if databases.len() != 1 {
                        return Err(ExecutionError::rejected(
                            "0A000",
                            "catalog database is unavailable or ambiguous",
                        ));
                    }
                    self.send(output, vec![some(databases.first().unwrap())], settings)
                        .await?;
                    count += 1;
                }
                Kind::Tables | Kind::Columns => {
                    let mut relations: Vec<_> = snapshot.relations.values().collect();
                    if matches!(self.kind, Kind::Tables) {
                        relations.sort_by_key(|r| {
                            (
                                matches!(r.kind, RelationKind::View),
                                &r.id.schema,
                                &r.id.name,
                            )
                        });
                    }
                    for relation in relations {
                        if !like(&relation.id.schema, &self.schema)
                            || !like(&relation.id.name, &self.table)
                        {
                            continue;
                        }
                        if matches!(self.kind, Kind::Tables) {
                            let kind = match relation.kind {
                                RelationKind::Table => "TABLE",
                                RelationKind::View => "VIEW",
                                _ => continue,
                            };
                            self.send(
                                output,
                                vec![
                                    some(&relation.id.database),
                                    some(&relation.id.schema),
                                    some(&relation.id.name),
                                    some(kind),
                                    relation.comment.clone(),
                                    some(""),
                                    some(""),
                                    some(""),
                                    some(""),
                                    some(""),
                                ],
                                settings,
                            )
                            .await?;
                            count += 1;
                        } else {
                            for (index, column) in relation.columns.iter().enumerate() {
                                if !like(&column.id.name, &self.column) {
                                    continue;
                                }
                                let row = column_row(column, profile, index + 1)?;
                                self.send(output, row, settings).await?;
                                count += 1;
                            }
                        }
                    }
                }
            }
            send_event(output, ResultEvent::Complete(count), settings).await
        };
        tokio::select! {
            biased;
            _ = &mut cancel => Err(ExecutionError::cancelled()),
            result = timeout(settings.query_timeout(), operation) => result.unwrap_or_else(|_| Err(ExecutionError::rejected("57014", "metadata response timed out"))),
        }
    }

    async fn send<S>(
        &self,
        output: &mut S,
        row: Vec<Option<String>>,
        settings: &GatewaySettings,
    ) -> ExecutionResult<()>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Send + Unpin,
    {
        let bytes = row.iter().flatten().map(String::len).sum::<usize>() + 7 + row.len() * 4;
        if bytes > settings.max_message_bytes {
            return Err(ExecutionError::rejected(
                "54000",
                "metadata row exceeds frame bound",
            ));
        }
        send_event(
            output,
            ResultEvent::Row(row.into_iter().map(|v| v.map(bytes::Bytes::from)).collect()),
            settings,
        )
        .await
    }

    fn columns(&self, profile: FrontendProfile) -> Vec<ResultColumn> {
        let names = match self.kind {
            Kind::MetabaseTables | Kind::MetabaseColumns => metabase::columns(self.kind),
            Kind::RedashColumns => ["table_name", "table_schema", "column_name", "data_type"]
                .into_iter()
                .map(|name| (name, 0))
                .collect(),
            Kind::Schemas => vec![("TABLE_SCHEM", 0), ("TABLE_CATALOG", 0)],
            Kind::Database => vec![("current_catalog", 0)],
            Kind::Tables => [
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "TABLE_TYPE",
                "REMARKS",
                "TYPE_CAT",
                "TYPE_SCHEM",
                "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION",
            ]
            .into_iter()
            .map(|n| (n, 0))
            .collect(),
            Kind::Columns if profile == FrontendProfile::Postgres => vec![
                ("current_database", 0),
                ("nspname", 0),
                ("relname", 0),
                ("attname", 0),
                ("atttypid", 2),
                ("attnotnull", 3),
                ("atttypmod", 2),
                ("attlen", 1),
                ("typtypmod", 2),
                ("attnum", 4),
                ("attidentity", 0),
                ("attgenerated", 0),
                ("adsrc", 0),
                ("description", 0),
                ("typbasetype", 2),
                ("typtype", 0),
            ],
            Kind::Columns => vec![
                ("TABLE_CAT", 0),
                ("TABLE_SCHEM", 0),
                ("TABLE_NAME", 0),
                ("COLUMN_NAME", 0),
                ("DATA_TYPE", 1),
                ("TYPE_NAME", 0),
                ("COLUMN_SIZE", 2),
                ("BUFFER_LENGTH", 2),
                ("DECIMAL_DIGITS", 2),
                ("NUM_PREC_RADIX", 2),
                ("NULLABLE", 2),
                ("REMARKS", 0),
                ("COLUMN_DEF", 0),
                ("SQL_DATA_TYPE", 1),
                ("SQL_DATETIME_SUB", 1),
                ("CHAR_OCTET_LENGTH", 2),
                ("ORDINAL_POSITION", 2),
                ("IS_NULLABLE", 0),
                ("SCOPE_CATALOG", 0),
                ("SCOPE_SCHEMA", 0),
                ("SCOPE_TABLE", 0),
                ("SOURCE_DATA_TYPE", 2),
                ("IS_AUTOINCREMENT", 0),
                ("IS_GENERATEDCOLUMN", 0),
            ],
        };
        names
            .into_iter()
            .map(|(name, kind)| ResultColumn {
                name: if profile == FrontendProfile::Redshift {
                    name.to_ascii_lowercase()
                } else {
                    name.into()
                },
                kind: match kind {
                    1 => ColumnType::Int16,
                    2 => ColumnType::Int32,
                    3 => ColumnType::Bool,
                    4 => ColumnType::Int64,
                    _ => ColumnType::Text,
                },
            })
            .collect()
    }
}

async fn send_event<S>(
    output: &mut S,
    event: ResultEvent,
    settings: &GatewaySettings,
) -> ExecutionResult<()>
where
    S: Sink<ResultEvent, Error = ExecutionError> + Send + Unpin,
{
    timeout(settings.write_timeout(), output.send(event))
        .await
        .map_err(|_| ExecutionError::rejected("57014", "metadata write timed out"))?
}

fn column_row(
    c: &Column,
    profile: FrontendProfile,
    sequence: usize,
) -> ExecutionResult<Vec<Option<String>>> {
    let t = FrontendType::column(c)?;
    let r = &c.id.relation;
    let prefix = vec![
        some(&r.database),
        some(&r.schema),
        some(&r.name),
        some(&c.id.name),
    ];
    let rest = if profile == FrontendProfile::Postgres {
        vec![
            some(t.oid),
            some(if c.nullable { "f" } else { "t" }),
            some(t.modifier),
            some(t.length),
            some(-1),
            some(sequence),
            c.identity.then(|| "d".into()),
            c.generated.and_then(|g| g.then(|| "s".into())),
            c.default.clone(),
            c.comment.clone(),
            some(0),
            some("b"),
        ]
    } else {
        vec![
            some(t.jdbc),
            some(t.name),
            t.size.map(|v| v.to_string()),
            None,
            t.scale.map(|v| v.to_string()),
            some(10),
            some(if c.nullable { 1 } else { 0 }),
            c.comment.clone(),
            c.default.clone(),
            some(t.jdbc),
            None,
            None, // Character count is not a byte limit; this provider does not retain octet length.
            some(c.ordinal),
            some(if c.nullable { "YES" } else { "NO" }),
            None,
            None,
            None,
            None,
            some(
                if c.identity
                    || c.default
                        .as_deref()
                        .is_some_and(|v| v.starts_with("nextval("))
                {
                    "YES"
                } else {
                    "NO"
                },
            ),
            c.generated
                .map(|g| if g { "YES".into() } else { "NO".into() }),
        ]
    };
    Ok(prefix.into_iter().chain(rest).collect())
}
fn some(value: impl ToString) -> Option<String> {
    Some(value.to_string())
}

fn templates(redshift: bool) -> Vec<Template> {
    let (dialect, queries): (&dyn Dialect, [&str; 3]) = if redshift {
        (
            &RedshiftSqlDialect {},
            [
                include_str!("metadata/queries/redshift_schemas.sql"),
                include_str!("metadata/queries/redshift_tables.sql"),
                include_str!("metadata/queries/redshift_columns.sql"),
            ],
        )
    } else {
        (
            &PostgreSqlDialect {},
            [
                include_str!("metadata/queries/postgresql_schemas.sql"),
                include_str!("metadata/queries/postgresql_tables.sql"),
                include_str!("metadata/queries/postgresql_columns.sql"),
            ],
        )
    };
    let queries = [Kind::Schemas, Kind::Tables, Kind::Columns]
        .into_iter()
        .zip(queries)
        .chain([(Kind::Database, "select current_catalog")])
        .chain(if redshift {
            metabase::templates()
                .into_iter()
                .chain([(
                    Kind::RedashColumns,
                    include_str!("metadata/queries/redash_redshift_columns.sql"),
                )])
                .collect()
        } else {
            Vec::new()
        });
    queries
        .map(|(kind, sql)| Template {
            kind,
            tokens: tokens(sql, dialect).expect("captured metadata SQL"),
        })
        .collect()
}

/// SQL LIKE with backslash escapes, matching source case and Unicode characters.
fn like(value: &str, pattern: &str) -> bool {
    let value: Vec<_> = value.chars().collect();
    let mut matches = vec![false; value.len() + 1];
    matches[0] = true;
    let mut pattern = pattern.chars();
    while let Some(c) = pattern.next() {
        if c == '%' {
            for i in 1..matches.len() {
                matches[i] |= matches[i - 1];
            }
            continue;
        }
        let (c, wildcard) = if c == '\\' {
            let Some(c) = pattern.next() else {
                return false;
            };
            (c, false)
        } else {
            (c, c == '_')
        };
        for i in (1..matches.len()).rev() {
            matches[i] = matches[i - 1] && (wildcard || value[i - 1] == c);
        }
        matches[0] = false;
    }
    matches[value.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redash_scan_matches_only_the_captured_metadata_statement() {
        let sql = include_str!("metadata/queries/redash_redshift_columns.sql");
        assert!(Request::recognize(sql, FrontendProfile::Redshift).is_some());
        assert!(Request::recognize(sql, FrontendProfile::Postgres).is_none());
        for altered in [
            format!("{sql}; SELECT * FROM public.users"),
            format!("{sql} UNION ALL SELECT * FROM public.users"),
            sql.replace("svv_columns", "public.users"),
            sql.replace("SVV_EXTERNAL_SCHEMAS", "public.users"),
            sql.replace("'USAGE'", "(SELECT secret FROM public.users)"),
        ] {
            assert!(Request::recognize(&altered, FrontendProfile::Redshift).is_none());
        }
    }
    #[test]
    fn metabase_redshift_sync_matches_complete_queries_and_literal_lists_only() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/metabase_redshift.json"))
                .unwrap();
        for query in fixture["queries"].as_array().unwrap() {
            let sql = query["sql"].as_str().unwrap();
            assert!(
                Request::recognize(sql, FrontendProfile::Redshift).is_some(),
                "{sql}"
            );
            assert!(Request::recognize(sql, FrontendProfile::Postgres).is_none());
            for altered in [
                format!("{sql}; SELECT 1"),
                format!("{sql} UNION ALL SELECT * FROM public.users"),
                sql.replace("svv_columns", "users")
                    .replace("svv_external_tables", "users"),
            ] {
                assert!(Request::recognize(&altered, FrontendProfile::Redshift).is_none());
            }
            let list = sql.replace(
                "'public'",
                "'public', 'x''; SELECT secret FROM public.users --'",
            );
            assert!(Request::recognize(&list, FrontendProfile::Redshift).is_some());
            let jdbc = sql
                .replace("'public'", "('public')")
                .replace("'users'", "('users')");
            assert!(Request::recognize(&jdbc, FrontendProfile::Redshift).is_some());
            let nested = sql.replace("'public'", "(SELECT secret FROM public.users)");
            if nested != sql {
                assert!(Request::recognize(&nested, FrontendProfile::Redshift).is_none());
            }
        }
        let sql = include_str!("metadata/queries/metabase_redshift_tables_filtered.sql");
        let inconsistent = sql.replacen("'__YASP_SCHEMAS__'", "'other'", 1);
        assert!(Request::recognize(&inconsistent, FrontendProfile::Redshift).is_none());
    }
    #[test]
    fn captured_metadata_shapes_allow_only_literal_filters_and_whole_statements() {
        for (profile, fixture) in [
            (
                FrontendProfile::Postgres,
                include_str!("../../tests/fixtures/jdbc_postgresql.json"),
            ),
            (
                FrontendProfile::Redshift,
                include_str!("../../tests/fixtures/jdbc_redshift.json"),
            ),
        ] {
            let fixture: serde_json::Value = serde_json::from_str(fixture).unwrap();
            for index in [
                1,
                if profile == FrontendProfile::Postgres {
                    2
                } else {
                    3
                },
                if profile == FrontendProfile::Postgres {
                    4
                } else {
                    5
                },
            ] {
                let sql = fixture["queries"][index].as_str().unwrap();
                assert!(Request::recognize(sql, profile).is_some());
                assert!(
                    Request::recognize(&format!("{sql}; SELECT secret FROM public.users"), profile)
                        .is_none()
                );
                if index != 1 {
                    assert!(
                        Request::recognize(
                            &sql.replace("pg_catalog.pg_class", "public.users"),
                            profile
                        )
                        .is_none()
                    );
                    assert!(
                        Request::recognize(
                            &sql.replace("'public'", "(SELECT secret FROM public.users)"),
                            profile
                        )
                        .is_none()
                    );
                    let literal = Request::recognize(
                        &sql.replace("'public'", "'x''; SELECT secret FROM public.users --'"),
                        profile,
                    )
                    .unwrap();
                    assert_eq!(literal.schema, "x'; SELECT secret FROM public.users --");
                }
            }
        }
        let sql = include_str!("metadata/queries/redshift_columns.sql")
            .replace("'__YASP_SCHEMA__'", "''");
        let inconsistent = sql.replacen("schemaname LIKE ''", "schemaname LIKE 'other'", 1);
        assert!(Request::recognize(&inconsistent, FrontendProfile::Redshift).is_none());
        for sql in [
            "SELECT * FROM public.users",
            "SELECT * FROM pg_catalog.pg_class JOIN public.users ON true",
            "WITH x AS (SELECT * FROM public.users) SELECT * FROM x",
        ] {
            assert!(Request::recognize(sql, FrontendProfile::Postgres).is_none());
        }
    }
}
