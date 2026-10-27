//! Captured frontend requests shared by snapshot-backed discovery sources.
use super::*;
use crate::catalog::{ConstraintKind, Relation};

pub(super) struct Request {
    pub(super) kind: Kind,
    pub(super) schema: Option<String>,
    pub(super) table: Option<String>,
    pub(super) schemas: Vec<String>,
    pub(super) oid: Option<u32>,
}
#[derive(Clone, Copy)]
pub(super) enum Kind {
    RedashColumns,
    PrimaryKeys,
    ForeignKeys,
    Indexes,
    MaxIndexKeys,
    Setting(&'static str, &'static str),
    VersionSetting,
    ConnectionCheck,
    Version(FrontendProfile),
    CurrentSchema,
    Schemas,
    Hstore,
    PgSqlAlchemy(super::sqlalchemy::Kind),
    SqlAlchemy(crate::discovery::redshift::Kind),
    Metabase(u8),
}

impl Request {
    pub(super) fn recognize(sql: &str, profile: FrontendProfile) -> Option<Self> {
        let dialect: &dyn Dialect = if profile == FrontendProfile::Postgres {
            &PostgreSqlDialect {}
        } else {
            &RedshiftSqlDialect {}
        };
        let actual = tokens(sql, dialect)?;
        static PG: OnceLock<Vec<(Kind, Vec<Token>)>> = OnceLock::new();
        static RS: OnceLock<Vec<(Kind, Vec<Token>)>> = OnceLock::new();
        let storage = if profile == FrontendProfile::Postgres {
            &PG
        } else {
            &RS
        };
        let templates = storage.get_or_init(|| {
        let jdbc = if profile == FrontendProfile::Postgres {
            [
                include_str!("queries/postgresql_primary_keys.sql"),
                include_str!("queries/postgresql_foreign_keys.sql"),
                include_str!("queries/postgresql_indexes.sql"),
            ]
        } else {
            [
                include_str!("queries/redshift_primary_keys.sql"),
                include_str!("queries/redshift_foreign_keys.sql"),
                include_str!("queries/redshift_indexes.sql"),
            ]
        };
        let candidates = [Kind::PrimaryKeys, Kind::ForeignKeys, Kind::Indexes].into_iter().zip(jdbc)
            .chain([(Kind::ConnectionCheck, "SELECT 1"),
                (Kind::Setting("server_version", "server_version"), "SHOW server_version"),
                (Kind::Setting("server_version_num", "server_version_num"), "SHOW server_version_num"),
                (Kind::Setting("server_version", "current_setting"), "SELECT current_setting('server_version')"),
                (Kind::Setting("server_version_num", "current_setting"), "SELECT current_setting('server_version_num')"),
                (Kind::VersionSetting, "SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'server_version'"),
                (Kind::MaxIndexKeys, "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'"),
                (Kind::Version(profile), "SELECT version()"), (Kind::Version(profile), "SELECT pg_catalog.version()"),
                (Kind::Hstore,"SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore'"),
                (Kind::Hstore,"SELECT t.oid, NULL FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore'"),
                (Kind::CurrentSchema, "SELECT current_schema()"), (Kind::CurrentSchema,"SELECT pg_catalog.current_schema()"),
                (Kind::Schemas,"SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' ORDER BY nspname"),
                (Kind::Schemas,"SELECT nspname FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname != 'information_schema' ORDER BY nspname"),
                (Kind::PgSqlAlchemy(super::sqlalchemy::Kind::Oid),"SELECT '__YASP_RELATION__'::regclass::oid")])
            .chain(super::sqlalchemy::templates())
            .chain(super::metabase::snapshot_templates(profile))
            .chain(if profile == FrontendProfile::Postgres { vec![(Kind::RedashColumns,include_str!("queries/redash_postgresql_columns.sql"))] } else { Vec::new() });
            candidates.map(|(kind,sql)| (kind,tokens(sql,dialect).expect("captured metadata SQL"))).collect()
        });
        for (kind, template) in templates {
            if let Some(values) = capture(&actual, template) {
                let mut schema = values.get("__YASP_SCHEMA__").map(|v| v[0].to_owned());
                let mut table = values.get("__YASP_TABLE__").map(|v| v[0].to_owned());
                if let Some(relation) = values.get("__YASP_RELATION__") {
                    use sqlparser::tokenizer::{Tokenizer, Whitespace};
                    let mut parts = Tokenizer::new(&PostgreSqlDialect {}, relation[0])
                        .tokenize()
                        .ok()?;
                    parts.retain(|t| {
                        !matches!(
                            t,
                            Token::Whitespace(
                                Whitespace::Space | Whitespace::Newline | Whitespace::Tab
                            )
                        )
                    });
                    let [Token::Word(s), Token::Period, Token::Word(t)] = parts.as_slice() else {
                        return None;
                    };
                    let ident = |word: &sqlparser::tokenizer::Word| {
                        if word.quote_style.is_some() {
                            word.value.clone()
                        } else {
                            word.value.to_ascii_lowercase()
                        }
                    };
                    if s.value.is_empty() || t.value.is_empty() {
                        return None;
                    }
                    schema = Some(ident(s));
                    table = Some(ident(t));
                }
                return Some(Self {
                    kind: *kind,
                    oid: values
                        .get("__YASP_OID__")
                        .map(|v| v[0].parse())
                        .transpose()
                        .ok()?,
                    schema,
                    table,
                    schemas: values
                        .get("__YASP_SCHEMAS__")
                        .into_iter()
                        .flatten()
                        .map(|s| (*s).to_owned())
                        .collect(),
                });
            }
        }
        if profile != FrontendProfile::Redshift {
            return None;
        }
        let request = crate::discovery::redshift::Request::recognize_tokens(&actual)?;
        Some(Self {
            kind: Kind::SqlAlchemy(request.kind),
            schema: request.schema,
            table: request.table,
            schemas: Vec::new(),
            oid: None,
        })
    }

    pub(super) fn columns(&self, profile: FrontendProfile) -> Vec<ResultColumn> {
        let names: Vec<(&str, u8)> = match self.kind {
            Kind::Metabase(kind) => super::metabase::postgres_columns(kind),
            Kind::PgSqlAlchemy(kind) => super::sqlalchemy::columns(kind),
            Kind::SqlAlchemy(kind) => sqlalchemy_columns(kind),
            Kind::RedashColumns => vec![
                ("table_schema", 0),
                ("table_name", 0),
                ("column_name", 0),
                ("data_type", 0),
            ],
            Kind::PrimaryKeys => vec![
                ("TABLE_CAT", 0),
                ("TABLE_SCHEM", 0),
                ("TABLE_NAME", 0),
                ("COLUMN_NAME", 0),
                ("KEY_SEQ", 1),
                ("PK_NAME", 0),
            ],
            Kind::ForeignKeys => vec![
                ("PKTABLE_CAT", 0),
                ("PKTABLE_SCHEM", 0),
                ("PKTABLE_NAME", 0),
                ("PKCOLUMN_NAME", 0),
                ("FKTABLE_CAT", 0),
                ("FKTABLE_SCHEM", 0),
                ("FKTABLE_NAME", 0),
                ("FKCOLUMN_NAME", 0),
                ("KEY_SEQ", 1),
                ("UPDATE_RULE", 1),
                ("DELETE_RULE", 1),
                ("FK_NAME", 0),
                ("PK_NAME", 0),
                ("DEFERRABILITY", 1),
            ],
            Kind::Indexes => vec![
                ("TABLE_CAT", 0),
                ("TABLE_SCHEM", 0),
                ("TABLE_NAME", 0),
                ("NON_UNIQUE", 3),
                ("INDEX_QUALIFIER", 0),
                ("INDEX_NAME", 0),
                ("TYPE", 1),
                ("ORDINAL_POSITION", 1),
                ("COLUMN_NAME", 0),
                ("ASC_OR_DESC", 0),
                (
                    "CARDINALITY",
                    if profile == FrontendProfile::Postgres {
                        9
                    } else {
                        4
                    },
                ),
                (
                    "PAGES",
                    if profile == FrontendProfile::Postgres {
                        2
                    } else {
                        4
                    },
                ),
                ("FILTER_CONDITION", 0),
            ],
            Kind::Hstore => vec![("oid", 8), ("typarray", 8)],
            Kind::Setting(_, column) => vec![(column, 0)],
            Kind::VersionSetting => vec![("name", 0), ("setting", 0)],
            Kind::MaxIndexKeys => vec![("setting", 0)],
            Kind::ConnectionCheck => vec![("?column?", 2)],
            Kind::Version(_) => vec![("version", 0)],
            Kind::CurrentSchema => vec![("current_schema", 0)],
            Kind::Schemas => vec![("nspname", 0)],
        };
        let mut names = names;
        if matches!(self.kind, Kind::Indexes) && profile == FrontendProfile::Postgres {
            names.push(("REMARKS", 0));
        }
        names
            .into_iter()
            .map(|(name, kind)| ResultColumn {
                name: if profile == FrontendProfile::Redshift {
                    name.to_ascii_lowercase()
                } else {
                    name.into()
                },
                kind: match kind {
                    5 => ColumnType::Int16Array,
                    6 => ColumnType::Json,
                    7 => ColumnType::TextArray,
                    1 => ColumnType::Int16,
                    2 => ColumnType::Int32,
                    8 => ColumnType::Oid,
                    9 => ColumnType::Float32,
                    3 => ColumnType::Bool,
                    4 => ColumnType::Int64,
                    _ => ColumnType::Text,
                },
            })
            .collect()
    }

    pub(super) async fn serve<S>(
        &self,
        request: &super::Request,
        snapshot: &Snapshot,
        output: &mut S,
        settings: &GatewaySettings,
        profile: FrontendProfile,
        search_path: &str,
    ) -> ExecutionResult<u64>
    where
        S: Sink<ResultEvent, Error = ExecutionError> + Send + Unpin,
    {
        if matches!(self.kind, Kind::CurrentSchema)
            && (snapshot.postgres.is_some()
                || snapshot.source.dialect == crate::query::SqlDialect::Redshift)
        {
            let tokens = sqlparser::tokenizer::Tokenizer::new(&PostgreSqlDialect {}, search_path)
                .tokenize()
                .map_err(|_| ExecutionError::rejected("0A000", "invalid metadata search path"))?;
            let schema = tokens
                .into_iter()
                .filter_map(|token| match token {
                    Token::Word(word) => Some(if word.quote_style.is_some() {
                        word.value
                    } else {
                        word.value.to_ascii_lowercase()
                    }),
                    Token::Placeholder(value) => Some(value),
                    _ => None,
                })
                .map(|name| {
                    if name == "$user" {
                        snapshot.source.principal.clone()
                    } else {
                        name
                    }
                })
                .find(|name| snapshot.schemas.iter().any(|(_, s)| s == name));
            request.send(output, vec![schema], settings).await?;
            return Ok(1);
        }
        if let Some(count) =
            super::postgres::serve(self, request, snapshot, profile, output, settings).await?
        {
            return Ok(count);
        }
        if matches!(self.kind, Kind::RedashColumns) {
            return super::redash::serve_postgres(request, snapshot, output, settings).await;
        }
        if let Kind::Metabase(kind) = self.kind {
            return super::metabase::serve_postgres(
                kind,
                &self.schemas,
                request,
                snapshot,
                output,
                settings,
            )
            .await;
        }
        if let Kind::Setting(name, _) = self.kind {
            let value = match (name, profile) {
                ("server_version", FrontendProfile::Redshift) => "8.0.2",
                ("server_version_num", FrontendProfile::Redshift) => "80002",
                ("server_version", _) => "18.0",
                _ => "180000",
            };
            request.send(output, vec![some(value)], settings).await?;
            return Ok(1);
        }
        if matches!(self.kind, Kind::VersionSetting) {
            request
                .send(output, vec![some("server_version"), some("18.0")], settings)
                .await?;
            return Ok(1);
        }
        if matches!(self.kind, Kind::Hstore) {
            return Ok(0);
        }
        if matches!(self.kind, Kind::ConnectionCheck) {
            // JDBC BI connection checks expect the frontend integer literal.
            // Native charts bypass this handler and keep Snowflake NUMBER types.
            request.send(output, vec![some(1)], settings).await?;
            return Ok(1);
        }
        if let Kind::Version(profile) = self.kind {
            let version = if profile == FrontendProfile::Postgres {
                "PostgreSQL 18.0 (YASP compatibility profile)"
            } else {
                "PostgreSQL 8.0.2, Redshift 1.0 (YASP compatibility profile)"
            };
            request.send(output, vec![some(version)], settings).await?;
            return Ok(1);
        }
        if matches!(self.kind, Kind::CurrentSchema) {
            request
                .send(
                    output,
                    vec![snapshot.source.default_schema.clone()],
                    settings,
                )
                .await?;
            return Ok(1);
        }
        if matches!(self.kind, Kind::Schemas) {
            for (_, schema) in &snapshot.schemas {
                request.send(output, vec![some(schema)], settings).await?;
            }
            return Ok(snapshot.schemas.len() as u64);
        }
        if matches!(self.kind, Kind::MaxIndexKeys) {
            request.send(output, vec![some(32)], settings).await?;
            return Ok(1);
        }
        let mut count = 0;
        for relation in snapshot.relations.values() {
            if self
                .oid
                .is_some_and(|oid| relation.id.frontend_oid() != oid)
            {
                continue;
            }
            if self
                .schema
                .as_ref()
                .is_some_and(|s| relation.id.schema != *s)
                || self.table.as_ref().is_some_and(|s| relation.id.name != *s)
            {
                continue;
            }
            let keys = relation.constraints.as_ref().ok_or_else(|| {
                ExecutionError::rejected("0A000", "catalog key facts are unavailable")
            })?;
            match self.kind {
                Kind::PrimaryKeys | Kind::ForeignKeys => {
                    for key in keys {
                        if (matches!(self.kind, Kind::PrimaryKeys)
                            && key.kind != ConstraintKind::PrimaryKey)
                            || (matches!(self.kind, Kind::ForeignKeys)
                                && key.kind != ConstraintKind::ForeignKey)
                        {
                            continue;
                        }
                        for (index, column) in key.columns.iter().enumerate() {
                            let row = if let Some(reference) = &key.references {
                                vec![
                                    some(&reference.relation.database),
                                    some(&reference.relation.schema),
                                    some(&reference.relation.name),
                                    some(&reference.columns[index]),
                                    some(&relation.id.database),
                                    some(&relation.id.schema),
                                    some(&relation.id.name),
                                    some(column),
                                    some(index + 1),
                                    action(&reference.update_rule)?,
                                    action(&reference.delete_rule)?,
                                    some(&key.name),
                                    reference.constraint_name.clone(),
                                    some(7),
                                ]
                            } else {
                                vec![
                                    some(&relation.id.database),
                                    some(&relation.id.schema),
                                    some(&relation.id.name),
                                    some(column),
                                    some(index + 1),
                                    some(&key.name),
                                ]
                            };
                            request.send(output, row, settings).await?;
                            count += 1;
                        }
                    }
                }
                Kind::PgSqlAlchemy(kind) => {
                    for row in super::sqlalchemy::rows(kind, relation)? {
                        request.send(output, row, settings).await?;
                        count += 1;
                    }
                }
                Kind::SqlAlchemy(kind) => {
                    for row in sqlalchemy_rows(kind, relation)? {
                        request.send(output, row, settings).await?;
                        count += 1;
                    }
                }
                // Standard Snowflake tables have no PostgreSQL physical indexes.
                Kind::Indexes => {}
                _ => unreachable!("handled metadata response"),
            }
        }
        Ok(count)
    }
}
fn action(value: &str) -> ExecutionResult<Option<String>> {
    Ok(some(match value {
        "CASCADE" => 0,
        "RESTRICT" => 1,
        "SET NULL" => 2,
        "NO ACTION" => 3,
        "SET DEFAULT" => 4,
        _ => {
            return Err(ExecutionError::rejected(
                "0A000",
                "unsupported referential action",
            ));
        }
    }))
}
fn sqlalchemy_columns(kind: crate::discovery::redshift::Kind) -> Vec<(&'static str, u8)> {
    use crate::discovery::redshift::Kind::*;
    match kind {
        Relations => vec![
            ("relkind", 0),
            ("schema_oid", 8),
            ("schema", 0),
            ("rel_oid", 8),
            ("relname", 0),
            ("diststyle", 0),
            ("owner_id", 8),
            ("owner_name", 0),
            ("view_definition", 0),
            ("privileges", 0),
        ],
        Columns => vec![
            ("schema", 0),
            ("table_name", 0),
            ("name", 0),
            ("encode", 0),
            ("type", 0),
            ("distkey", 3),
            ("sortkey", 2),
            ("notnull", 3),
            ("comment", 0),
            ("adsrc", 0),
            ("attnum", 2),
            ("format_type", 0),
            ("default", 0),
            ("schema_oid", 8),
            ("table_oid", 8),
        ],
        Constraints => vec![
            ("schema", 0),
            ("table_name", 0),
            ("contype", 0),
            ("conname", 0),
            ("conkey", 5),
            ("attnum", 2),
            ("attname", 0),
            ("condef", 0),
            ("schema_oid", 8),
            ("rel_oid", 8),
        ],
    }
}
fn sqlalchemy_rows(
    kind: crate::discovery::redshift::Kind,
    r: &Relation,
) -> ExecutionResult<Vec<Vec<Option<String>>>> {
    use crate::discovery::redshift::Kind::*;
    let schema_oid = some(r.id.frontend_schema_oid());
    let oid = some(r.id.frontend_oid());
    match kind {
        Relations => Ok(vec![vec![
            some(
                if matches!(r.kind, RelationKind::View | RelationKind::MaterializedView) {
                    "v"
                } else {
                    "r"
                },
            ),
            schema_oid,
            some(&r.id.schema),
            oid,
            some(&r.id.name),
            None,
            None,
            None,
            r.view_definition.clone(),
            None,
        ]]),
        Columns => r
            .columns
            .iter()
            .map(|c| {
                let t = FrontendType::for_profile(c, FrontendProfile::Redshift)?;
                Ok(vec![
                    some(&r.id.schema),
                    some(&r.id.name),
                    some(&c.id.name),
                    None,
                    some(t.sql_name()),
                    None,
                    None,
                    some(if c.nullable { "f" } else { "t" }),
                    c.comment.clone(),
                    c.default.clone(),
                    some(c.ordinal),
                    some(t.sql_name()),
                    c.default.clone(),
                    schema_oid.clone(),
                    oid.clone(),
                ])
            })
            .collect(),
        Constraints => {
            let mut rows = Vec::new();
            for key in r.constraints.as_ref().ok_or_else(|| {
                ExecutionError::rejected("0A000", "catalog key facts are unavailable")
            })? {
                let positions = key
                    .columns
                    .iter()
                    .map(|name| {
                        r.columns
                            .iter()
                            .find(|c| c.id.name == *name)
                            .map(|c| c.ordinal)
                            .ok_or_else(|| {
                                ExecutionError::rejected("55000", "inconsistent catalog key")
                            })
                    })
                    .collect::<ExecutionResult<Vec<_>>>()?;
                let conkey = format!(
                    "{{{}}}",
                    positions
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                );
                let kind = match key.kind {
                    ConstraintKind::PrimaryKey => "p",
                    ConstraintKind::Unique => "u",
                    ConstraintKind::ForeignKey => "f",
                };
                let definition = super::sqlalchemy::definition(key);
                for (name, ordinal) in key.columns.iter().zip(positions) {
                    rows.push(vec![
                        some(&r.id.schema),
                        some(&r.id.name),
                        some(kind),
                        some(&key.name),
                        some(&conkey),
                        some(ordinal),
                        some(name),
                        some(&definition),
                        schema_oid.clone(),
                        oid.clone(),
                    ]);
                }
            }
            Ok(rows)
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metabase_filtered_keys_capture_only_bounded_literal_schema_lists() {
        let template = include_str!("queries/metabase_foreign_keys_filtered.sql");
        let captured = template.replace("'__YASP_SCHEMAS__'", "('analytics'), ('public')");
        for profile in [FrontendProfile::Postgres, FrontendProfile::Redshift] {
            let request = Request::recognize(&captured, profile).unwrap();
            assert!(matches!(request.kind, Kind::Metabase(3)));
            assert_eq!(request.schemas, ["analytics", "public"]);
            for altered in [
                format!("{captured}; SELECT * FROM public.users"),
                format!("{captured} UNION ALL SELECT * FROM public.users"),
                captured.replace("\"pg_constraint\"", "\"public\".\"users\""),
                template.replace("'__YASP_SCHEMAS__'", "(SELECT secret FROM public.users)"),
                template.replace("'__YASP_SCHEMAS__'", "'public', NULL"),
                template.replace("'__YASP_SCHEMAS__'", &["'public'"; 257].join(",")),
            ] {
                assert!(Request::recognize(&altered, profile).is_none(), "{altered}");
            }
            for sample in [
                r#"SELECT "analytics"."metadata_types"."amount" AS "amount", SUBSTRING("analytics"."metadata_types"."mixedcase", 1, 1234) AS "substring2151", SUBSTRING("analytics"."metadata_types"."nullable_text", 1, 1234) AS "substring2152" FROM "analytics"."metadata_types" LIMIT 10000"#,
                r#"SELECT "analytics"."type_references"."tenant_id" AS "tenant_id", "analytics"."type_references"."type_id" AS "type_id" FROM "analytics"."type_references" LIMIT 10000"#,
            ] {
                assert!(Request::recognize(sample, profile).is_none());
            }
        }
    }
    #[test]
    fn integer_connection_probe_accepts_no_application_reads_or_expressions() {
        for profile in [FrontendProfile::Postgres, FrontendProfile::Redshift] {
            assert!(Request::recognize("SELECT 1", profile).is_some());
            for sql in [
                "SELECT 1 FROM PUBLIC.USERS",
                "SELECT 1 UNION ALL SELECT secret FROM PUBLIC.USERS",
                "SELECT 1; SELECT secret FROM PUBLIC.USERS",
                "SELECT 1 + (SELECT count(*) FROM PUBLIC.USERS)",
                "WITH hidden AS (SELECT * FROM PUBLIC.USERS) SELECT 1",
            ] {
                assert!(Request::recognize(sql, profile).is_none(), "{sql}");
            }
        }
    }

    #[test]
    fn reflection_accepts_only_the_complete_captured_request_and_literal_object_ids() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/superset_postgresql.json"
        ))
        .unwrap();
        for sql in fixture["queries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
        {
            assert!(
                Request::recognize(sql, FrontendProfile::Postgres).is_some(),
                "{sql}"
            );
            assert!(
                Request::recognize(
                    &format!("{sql}; SELECT * FROM public.users"),
                    FrontendProfile::Postgres
                )
                .is_none()
            );
            let altered = sql.replace("pg_catalog.pg_class", "public.users");
            if altered != sql {
                assert!(Request::recognize(&altered, FrontendProfile::Postgres).is_none());
            }
            for token in tokens(sql, &PostgreSqlDialect {}).unwrap() {
                if let Token::Number(value, false) = token
                    && value.parse::<u32>().is_ok_and(|n| n >= 1 << 30)
                {
                    assert!(
                        Request::recognize(
                            &sql.replace(&value, "(SELECT id FROM public.users)"),
                            FrontendProfile::Postgres
                        )
                        .is_none()
                    );
                }
            }
        }
        for value in [
            "\"PUBLIC\".\"USERS\";",
            "\"PUBLIC\".\"USERS\" /* hidden */",
            "\"\".\"\"",
            "PUBLIC.USERS UNION SELECT secret FROM users",
        ] {
            assert!(
                Request::recognize(
                    &format!("SELECT '{}'::regclass::oid", value.replace('\'', "''")),
                    FrontendProfile::Redshift
                )
                .is_none()
            );
        }
    }
}
