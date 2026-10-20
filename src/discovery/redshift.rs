//! Captured SQLAlchemy Redshift metadata requests, independent of source engines.
use super::template::{capture, tokens};
use crate::query::{ParsedQuery, SqlDialect};
use sqlparser::{dialect::RedshiftSqlDialect, tokenizer::Token};
use std::sync::OnceLock;

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Relations,
    Columns,
    Constraints,
}

pub(crate) struct Request {
    pub kind: Kind,
    pub schema: Option<String>,
    pub table: Option<String>,
}

impl Request {
    pub(crate) fn recognize(query: &ParsedQuery) -> Option<Self> {
        if query.input_options().native || query.input_options().dialect != SqlDialect::Redshift {
            return None;
        }
        let actual = tokens(query.sql(), &RedshiftSqlDialect {})?;
        Self::recognize_tokens(&actual)
    }

    pub(crate) fn recognize_tokens(actual: &[Token]) -> Option<Self> {
        static TEMPLATES: OnceLock<Vec<(Kind, Vec<Token>)>> = OnceLock::new();
        let templates = TEMPLATES.get_or_init(|| {
            let mut templates = Vec::new();
            for (kind, sql, table_column) in [
                (
                    Kind::Relations,
                    include_str!("queries/sqlalchemy_redshift_relations.sql"),
                    "relname",
                ),
                (
                    Kind::Columns,
                    include_str!("queries/sqlalchemy_redshift_columns.sql"),
                    "table_name",
                ),
                (
                    Kind::Constraints,
                    include_str!("queries/sqlalchemy_redshift_constraints.sql"),
                    "table_name",
                ),
            ] {
                for schema in [false, true] {
                    for table in [false, true] {
                        let mut sql = sql.to_owned();
                        if !schema {
                            sql = sql.replace("AND schema = '__YASP_SCHEMA__'", "");
                        }
                        if !table {
                            sql =
                                sql.replace(&format!("AND {table_column} = '__YASP_TABLE__'"), "");
                        }
                        templates.push((
                            kind,
                            tokens(&sql, &RedshiftSqlDialect {})
                                .expect("captured SQLAlchemy metadata"),
                        ));
                    }
                }
            }
            templates
        });
        templates.iter().find_map(|(kind, template)| {
            let captures = capture(actual, template)?;
            let value = |key| captures.get(key).map(|v| v[0].to_owned());
            Some(Self {
                kind: *kind,
                schema: value("__YASP_SCHEMA__"),
                table: value("__YASP_TABLE__"),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        discovery::{DiscoveryKind, classify},
        query::InputOptions,
    };

    #[test]
    fn sqlalchemy_reflection_accepts_only_captured_metadata_with_literal_filters() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/superset_redshift.json"))
                .unwrap();
        let options = InputOptions::frontend(SqlDialect::Redshift);
        for captured in fixture["queries"].as_array().unwrap() {
            let sql = captured["sql"].as_str().unwrap();
            let parsed = ParsedQuery::parse_with_options(sql, 128 * 1024, options).unwrap();
            assert!(Request::recognize(&parsed).is_some(), "{sql}");
            assert_eq!(classify(&parsed), Some(DiscoveryKind::Catalog));
            for altered in [
                format!("{sql}; SELECT * FROM public.users"),
                format!("{sql} UNION ALL SELECT * FROM public.users"),
                sql.replace("pg_catalog.pg_class", "public.users"),
                sql.replace("svv_external_tables", "public.users")
                    .replace("svv_external_columns", "public.users"),
                sql.replacen("'public'", "(SELECT secret FROM public.users)", 1),
            ] {
                if altered != sql
                    && let Ok(parsed) =
                        ParsedQuery::parse_with_options(&altered, 128 * 1024, options)
                {
                    assert!(Request::recognize(&parsed).is_none());
                    assert_eq!(classify(&parsed), None, "{altered}");
                }
            }
            if sql.contains("'public'") {
                let different = sql.replacen("'public'", "'other'", 1);
                assert!(
                    Request::recognize(
                        &ParsedQuery::parse_with_options(&different, 128 * 1024, options).unwrap()
                    )
                    .is_none()
                );
            }
            let native = ParsedQuery::parse_with_options(
                sql,
                128 * 1024,
                InputOptions::native(SqlDialect::Redshift),
            )
            .unwrap();
            assert!(Request::recognize(&native).is_none());
            assert_eq!(classify(&native), None);
            if let Ok(postgres) = ParsedQuery::parse_with_options(
                sql,
                128 * 1024,
                InputOptions::frontend(SqlDialect::Postgresql),
            ) {
                assert!(Request::recognize(&postgres).is_none());
                assert_eq!(classify(&postgres), None);
            }
        }
    }
}
