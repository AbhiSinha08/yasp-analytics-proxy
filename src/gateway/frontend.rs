//! Compatibility profiles share pgwire framing and connection lifetime rules.

use crate::discovery::SetupResponse;
use crate::{
    config::ConfigError,
    query::{InputOptions, ParsedQuery, QueryError, SqlDialect},
};
use sqlparser::ast::{Expr, SetExpr, Statement, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrontendProfile {
    Postgres,
    Redshift,
}

impl FrontendProfile {
    pub(crate) fn from_protocol(protocol: &str) -> Result<Self, ConfigError> {
        match protocol {
            "postgresql" => Ok(Self::Postgres),
            "redshift" => Ok(Self::Redshift),
            _ => Err(ConfigError::UnsupportedGateway),
        }
    }

    pub fn dialect(self) -> SqlDialect {
        match self {
            Self::Postgres => SqlDialect::Postgresql,
            Self::Redshift => SqlDialect::Redshift,
        }
    }

    pub(crate) fn parse_query(
        self,
        sql: &str,
        bound: usize,
        options: InputOptions,
    ) -> Result<ParsedQuery, QueryError> {
        let parsed = ParsedQuery::parse_with_options(sql, bound, options);
        // Redshift JDBC emits PostgreSQL catalog vector subscripts such as
        // con.confkey[pos.n]. Its normal grammar treats [pos.n] as a quoted name.
        // The alternate grammar is restricted to verified metadata-only ASTs;
        // native SQL and application-data queries never use this path.
        if self == Self::Redshift
            && !options.native
            && matches!(parsed, Err(QueryError::Syntax))
            && let Ok(catalog) = ParsedQuery::parse_with_options(
                sql,
                bound,
                InputOptions::frontend(SqlDialect::Postgresql),
            )
            && crate::discovery::classify(&catalog)
                == Some(crate::discovery::DiscoveryKind::Catalog)
        {
            return Ok(catalog);
        }
        parsed
    }

    pub(crate) fn setup(self, statement: &Statement) -> Option<SetupResponse> {
        if self == Self::Redshift
            && matches!(statement, Statement::Set(_))
            && matches!(
                statement.to_string().to_ascii_lowercase().as_str(),
                "set readonly = 1" | "set readonly = 0"
            )
        {
            // Redshift JDBC toggles this advisory flag around transactions.
            // Engine validation and source read-only enforcement remain fixed.
            return Some(SetupResponse::Set { parameter: None });
        }
        None
    }

    /// Profile fingerprints describe the frontend, independently of the source.
    /// PostgreSQL retains its existing source-backed discovery behavior.
    pub(crate) fn fingerprint(self, statement: &Statement) -> Option<SetupResponse> {
        if self != Self::Redshift {
            return None;
        }
        if let Statement::ShowVariable { variable } = statement {
            let [name] = variable.as_slice() else {
                return None;
            };
            let value = match name.value.to_ascii_lowercase().as_str() {
                "server_version" => "8.0.2",
                "server_version_num" => "80002",
                _ => return None,
            };
            return Some(SetupResponse::Show {
                name: name.value.clone(),
                value: value.into(),
            });
        }
        let sql = statement.to_string();
        if let Statement::Query(query) = statement
            && let SetExpr::Select(select) = query.body.as_ref()
            && let Some(Expr::Like { pattern, .. }) = &select.selection
            && matches!(pattern.as_ref(), Expr::Value(v) if matches!(v.value, Value::SingleQuotedString(_)))
            && sql.eq_ignore_ascii_case(&format!(
                "SELECT 1 FROM svv_external_schemas WHERE schemaname LIKE {pattern}"
            ))
        {
            // The virtual frontend exports local schemas, with no Redshift
            // Spectrum/federated namespace registry. No source rows are read.
            return Some(SetupResponse::EmptyInteger { name: "?column?" });
        }
        if sql.eq_ignore_ascii_case("SELECT version()")
            || sql.eq_ignore_ascii_case("SELECT pg_catalog.version()")
        {
            Some(SetupResponse::Row {
                name: "version".into(),
                value: "PostgreSQL 8.0.2, Redshift 1.0 (YASP compatibility profile)".into(),
                tag: "SELECT 1",
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_catalog_grammar_does_not_admit_native_or_application_queries() {
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/jdbc_redshift.json")).unwrap();
        let sql = corpus["queries"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|q| q.as_str())
            .find(|sql| sql.contains("con.confkey[pos.n]"))
            .unwrap();
        let options = InputOptions::frontend(SqlDialect::Redshift);
        let parsed = FrontendProfile::Redshift
            .parse_query(sql, 4096, options)
            .unwrap();
        assert_eq!(parsed.input_options().dialect, SqlDialect::Postgresql);
        assert!(
            FrontendProfile::Redshift
                .parse_query(sql, 4096, InputOptions::native(SqlDialect::Redshift))
                .is_err()
        );
        for sql in [
            sql.replace("pg_catalog.pg_constraint con,", "public.users con,"),
            sql.replace(
                "pg_catalog.pg_constraint con,",
                "pg_catalog.pg_constraint con, public.users hidden,",
            ),
        ] {
            assert!(
                FrontendProfile::Redshift
                    .parse_query(&sql, 4096, options)
                    .is_err(),
                "{sql}"
            );
        }
    }
}
