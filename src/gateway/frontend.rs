//! Compatibility profiles share pgwire framing and connection lifetime rules.

use crate::discovery::SetupResponse;
use crate::{
    config::ConfigError,
    query::{ParsedQuery, QueryError, SqlDialect},
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

    /// Inspect only the frontend compatibility surface before application hooks.
    pub(crate) fn parse_query(self, sql: &str, bound: usize) -> Result<ParsedQuery, QueryError> {
        let parsed = ParsedQuery::parse_with_dialect(sql, bound, self.dialect());
        // Captured Redshift catalog SQL uses PostgreSQL vector subscripts.
        // This alternate grammar is restricted to catalog-only statements.
        if self == Self::Redshift
            && matches!(parsed, Err(QueryError::Syntax))
            && let Ok(catalog) = ParsedQuery::parse_with_dialect(sql, bound, SqlDialect::Postgresql)
            && crate::discovery::classify(&catalog)
                == Some(crate::discovery::DiscoveryKind::Catalog)
        {
            return Ok(catalog);
        }
        parsed
    }

    pub(crate) fn is_frontend_request(self, query: &ParsedQuery) -> bool {
        !matches!(query.statement(), Statement::Query(_))
            || self.setup(query).is_some()
            || self.fingerprint(query.statement()).is_some()
            || crate::discovery::classify(query).is_some()
    }

    pub(crate) fn setup(self, query: &ParsedQuery) -> Option<SetupResponse> {
        let statement = query.statement();
        if matches!(statement, Statement::Query(_)) {
            let sql = statement.to_string();
            if sql.eq_ignore_ascii_case("SELECT pg_backend_pid()")
                || sql.eq_ignore_ascii_case("SELECT pg_catalog.pg_backend_pid()")
            {
                return Some(SetupResponse::BackendPid);
            }
        }
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
    /// PostgreSQL profile probes use the captured source catalog settings.
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
    fn legacy_catalog_grammar_cannot_admit_application_queries() {
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/jdbc_redshift.json")).unwrap();
        let sql = corpus["queries"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|q| q.as_str())
            .find(|sql| sql.contains("con.confkey[pos.n]"))
            .unwrap();
        let parsed = FrontendProfile::Redshift.parse_query(sql, 4096).unwrap();
        assert_eq!(parsed.dialect(), SqlDialect::Postgresql);
        for altered in [
            sql.replace("pg_catalog.pg_constraint con,", "public.users con,"),
            sql.replace(
                "pg_catalog.pg_constraint con,",
                "pg_catalog.pg_constraint con, public.users hidden,",
            ),
        ] {
            assert!(
                FrontendProfile::Redshift
                    .parse_query(&altered, 4096)
                    .is_err()
            );
        }
    }
}
