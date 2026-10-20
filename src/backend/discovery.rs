//! Configured catalog source identity, independent of client SQL.
use super::PostgresBackend;

/// Engine identity comes from the configured backend, independently of client SQL.
pub(crate) enum DiscoverySource<'a> {
    Postgres(&'a PostgresBackend),
    #[cfg(feature = "snowflake")]
    Snowflake(&'a super::snowflake::SnowflakeBackend),
}

impl DiscoverySource<'_> {
    pub(crate) fn catalog_identity(&self) -> crate::catalog::SourceIdentity {
        match self {
            #[cfg(feature = "snowflake")]
            Self::Snowflake(source) => source.catalog_identity(),
            Self::Postgres(source) => crate::catalog::SourceIdentity {
                dialect: crate::query::SqlDialect::Postgresql,
                endpoint: source.target().into(),
                principal: source.database_user().into(),
                role: None,
                warehouse: None,
                default_schema: None,
            },
        }
    }
}
