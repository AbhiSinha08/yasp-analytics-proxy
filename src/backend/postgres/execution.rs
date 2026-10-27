//! PostgreSQL execution and physical source admission.
use super::{BackendRoute, PostgresBackend, error_response as protocol_error};
use crate::backend::execution::ExecutionRequest;
use futures::{Sink, channel::oneshot};
use pgwire::{
    error::{PgWireError, PgWireResult},
    messages::PgWireBackendMessage,
};
use std::fmt;

impl PostgresBackend {
    pub(crate) fn execution_identity(&self) -> [u8; 32] {
        crate::backend::execution::identity(&[
            match self.engine() {
                crate::config::PgWireEngine::Postgresql => "postgresql:text:v1",
                crate::config::PgWireEngine::Redshift => "redshift:text:v1",
            },
            self.target(),
            self.database_user(),
            self.compute_type().unwrap_or(""),
            self.compute(),
            self.engine().session_defaults(),
        ])
    }
}
impl BackendRoute<'_> {
    pub(crate) fn validate_input(&self, query: &crate::query::ParsedQuery) -> PgWireResult<()> {
        validate_input(query)
    }
    pub(crate) async fn execute_query<S>(
        &self,
        execution: ExecutionRequest<'_, '_>,
        socket: &mut S,
        mut cancel: oneshot::Receiver<()>,
    ) -> PgWireResult<Option<[u8; 32]>>
    where
        S: Sink<PgWireBackendMessage> + Unpin + Send,
        S::Error: fmt::Debug,
        PgWireError: From<S::Error>,
    {
        let ExecutionRequest {
            selection: request,
            settings,
            session,
        } = execution;
        let query = request.query;
        let selected = self.selection();
        let lease = tokio::select! {
            biased;
            _ = &mut cancel => return Err(protocol_error("57014", "query cancelled")),
            result = self.acquire(settings.queue_timeout()) => result?,
        };
        let backend = lease.backend();
        let identity = backend.execution_identity();
        tracing::debug!(
            query_id = request.query_id, client_id = request.client_id,
            database = ?backend.target(), database_user = %backend.database_user(),
            target = %selected.target, backend_login = %selected.backend_login,
            sql = ?crate::hooks::builtin::extractors::sql_without_observed_postgres_remark(query.sql()),
            "query sent to backend"
        );
        let reusable = lease
            .execute_with_session(
                query,
                socket,
                settings.query_timeout(),
                settings.write_timeout(),
                Some(cancel),
                Some(session),
            )
            .await?;
        Ok(reusable.then_some(identity))
    }
}

pub(super) fn validate_input(query: &crate::query::ParsedQuery) -> PgWireResult<()> {
    query
        .validate_read_only()
        .map_err(|_| protocol_error("0A000", "only read-only queries are supported"))
}

/// Replay only the bounded frontend settings; protected text settings stay fixed.
pub(super) fn session_sql(
    engine: crate::config::PgWireEngine,
    session: &crate::discovery::SessionSettings,
) -> String {
    let set_config = match engine {
        crate::config::PgWireEngine::Postgresql => "pg_catalog.set_config",
        crate::config::PgWireEngine::Redshift => "set_config",
    };
    let literal = |value: &str| {
        let value = if engine == crate::config::PgWireEngine::Redshift {
            // Redshift always interprets backslashes in ordinary SQL strings.
            value.replace('\\', "\\\\")
        } else {
            value.to_owned()
        };
        value.replace('\'', "''")
    };
    format!(
        "SELECT {set_config}('search_path', '{}', true); SET LOCAL application_name = '{}'",
        literal(&session.search_path),
        literal(&session.application_name),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redshift_session_values_cannot_escape_their_native_sql_literals() {
        let session = crate::discovery::SessionSettings {
            search_path: "\"a\\b'c\",public".into(),
            application_name: "\\'; RESET ALL; SELECT '\\".into(),
        };
        let sql = session_sql(crate::config::PgWireEngine::Redshift, &session);
        let parsed =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::RedshiftSqlDialect {}, &sql)
                .unwrap();
        assert_eq!(
            parsed.len(),
            2,
            "labels remain values in the two controlled statements"
        );
        let sqlparser::ast::Statement::Query(query) = &parsed[0] else {
            panic!("session function")
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("session function")
        };
        let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Function(function)) =
            &select.projection[0]
        else {
            panic!("session function")
        };
        let sqlparser::ast::FunctionArguments::List(arguments) = &function.args else {
            panic!("session arguments")
        };
        assert!(
            matches!(&arguments.args[1], sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(sqlparser::ast::Expr::Value(value))) if value.value == sqlparser::ast::Value::SingleQuotedString(session.search_path))
        );
    }
}
