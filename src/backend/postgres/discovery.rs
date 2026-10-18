//! PostgreSQL source rules for PostgreSQL client metadata requests.

use super::{PostgresBackend, error_response};
use crate::backend::discovery::DiscoveryRequest;
use crate::{
    discovery::{catalog_relation, helper, name},
    query::{ParsedQuery, QueryError},
};
use futures::{Sink, channel::oneshot};
use pgwire::{
    error::{PgWireError, PgWireResult},
    messages::PgWireBackendMessage,
};
use sqlparser::ast::{
    DataType, Expr, Ident, ObjectName, ObjectNamePart, Statement, TableAlias, TableFactor,
    VisitMut, VisitorMut,
};
use std::{fmt, ops::ControlFlow};

/// Handle PostgreSQL client discovery using this source's configured pool.
/// Rewriting and session setup belong here; the executor retains bounded
/// streaming, cancellation, read-only enforcement, and connection cleanup.
pub(in crate::backend) async fn discover<S>(
    backend: &PostgresBackend,
    request: DiscoveryRequest<'_>,
    output: &mut S,
    cancel: oneshot::Receiver<()>,
) -> PgWireResult<()>
where
    S: Sink<PgWireBackendMessage> + Unpin + Send,
    S::Error: fmt::Debug,
    PgWireError: From<S::Error>,
{
    let DiscoveryRequest {
        query,
        kind,
        session,
        client_id,
        query_timeout,
        write_timeout,
    } = request;
    tracing::debug!(query_id = query.query_id(), client_id, ?kind, database = ?backend.target(), database_user = %backend.database_user(), sql = ?query.sql(), "discovery query sent to backend");
    let query =
        prepare(query).map_err(|_| error_response("54000", "metadata query exceeds its bound"))?;
    let setup = session.setup_sql();
    backend
        .route()
        .acquire()
        .await?
        .execute_discovery(&query, output, query_timeout, write_timeout, cancel, &setup)
        .await
        .map(|_| ())
}

/// Qualify native catalogs/helpers and restrict every database listing to its source.
/// Only statements accepted by the frontend discovery classifier enter this path.
pub(in crate::backend) fn prepare(query: &ParsedQuery) -> Result<ParsedQuery, QueryError> {
    let mut statement = query.statement().clone();
    static LISTING: std::sync::OnceLock<sqlparser::ast::Query> = std::sync::OnceLock::new();
    let listing = LISTING.get_or_init(|| {
        let query = ParsedQuery::parse(
            "SELECT * FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()",
        )
        .expect("fixed PostgreSQL catalog SQL");
        let Statement::Query(listing) = query.statement() else {
            unreachable!()
        };
        *listing.clone()
    });
    let mut rewriter = NativeNames { listing };
    let _ = statement.visit(&mut rewriter);
    ParsedQuery::from_statement(statement)
}

fn qualify(object: &mut ObjectName, schema: &str) {
    if object.0.len() == 1 {
        object
            .0
            .insert(0, ObjectNamePart::Identifier(Ident::new(schema)));
    }
}

struct NativeNames<'a> {
    listing: &'a sqlparser::ast::Query,
}

impl VisitorMut for NativeNames<'_> {
    type Break = ();
    fn post_visit_table_factor(&mut self, table: &mut TableFactor) -> ControlFlow<()> {
        match table {
            TableFactor::Table {
                name: object,
                args: None,
                alias,
                ..
            } => {
                if let Some(schema) = catalog_relation(object) {
                    if name(object).is_some_and(|(_, table)| table == "pg_database") {
                        let alias = alias.clone().or_else(|| {
                            Some(TableAlias {
                                name: Ident::new("pg_database"),
                                columns: vec![],
                                explicit: false,
                                at: None,
                            })
                        });
                        *table = TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(self.listing.clone()),
                            alias,
                            sample: None,
                        };
                    } else {
                        qualify(object, schema);
                    }
                }
            }
            TableFactor::Table {
                name: object,
                args: Some(_),
                ..
            }
            | TableFactor::Function { name: object, .. } => {
                if helper(object) {
                    qualify(object, "pg_catalog");
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Function(function) if helper(&function.name) => {
                if !name(&function.name).is_some_and(|(_, n)| {
                    matches!(n.as_str(), "coalesce" | "nullif" | "greatest" | "least")
                }) {
                    qualify(&mut function.name, "pg_catalog");
                }
            }
            Expr::Cast { data_type, .. } => qualify_type(data_type),
            Expr::TypedString(typed) => qualify_type(&mut typed.data_type),
            Expr::Convert {
                data_type: Some(data_type),
                ..
            } => qualify_type(data_type),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn qualify_type(ty: &mut DataType) {
    match ty {
        DataType::Custom(object, _) => qualify(object, "pg_catalog"),
        DataType::Array(
            sqlparser::ast::ArrayElemTypeDef::SquareBracket(ty, _)
            | sqlparser::ast::ArrayElemTypeDef::AngleBracket(ty)
            | sqlparser::ast::ArrayElemTypeDef::Parenthesis(ty),
        ) => qualify_type(ty),
        _ => {}
    }
}
