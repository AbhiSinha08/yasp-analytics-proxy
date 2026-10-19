//! PostgreSQL schema introspection uses the configured discovery login and its
//! normal admission, read-only execution, cancellation and cleanup machinery.
use super::PostgresBackend;
use crate::{
    catalog::{CatalogError, CatalogSettings, Rows},
    query::ParsedQuery,
};
use bytes::Buf;
use futures::Sink;
use pgwire::{
    error::{PgWireError, PgWireResult},
    messages::PgWireBackendMessage,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

impl PostgresBackend {
    pub(crate) async fn catalog_rows(
        &self,
        settings: &CatalogSettings,
    ) -> Result<Rows, CatalogError> {
        let schemas = settings
            .schemas
            .iter()
            .map(|s| format!("'{}'", s.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            r#"SELECT current_database(), n.nspname, c.relname,
            CASE c.relkind WHEN 'r' THEN 'TABLE' WHEN 'p' THEN 'TABLE' WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'f' THEN 'FOREIGN TABLE' END,
            pg_catalog.obj_description(c.oid, 'pg_class'), a.attname, a.attnum::text, t.typname,
            CASE WHEN a.attnotnull OR t.typnotnull THEN 'NO' ELSE 'YES' END,
            pg_catalog.pg_get_expr(d.adbin, d.adrelid), pg_catalog.col_description(c.oid, a.attnum),
            ic.character_maximum_length::text, ic.numeric_precision::text, ic.numeric_scale::text,
            ic.datetime_precision::text, a.atttypid::text, a.atttypmod::text,
            CASE WHEN a.attidentity <> '' THEN 'YES' ELSE 'NO' END,
            CASE WHEN a.attgenerated <> '' THEN 'YES' ELSE 'NO' END, c.oid::text,
            CASE WHEN a.attnum IS NULL THEN NULL WHEN EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint pk
                WHERE pk.conrelid = c.oid AND pk.contype = 'p' AND a.attnum = ANY(pk.conkey)
            ) THEN 'YES' ELSE 'NO' END,
            CASE WHEN c.oid IS NULL THEN NULL WHEN pg_catalog.has_table_privilege(c.oid, 'SELECT') THEN 'YES' ELSE 'NO' END
            FROM pg_catalog.pg_namespace n
            LEFT JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relkind IN ('r','p','v','m','f')
                AND (pg_catalog.has_table_privilege(c.oid, 'SELECT') OR pg_catalog.has_any_column_privilege(c.oid, 'SELECT'))
            LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                AND pg_catalog.has_column_privilege(c.oid, a.attnum, 'SELECT')
            LEFT JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
            LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
            LEFT JOIN information_schema.columns ic ON ic.table_catalog = current_database()
                AND ic.table_schema = n.nspname AND ic.table_name = c.relname AND ic.column_name = a.attname
            WHERE n.nspname IN ({}) AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
            ORDER BY n.nspname, c.relname, a.attnum LIMIT {}"#,
            schemas,
            settings.max_rows + 1
        );
        let query = ParsedQuery::parse(&sql).map_err(|_| CatalogError::InvalidSettings)?;
        let mut output = Collector {
            rows: Rows::new(settings),
            error: None,
        };
        let result = self
            .execute(
                &query,
                &mut output,
                settings.refresh_timeout,
                settings.refresh_timeout,
            )
            .await;
        if let Some(error) = output.error {
            return Err(error);
        }
        result.map_err(|_| CatalogError::SourceUnavailable)?;
        Ok(output.rows)
    }
}

struct Collector {
    rows: Rows,
    error: Option<CatalogError>,
}
impl Collector {
    fn row(&mut self, mut row: pgwire::messages::data::DataRow) -> Result<(), CatalogError> {
        if row.field_count != 22 {
            return Err(CatalogError::InvalidMetadata);
        }
        let mut values = Vec::with_capacity(22);
        for _ in 0..22 {
            if row.data.remaining() < 4 {
                return Err(CatalogError::InvalidMetadata);
            }
            let size = row.data.get_i32();
            if size == -1 {
                values.push(None);
                continue;
            }
            let size = usize::try_from(size).map_err(|_| CatalogError::InvalidMetadata)?;
            if size > row.data.remaining() {
                return Err(CatalogError::InvalidMetadata);
            }
            let value = row.data.split_to(size);
            values.push(Some(
                std::str::from_utf8(&value)
                    .map_err(|_| CatalogError::InvalidMetadata)?
                    .to_owned(),
            ));
        }
        if row.data.has_remaining() {
            return Err(CatalogError::InvalidMetadata);
        }
        self.rows.push(values)
    }
}
impl Sink<PgWireBackendMessage> for Collector {
    type Error = PgWireError;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<PgWireResult<()>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, event: PgWireBackendMessage) -> PgWireResult<()> {
        if let PgWireBackendMessage::DataRow(row) = event
            && let Err(error) = self.row(row)
        {
            self.error = Some(error);
            return Err(std::io::Error::other("catalog collection failed").into());
        }
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<PgWireResult<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<PgWireResult<()>> {
        Poll::Ready(Ok(()))
    }
}
