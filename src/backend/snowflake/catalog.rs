//! Only Snowflake information-schema SQL reaches this provider. The connector
//! and capacity accounting are identical to native application execution.
use super::SnowflakeBackend;
use crate::{
    backend::result::{ExecutionError, ResultEvent},
    catalog::{CatalogError, CatalogSettings, Rows, SourceIdentity},
    config::GatewaySettings,
    discovery::SessionSettings,
    query::{InputOptions, ParsedQuery, SqlDialect},
};
use futures::{Sink, channel::oneshot};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

impl SnowflakeBackend {
    pub(crate) fn catalog_identity(&self) -> SourceIdentity {
        self.inner.catalog_identity.clone()
    }
    pub(crate) async fn catalog_rows(
        &self,
        settings: &CatalogSettings,
    ) -> Result<Rows, CatalogError> {
        let schemas = settings
            .schemas
            .iter()
            .map(|s| format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            r#"SELECT s.catalog_name, s.schema_name, t.table_name, t.table_type, t.comment,
            c.column_name, c.ordinal_position, c.data_type, c.is_nullable, c.column_default, c.comment,
            c.character_maximum_length, c.numeric_precision, c.numeric_scale, NULL,
            NULL, NULL, c.is_identity, NULL, NULL, NULL, NULL
            FROM information_schema.schemata s
            LEFT JOIN information_schema.tables t ON t.table_catalog = s.catalog_name AND t.table_schema = s.schema_name
            LEFT JOIN information_schema.columns c ON c.table_catalog = t.table_catalog AND c.table_schema = t.table_schema AND c.table_name = t.table_name
            WHERE s.catalog_name = CURRENT_DATABASE() AND s.schema_name IN ({})
            ORDER BY s.schema_name, t.table_name, c.ordinal_position LIMIT {}"#,
            schemas,
            settings.max_rows + 1
        );
        let query = ParsedQuery::parse_with_options(
            &sql,
            1024 * 1024,
            InputOptions::native(SqlDialect::Snowflake),
        )
        .map_err(|_| CatalogError::InvalidSettings)?;
        query
            .validate_read_only()
            .map_err(|_| CatalogError::InvalidSettings)?;
        let milliseconds = u32::try_from(settings.refresh_timeout.as_millis())
            .map_err(|_| CatalogError::InvalidSettings)?;
        let execution = GatewaySettings {
            query_timeout_ms: milliseconds,
            ..GatewaySettings::default()
        };
        let mut session = self
            .open(&execution, false)
            .await
            .map_err(|_| CatalogError::SourceUnavailable)?;
        let mut output = Collector {
            rows: Rows::new(settings),
            error: None,
        };
        let (_cancel, receiver) = oneshot::channel();
        let result = session
            .execute(
                &query,
                &mut output,
                &execution,
                &SessionSettings::default(),
                receiver,
            )
            .await;
        let finish = session.finish(false).await;
        if let Some(error) = output.error {
            return Err(error);
        }
        result
            .and(finish.map(|_| ResultEvent::Complete(0)))
            .map_err(|_| CatalogError::SourceUnavailable)?;
        Ok(output.rows)
    }
}
struct Collector {
    rows: Rows,
    error: Option<CatalogError>,
}
impl Sink<ResultEvent> for Collector {
    type Error = ExecutionError;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, event: ResultEvent) -> Result<(), Self::Error> {
        if let ResultEvent::Row(row) = event {
            let result = row
                .into_iter()
                .map(|value| {
                    value
                        .map(|v| {
                            String::from_utf8(v.to_vec()).map_err(|_| CatalogError::InvalidMetadata)
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()
                .and_then(|row| self.rows.push(row));
            if let Err(error) = result {
                self.error = Some(error);
                return Err(ExecutionError::rejected(
                    "54000",
                    "catalog collection failed",
                ));
            }
        }
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
