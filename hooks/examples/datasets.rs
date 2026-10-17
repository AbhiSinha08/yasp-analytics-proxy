use futures::future::BoxFuture;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use yasp::datasets::{DatasetLoadError, DatasetLoadRequest, bind_dataset_loader};

pub fn load_email_locations<'a>(
    _request: DatasetLoadRequest<'a>,
    _state: Arc<()>,
) -> BoxFuture<'a, Result<Map<String, Value>, DatasetLoadError>> {
    Box::pin(async {
        let Value::Object(value) = json!({
            "columns": ["crm.contacts.email", "billing.invoices.recipient_email"]
        }) else {
            unreachable!()
        };
        Ok(value)
    })
}

pub fn bind_email_locations() -> yasp::datasets::DatasetLoader {
    bind_dataset_loader((), load_email_locations).into()
}
