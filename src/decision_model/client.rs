//! Send bounded column samples to the configured classification HTTP endpoint.
//!
//! Requests contain all configured questions for one column. Concurrency permits
//! remain held through response consumption; malformed, oversized, or failed
//! responses produce an unavailable outcome. Callers decide whether uncertainty
//! fails the query. Classification itself never edits values or grants access.

use super::*;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};
use std::{io, time::Duration};
use tokio::sync::Semaphore;

// Shared across clients as well as sessions; admitted HTTP requests own permits
// through response consumption and cancellation.
static REQUESTS: Semaphore = Semaphore::const_new(8);

/// Reusable HTTP client and validated questions for raw-column classification.
/// Its fingerprint associates cached outcomes with the model request settings.
pub struct DecisionModel {
    settings: DecisionModelSettings,
    client: reqwest::Client,
    fingerprint: String,
}
impl DecisionModel {
    /// The consuming host resolves environment references before construction.
    /// Resolved header values and request/response bodies are never logged.
    pub fn new(
        settings: DecisionModelSettings,
        headers: BTreeMap<String, String>,
    ) -> io::Result<Self> {
        settings.validate().map_err(|_| invalid())?;
        if settings.headers_env.keys().ne(headers.keys()) {
            return Err(invalid());
        }
        let mut resolved = HeaderMap::new();
        for (key, value) in headers {
            let name = HeaderName::from_bytes(key.as_bytes()).map_err(|_| invalid())?;
            let mut value = HeaderValue::from_str(&value).map_err(|_| invalid())?;
            value.set_sensitive(true);
            resolved.insert(name, value);
        }
        let client = reqwest::Client::builder()
            .default_headers(resolved)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| invalid())?;
        let fingerprint = Sha256::digest(
            serde_json::to_vec(&(
                "systemone:noul:v1",
                &settings.endpoint,
                &settings.questions,
                settings.sample_size,
                settings.include_sql,
                PREFIX_ROWS,
                PREFIX_BYTES,
            ))
            .map_err(|_| invalid())?,
        )
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
        Ok(Self {
            settings,
            client,
            fingerprint,
        })
    }
    pub fn settings(&self) -> &DecisionModelSettings {
        &self.settings
    }
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// One request containing every configured question for this column.
    pub async fn classify(&self, request: ColumnRequest<'_>) -> ClassificationOutcome {
        tokio::time::timeout(
            Duration::from_millis(self.settings.timeout_ms.into()),
            self.request(request),
        )
        .await
        .ok()
        .flatten()
        .unwrap_or(ClassificationOutcome::Unavailable)
    }
    async fn request(&self, request: ColumnRequest<'_>) -> Option<ClassificationOutcome> {
        let sql_bytes = if self.settings.include_sql {
            request.sql.len()
        } else {
            0
        };
        if request.samples.len() > self.settings.sample_size
            || sql_bytes
                .saturating_add(request.column.name.len())
                .saturating_add(request.column.data_type.len())
                .saturating_add(request.samples.iter().map(String::len).sum::<usize>())
                > MAX_HTTP_BYTES
        {
            return None;
        }
        let questions: BTreeMap<_, _> = self
            .settings
            .questions
            .iter()
            .map(|(id, description)| {
                (
                    id,
                    serde_json::json!({"type":"noul", "instructions": description}),
                )
            })
            .collect();
        let mut state = serde_json::json!({"column": request.column, "samples": request.samples});
        if self.settings.include_sql {
            state["sql"] = request.sql.into();
        }
        let body = serde_json::to_vec(&serde_json::json!({"state": state, "questions": questions}))
            .ok()?;
        if body.len() > MAX_HTTP_BYTES {
            return None;
        }
        let _permit = REQUESTS.acquire().await.ok()?;
        let response = self
            .client
            .post(&self.settings.endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .ok()?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|n| n > MAX_HTTP_BYTES as u64)
        {
            return None;
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.ok()?;
            if chunk.len() > MAX_HTTP_BYTES.saturating_sub(bytes.len()) {
                return None;
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
        let usage = &value["usage"];
        if usage["truncated"].as_bool().unwrap_or(false)
            || usage["state_tokens_dropped"].as_u64().unwrap_or(0) > 0
            || usage["truncated_questions"]
                .as_array()
                .is_some_and(|v| !v.is_empty())
        {
            return None;
        }
        let raw = value["answers"].as_object()?;
        if raw.len() != self.settings.questions.len() {
            return None;
        }
        let mut answers = BTreeMap::new();
        for id in self.settings.questions.keys() {
            let answer = raw.get(id)?;
            if answer["type"].as_str()? != "noul" {
                return None;
            }
            let answer = DecisionAnswer {
                value: answer["noul"].as_f64()?,
                confidence: answer["confidence"].as_f64()?,
            };
            if !answer.valid() {
                return None;
            }
            answers.insert(id.clone(), answer);
        }
        Some(ClassificationOutcome::Available { answers })
    }

    pub(crate) async fn classify_columns(
        &self,
        sql: &str,
        columns: &[ColumnIdentity],
        samples: &[Vec<String>],
    ) -> Classifications {
        let mut results = Classifications::default();
        let mut remaining = MAX_METADATA_BYTES;
        // Reserve unavailable placeholders first, so a query-wide deadline keeps
        // completed decisions and explicit unavailability for unfinished columns.
        for column in columns {
            let item = ColumnClassification {
                column: column.clone(),
                outcome: ClassificationOutcome::Unavailable,
            };
            let size = serde_json::to_vec(&item).map_or(usize::MAX, |v| v.len() + 1);
            if size > remaining {
                break;
            }
            remaining -= size;
            results.columns.push(item);
        }
        let mut work = FuturesUnordered::new();
        let count = results.columns.len();
        let mut next = 0;
        while next < count.min(8) {
            work.push(self.indexed(sql, columns, samples, next).boxed());
            next += 1;
        }
        let collect = async {
            while let Some((ordinal, outcome)) = work.next().await {
                let extra = serde_json::to_vec(&outcome).map_or(usize::MAX, |v| v.len());
                if extra <= remaining {
                    remaining -= extra;
                    results.columns[ordinal].outcome = outcome;
                }
                if next < count {
                    work.push(self.indexed(sql, columns, samples, next).boxed());
                    next += 1;
                }
            }
        };
        let _ = tokio::time::timeout(
            Duration::from_millis(self.settings.timeout_ms.into()),
            collect,
        )
        .await;
        results
    }

    async fn indexed(
        &self,
        sql: &str,
        columns: &[ColumnIdentity],
        samples: &[Vec<String>],
        ordinal: usize,
    ) -> (usize, ClassificationOutcome) {
        (
            ordinal,
            self.classify(ColumnRequest {
                sql,
                column: &columns[ordinal],
                samples: &samples[ordinal],
            })
            .await,
        )
    }

    pub(crate) fn check(
        &self,
        results: &Classifications,
        count: usize,
    ) -> crate::backend::result::ExecutionResult<()> {
        if self.settings.fail_on_error
            && (results.columns.len() != count
                || results
                    .columns
                    .iter()
                    .any(|c| !self.valid_outcome(&c.outcome)))
        {
            return Err(crate::backend::result::ExecutionError::rejected(
                "XX000",
                "result classification unavailable",
            ));
        }
        Ok(())
    }
    pub(crate) fn valid_outcome(&self, outcome: &ClassificationOutcome) -> bool {
        matches!(outcome, ClassificationOutcome::Available { answers } if answers.keys().eq(self.settings.questions.keys()) && answers.values().all(DecisionAnswer::valid))
    }
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid decision model configuration or authentication headers",
    )
}
