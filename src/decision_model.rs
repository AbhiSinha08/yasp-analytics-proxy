//! Optional REST classification of bounded column samples. This module never
//! interprets classifications as authorization or changes database values.
mod client;
pub use client::DecisionModel;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const MAX_HTTP_BYTES: usize = 64 * 1024;
pub(crate) const MAX_METADATA_BYTES: usize = 256 * 1024;
pub(crate) const PREFIX_ROWS: usize = 100;
pub(crate) const PREFIX_BYTES: usize = 256 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionModelSettings {
    pub endpoint: String,
    #[serde(default = "namespace")]
    pub namespace: String,
    #[serde(default = "sample_size")]
    pub sample_size: usize,
    #[serde(default = "include_sql")]
    pub include_sql: bool,
    #[serde(default = "timeout_ms")]
    pub timeout_ms: u32,
    #[serde(default)]
    pub fail_on_error: bool,
    #[serde(default)]
    pub headers_env: BTreeMap<String, String>,
    pub questions: BTreeMap<String, String>,
}
fn namespace() -> String {
    "yasp:decision_cache".into()
}
fn sample_size() -> usize {
    5
}
fn include_sql() -> bool {
    true
}
fn timeout_ms() -> u32 {
    2000
}

impl DecisionModelSettings {
    pub fn validate(&self) -> Result<(), crate::config::ConfigError> {
        if self.namespace.trim().is_empty() || self.namespace.contains('\0') {
            return Err(crate::config::ConfigError::InvalidValue(
                "data_contexts.decision_model.namespace",
            ));
        }
        let valid = reqwest::Url::parse(&self.endpoint).is_ok_and(|url| {
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none()
        }) && (1..=PREFIX_ROWS).contains(&self.sample_size)
            && self.timeout_ms > 0
            && self.timeout_ms <= i32::MAX as u32
            && !self.questions.is_empty()
            && self.questions.iter().all(|(id, description)| {
                !id.trim().is_empty()
                    && !description.trim().is_empty()
                    && !id.contains('\0')
                    && !description.contains('\0')
            })
            && serde_json::to_vec(&self.questions).is_ok_and(|v| v.len() <= MAX_HTTP_BYTES / 2)
            && self.headers_env.iter().all(|(header, env)| {
                reqwest::header::HeaderName::from_bytes(header.as_bytes()).is_ok()
                    && !matches!(
                        header.to_ascii_lowercase().as_str(),
                        "host" | "content-length" | "transfer-encoding" | "content-type"
                    )
                    && !env.is_empty()
                    && !env.contains(['\0', '='])
            });
        if valid {
            Ok(())
        } else {
            Err(crate::config::ConfigError::InvalidDecisionModel)
        }
    }
}

/// Text values retain the backend adapter's lossless text representation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnIdentity {
    pub ordinal: usize,
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Serialize)]
pub struct ColumnRequest<'a> {
    pub sql: &'a str,
    pub column: &'a ColumnIdentity,
    pub samples: &'a [String],
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionAnswer {
    /// Probability that the answer to the configured question is yes.
    pub value: f64,
    pub confidence: f64,
}
impl DecisionAnswer {
    pub(crate) fn valid(&self) -> bool {
        [self.value, self.confidence]
            .into_iter()
            .all(|v| v.is_finite() && (0.0..=1.0).contains(&v))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClassificationOutcome {
    Available {
        answers: BTreeMap<String, DecisionAnswer>,
    },
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnClassification {
    pub column: ColumnIdentity,
    pub outcome: ClassificationOutcome,
}

/// Missing ordinals are unavailable too, including when the metadata bound is
/// exceeded. Consumers must never interpret absence as a negative classification.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Classifications {
    pub columns: Vec<ColumnClassification>,
}
impl Classifications {
    pub fn outcome(&self, ordinal: usize) -> &ClassificationOutcome {
        static UNAVAILABLE: ClassificationOutcome = ClassificationOutcome::Unavailable;
        self.columns
            .iter()
            .find(|c| c.column.ordinal == ordinal)
            .map(|c| &c.outcome)
            .unwrap_or(&UNAVAILABLE)
    }
}
