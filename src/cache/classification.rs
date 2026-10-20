//! Decision sidecars share the raw cache entry's publication, identity and TTL.
use super::*;
use crate::decision_model::{
    Classifications, ColumnClassification, DecisionModel, MAX_METADATA_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(super) const CLASSIFIED_MAGIC: &[u8] = b"YASP\x02";
pub(super) const CLASSIFIED_HEADER: usize = HEADER_BYTES + 32;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Sidecar {
    version: u8,
    association: String,
    fingerprint: String,
    classification: ColumnClassification,
}

fn key(
    target: &str,
    login: &str,
    query_id: &str,
    column: &crate::decision_model::ColumnIdentity,
) -> String {
    format!(
        "{}:{}:{}",
        cache_key("yasp:decision_cache", target, login, query_id),
        column.ordinal,
        escape_component(&column.name)
    )
}

impl RedisCache {
    pub(super) async fn put_classified(
        &self,
        target: &str,
        login: &str,
        query_id: &str,
        buffer: &CacheBuffer,
        results: &Classifications,
        model: &DecisionModel,
    ) {
        let mut hash = Sha256::new();
        hash.update(buffer.bytes.as_ref());
        hash.update(model.fingerprint());
        let Ok(metadata) = serde_json::to_vec(results) else {
            return;
        };
        hash.update(&metadata);
        let association: [u8; 32] = hash.finalize().into();
        let association_text = hex(&association);
        let mut sidecars = Vec::new();
        let mut total = buffer.bytes.len() + 32;
        let mut metadata_bytes = 0usize;
        for result in &results.columns {
            let Ok(value) = serde_json::to_vec(&Sidecar {
                version: 1,
                association: association_text.clone(),
                fingerprint: model.fingerprint().into(),
                classification: result.clone(),
            }) else {
                return;
            };
            let key = key(target, login, query_id, &result.column);
            metadata_bytes = metadata_bytes
                .saturating_add(value.len())
                .saturating_add(key.len());
            if metadata_bytes > MAX_METADATA_BYTES {
                return;
            }
            total = total.saturating_add(value.len()).saturating_add(key.len());
            if total > self.max_entry_bytes() {
                return;
            }
            sidecars.push((key, value));
        }
        if total > self.max_entry_bytes() {
            return;
        }
        let mut data = Vec::with_capacity(buffer.bytes.len() + 32);
        data.extend_from_slice(CLASSIFIED_MAGIC);
        data.extend_from_slice(&buffer.bytes[MAGIC.len()..HEADER_BYTES]);
        data.extend_from_slice(&association);
        data.extend_from_slice(&buffer.bytes[HEADER_BYTES..]);
        // One command under the existing Redis deadline and byte reservation.
        // Equal EX values within this atomic script keep decisions alive with data.
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("for i=1,#KEYS do redis.call('SET',KEYS[i],ARGV[i+1],'EX',ARGV[1]) end return 1")
            .arg(sidecars.len() + 1)
            .arg(cache_key(&self.settings.namespace, target, login, query_id));
        for (key, _) in &sidecars {
            cmd.arg(key);
        }
        cmd.arg(self.settings.ttl_seconds).arg(data);
        for (_, value) in sidecars {
            cmd.arg(value);
        }
        self.command(cmd).await;
    }

    pub(super) async fn classifications(
        &self,
        target: &str,
        login: &str,
        query_id: &str,
        buffer: &CacheBuffer,
        columns: &[crate::decision_model::ColumnIdentity],
        model: &DecisionModel,
    ) -> Classifications {
        let Some(association) = buffer.association else {
            return Classifications::default();
        };
        let mut cmd = redis::cmd("EVAL");
        // Each GETRANGE and the aggregate response remain bounded, even if Redis
        // contains corrupt or manually edited sidecars. No unbounded GET/MGET.
        cmd.arg("local r={} local left=tonumber(ARGV[1]) for i,k in ipairs(KEYS) do local v=redis.call('GETRANGE',k,0,left) if #v>left then return {} end left=left-#v r[i]=v end return r")
            .arg(columns.len());
        let mut key_bytes = 0usize;
        for column in columns {
            let key = key(target, login, query_id, column);
            key_bytes = key_bytes.saturating_add(key.len());
            if key_bytes > MAX_METADATA_BYTES
                || key_bytes > self.max_entry_bytes().saturating_sub(buffer.bytes.len())
            {
                return Classifications::default();
            }
            cmd.arg(key);
        }
        let budget = MAX_METADATA_BYTES.min(
            self.max_entry_bytes()
                .saturating_sub(buffer.bytes.len())
                .saturating_sub(key_bytes),
        );
        cmd.arg(budget);
        let Some(Value::Array(values)) = self.command(cmd).await else {
            return Classifications::default();
        };
        let mut results = Classifications::default();
        for (column, value) in columns.iter().zip(values) {
            let Value::BulkString(value) = value else {
                continue;
            };
            let Ok(sidecar) = serde_json::from_slice::<Sidecar>(&value) else {
                continue;
            };
            if sidecar.version == 1
                && sidecar.association == hex(&association)
                && sidecar.fingerprint == model.fingerprint()
                && sidecar.classification.column == *column
                && (matches!(
                    sidecar.classification.outcome,
                    crate::decision_model::ClassificationOutcome::Unavailable
                ) || model.valid_outcome(&sidecar.classification.outcome))
            {
                results.columns.push(sidecar.classification);
            }
        }
        results
    }
}
fn hex(value: &[u8]) -> String {
    value.iter().map(|b| format!("{b:02x}")).collect()
}
