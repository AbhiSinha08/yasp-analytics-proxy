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
struct Sidecar<T> {
    version: u8,
    association: String,
    fingerprint: String,
    classification: T,
}

#[derive(Serialize)]
struct AstSidecar<'a> {
    version: u8,
    association: &'a str,
    ast: &'a crate::lineage::AstDataContext,
}

fn key(
    namespace: &str,
    target: &str,
    login: &str,
    query_id: &str,
    column: &crate::decision_model::ColumnIdentity,
) -> String {
    format!(
        "{}:{}:{}",
        cache_key(namespace, target, login, query_id),
        column.ordinal,
        escape_component(&column.name)
    )
}

impl RedisCache {
    pub(super) async fn put_contexts(
        &self,
        target: &str,
        login: &str,
        query_id: &str,
        buffer: &mut CacheBuffer,
        decisions: Option<(&Classifications, &DecisionModel)>,
        ast: Option<(&crate::lineage::AstDataContext, &str)>,
    ) {
        let mut copied = 0;
        if let Some((results, _)) = decisions {
            let Ok(size) = crate::hooks::payload::serialized_size(results, MAX_METADATA_BYTES)
            else {
                return;
            };
            copied += size;
        }
        if let Some((ast, _)) = ast {
            let Ok(size) = crate::hooks::payload::serialized_size(ast, MAX_METADATA_BYTES) else {
                return;
            };
            copied += size;
        }
        if copied > MAX_METADATA_BYTES
            || !buffer.reserve_bytes(buffer.bytes.capacity() + copied + 32)
        {
            return;
        }
        let mut hash = Sha256::new();
        hash.update(buffer.bytes.as_ref());
        if let Some((results, model)) = decisions {
            hash.update(model.fingerprint());
            let Ok(metadata) = serde_json::to_vec(results) else {
                return;
            };
            hash.update(metadata);
        }
        if let Some((ast, _)) = ast {
            if crate::hooks::payload::serialized_size(ast, MAX_METADATA_BYTES).is_err() {
                return;
            }
            let Ok(metadata) = serde_json::to_vec(ast) else {
                return;
            };
            hash.update(metadata);
        }
        let association: [u8; 32] = hash.finalize().into();
        let association_text = hex(&association);
        let mut sidecars = Vec::new();
        let mut total = buffer.bytes.len() + 32;
        let mut metadata_bytes = 0usize;
        for (result, model) in decisions
            .into_iter()
            .flat_map(|(results, model)| results.columns.iter().map(move |result| (result, model)))
        {
            let sidecar = Sidecar {
                version: 1,
                association: association_text.clone(),
                fingerprint: model.fingerprint().into(),
                classification: result,
            };
            let Ok(size) = crate::hooks::payload::serialized_size(&sidecar, MAX_METADATA_BYTES)
            else {
                return;
            };
            let key_bound = model.settings().namespace.len()
                + 3 * (target.len() + login.len() + result.column.name.len())
                + query_id.len()
                + 32;
            if metadata_bytes + size + key_bound > MAX_METADATA_BYTES
                || metadata_bytes + size + key_bound + buffer.bytes.len() + 32
                    > self.max_entry_bytes()
                || !buffer
                    .reserve_bytes(buffer.bytes.capacity() + metadata_bytes + size + key_bound + 32)
            {
                return;
            }
            let Ok(value) = serde_json::to_vec(&sidecar) else {
                return;
            };
            let key = key(
                &model.settings().namespace,
                target,
                login,
                query_id,
                &result.column,
            );
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
        if let Some((ast, namespace)) = ast {
            if namespace.trim().is_empty() || namespace.contains('\0') {
                return;
            }
            let sidecar = AstSidecar {
                version: 1,
                association: &association_text,
                ast,
            };
            let Ok(size) = crate::hooks::payload::serialized_size(&sidecar, MAX_METADATA_BYTES)
            else {
                return;
            };
            let key_bound = namespace.len() + 3 * (target.len() + login.len()) + query_id.len() + 3;
            if metadata_bytes + size + key_bound > MAX_METADATA_BYTES
                || metadata_bytes + size + key_bound + buffer.bytes.len() + 32
                    > self.max_entry_bytes()
                || !buffer
                    .reserve_bytes(buffer.bytes.capacity() + metadata_bytes + size + key_bound + 32)
            {
                return;
            }
            let key = cache_key(namespace, target, login, query_id);
            let Ok(value) = serde_json::to_vec(&sidecar) else {
                return;
            };
            metadata_bytes = metadata_bytes
                .saturating_add(key.len())
                .saturating_add(value.len());
            total = total.saturating_add(key.len()).saturating_add(value.len());
            if metadata_bytes > MAX_METADATA_BYTES {
                return;
            }
            sidecars.push((key, value));
        }
        if total > self.max_entry_bytes() {
            return;
        }
        let raw_key = cache_key(&self.settings.namespace, target, login, query_id);
        if sidecars.iter().any(|(key, _)| key == &raw_key) {
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
            .arg(raw_key);
        for (key, _) in &sidecars {
            cmd.arg(key);
        }
        cmd.arg(self.settings.ttl_seconds).arg(data);
        for (_, value) in sidecars {
            cmd.arg(value);
        }
        if self.command(cmd).await.is_some() {
            tracing::debug!(
                query_id,
                target,
                backend_login = login,
                "query and data contexts cached"
            );
        }
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
            let key = key(&model.settings().namespace, target, login, query_id, column);
            key_bytes = key_bytes.saturating_add(key.len());
            if key_bytes > MAX_METADATA_BYTES
                || key_bytes > self.max_entry_bytes().saturating_sub(buffer.bytes.len())
            {
                return Classifications::default();
            }
            cmd.arg(key);
        }
        let budget = MAX_METADATA_BYTES
            .min(
                self.max_entry_bytes()
                    .saturating_sub(buffer.bytes.capacity()),
            )
            .min(
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
            let Ok(sidecar) = serde_json::from_slice::<Sidecar<ColumnClassification>>(&value)
            else {
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
