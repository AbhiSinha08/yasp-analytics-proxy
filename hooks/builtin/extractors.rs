//! Extract client-supplied metadata from observed BI SQL remarks.

use serde_json::{Map, Value};

#[derive(Debug, thiserror::Error)]
pub enum ExtractionError {
    #[error("BI metadata extraction is not implemented")]
    NotImplemented,
}

fn metabase_fields(comment: &str) -> Option<Map<String, Value>> {
    let Some(comment) = comment.strip_prefix("Metabase::") else {
        return None;
    };
    let Some(comment) = comment.lines().next() else {
        return None;
    };
    let mut fields = comment.split_whitespace();
    if fields.next() != Some("userID:") {
        return None;
    }
    let Some(user_id) = fields.next().and_then(|value| value.parse::<u64>().ok()) else {
        return None;
    };
    if fields.next() != Some("queryType:") {
        return None;
    }
    let Some(query_type) = fields
        .next()
        .filter(|value| !value.is_empty() && value.len() <= 64)
    else {
        return None;
    };
    if fields.next() != Some("queryHash:") {
        return None;
    }
    let Some(query_hash) = fields
        .next()
        .filter(|value| !value.is_empty() && value.len() <= 128)
    else {
        return None;
    };
    if fields.next().is_some() {
        return None;
    }

    let mut metadata = Map::new();
    metadata.insert("tool".into(), Value::String("metabase".into()));
    metadata.insert("user_id".into(), Value::from(user_id));
    metadata.insert("query_type".into(), Value::String(query_type.into()));
    metadata.insert("query_hash".into(), Value::String(query_hash.into()));
    Some(metadata)
}

pub fn metabase_pg(sql: &str) -> Result<Option<Map<String, Value>>, ExtractionError> {
    let Some(comment) = sql.trim_start().strip_prefix("-- ") else {
        return Ok(None);
    };
    Ok(metabase_fields(comment))
}

pub fn metabase_rs(sql: &str) -> Result<Option<Map<String, Value>>, ExtractionError> {
    let Some(comment) = sql
        .trim_start()
        .strip_prefix("-- /* partner: \"metabase\", ")
    else {
        return Ok(None);
    };
    let Some((partner_json, remark)) = comment.split_once(" */ ") else {
        return Ok(None);
    };
    let Ok(Value::Object(partner)) = serde_json::from_str::<Value>(partner_json) else {
        return Ok(None);
    };

    let valid_nullable_id = |key: &str| {
        partner
            .get(key)
            .is_some_and(|value| value.is_null() || value.as_u64().is_some())
    };
    if !valid_nullable_id("dashboard_id")
        || !valid_nullable_id("chart_id")
        || !valid_nullable_id("optional_user_id")
        || !partner
            .get("optional_account_id")
            .is_some_and(|value| value.is_null() || value.is_string())
        || !partner.get("filter_values").is_some_and(Value::is_object)
    {
        return Ok(None);
    }

    let Some(mut metadata) = metabase_fields(remark) else {
        return Ok(None);
    };
    for key in [
        "dashboard_id",
        "chart_id",
        "optional_user_id",
        "optional_account_id",
        "filter_values",
    ] {
        metadata.insert(key.into(), partner[key].clone());
    }
    Ok(Some(metadata))
}

pub fn superset(_sql: &str) -> Result<Option<Map<String, Value>>, ExtractionError> {
    Err(ExtractionError::NotImplemented)
}

pub fn redash(sql: &str) -> Result<Option<Map<String, Value>>, ExtractionError> {
    let Some(comment) = sql.trim_start().strip_prefix("/*") else {
        return Ok(None);
    };
    let Some((comment, _)) = comment.split_once("*/") else {
        return Ok(None);
    };
    let mut user_id = None;
    let mut query_id = None;
    let mut job_id = None;
    let mut query_hash = None;
    let mut scheduled = None;
    for field in comment.split(',') {
        let Some((key, value)) = field.trim().split_once(':') else {
            return Ok(None);
        };
        let value = value.trim();
        match key.trim() {
            "user_id" if user_id.is_none() => {
                user_id = value.parse::<u64>().ok();
                if user_id.is_none() {
                    return Ok(None);
                }
            }
            "query_id" if query_id.is_none() => {
                query_id = value.parse::<u64>().ok();
                if query_id.is_none() {
                    return Ok(None);
                }
            }
            "Job ID" if job_id.is_none() && !value.is_empty() && value.len() <= 128 => {
                job_id = Some(value);
            }
            "Query Hash" if query_hash.is_none() && !value.is_empty() && value.len() <= 128 => {
                query_hash = Some(value);
            }
            "Scheduled" if scheduled.is_none() => {
                scheduled = match value.to_ascii_lowercase().as_str() {
                    "true" => Some(true),
                    "false" => Some(false),
                    _ => None,
                };
                if scheduled.is_none() {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        }
    }
    let (Some(user_id), Some(query_id), Some(job_id), Some(query_hash), Some(scheduled)) =
        (user_id, query_id, job_id, query_hash, scheduled)
    else {
        return Ok(None);
    };

    let mut metadata = Map::new();
    metadata.insert("tool".into(), Value::String("redash".into()));
    metadata.insert("user_id".into(), Value::from(user_id));
    metadata.insert("query_id".into(), Value::from(query_id));
    metadata.insert("job_id".into(), Value::String(job_id.into()));
    metadata.insert("query_hash".into(), Value::String(query_hash.into()));
    metadata.insert("scheduled".into(), Value::Bool(scheduled));
    Ok(Some(metadata))
}

/// Return a normalized remark from one of the observed PostgreSQL BI clients.
/// These values are diagnostic hints and must not be used as authenticated identity.
pub fn observed_postgres_remark(sql: &str) -> Option<Map<String, Value>> {
    metabase_pg(sql)
        .ok()
        .flatten()
        .or_else(|| redash(sql).ok().flatten())
}

/// Remove an observed BI remark before logging the SQL text itself.
pub fn sql_without_observed_postgres_remark(sql: &str) -> &str {
    let leading = sql.trim_start();
    if let Some(comment) = leading.strip_prefix("-- Metabase::") {
        return comment
            .find('\n')
            .map(|end| &comment[end + 1..])
            .unwrap_or("");
    }
    if let Some(comment) = leading.strip_prefix("/*")
        && comment.trim_start().starts_with("user_id:")
    {
        return comment.split_once("*/").map(|(_, rest)| rest).unwrap_or("");
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::{
        metabase_pg, metabase_rs, observed_postgres_remark, redash,
        sql_without_observed_postgres_remark,
    };
    use serde_json::{Value, json};

    #[test]
    fn parses_leading_metabase_postgres_remark_with_json_types() {
        let metadata = metabase_pg(
            "  -- Metabase:: userID: 42 queryType: native queryHash: 0123456789abcdef\nSELECT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            metadata,
            json!({
                "tool": "metabase",
                "user_id": 42,
                "query_type": "native",
                "query_hash": "0123456789abcdef"
            })
            .as_object()
            .unwrap()
            .clone()
        );
        assert!(metadata["user_id"].is_u64());
    }

    #[test]
    fn parses_leading_metabase_redshift_remark() {
        let metadata = metabase_rs(
            "-- /* partner: \"metabase\", {\"dashboard_id\":null,\"chart_id\":57,\"optional_user_id\":1,\"optional_account_id\":\"account-1\",\"filter_values\":{\"region\":\"west\"}} */ Metabase:: userID: 1 queryType: MBQL queryHash: c33e2be5\nSELECT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(metadata["tool"], "metabase");
        assert_eq!(metadata["user_id"], 1);
        assert_eq!(metadata["query_type"], "MBQL");
        assert_eq!(metadata["query_hash"], "c33e2be5");
        assert_eq!(metadata["dashboard_id"], Value::Null);
        assert_eq!(metadata["chart_id"], 57);
        assert_eq!(metadata["optional_user_id"], 1);
        assert_eq!(metadata["optional_account_id"], "account-1");
        assert_eq!(metadata["filter_values"]["region"], "west");
        assert!(metadata["chart_id"].is_u64());
    }

    #[test]
    fn parses_leading_redash_postgres_remark_with_json_types() {
        let metadata = redash(
            "/* user_id: 7, query_id: 19, Job ID: 4dd4b53e-a642-4e1f-b249-67ba309f55e1, Query Hash: 0123456789abcdef0123456789abcdef, Scheduled: False */ SELECT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(metadata["tool"], "redash");
        assert_eq!(metadata["user_id"], 7);
        assert_eq!(metadata["query_id"], 19);
        assert_eq!(metadata["job_id"], "4dd4b53e-a642-4e1f-b249-67ba309f55e1");
        assert_eq!(metadata["query_hash"], "0123456789abcdef0123456789abcdef");
        assert_eq!(metadata["scheduled"], false);
        assert!(metadata["user_id"].is_u64());
        assert!(metadata["scheduled"].is_boolean());

        let scheduled = redash(
            "/* user_id: 7, query_id: 19, Job ID: abc, Query Hash: hash, Scheduled: True */ SELECT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(scheduled["scheduled"], true);
    }

    #[test]
    fn ignores_missing_malformed_and_nonleading_metadata() {
        for sql in [
            "SELECT 1",
            "SELECT '-- Metabase:: userID: 42 queryType: native queryHash: abc'",
            "SELECT 1 /* user_id: 7, query_id: 19, Job ID: abc, Query Hash: hash, Scheduled: False */",
            "-- Metabase:: userID: invalid queryType: native queryHash: abc\nSELECT 1",
            "-- Metabase:: userID: 42 queryType: native\nSELECT 1",
            "/* user_id: 7, query_id: 19, Job ID: abc, Query Hash: hash */ SELECT 1",
            "/* user_id: 7, query_id: 19, Job ID: abc, Query Hash: hash, Scheduled: maybe */ SELECT 1",
            "/* user_id: 7, user_id: 8, query_id: 19, Job ID: abc, Query Hash: hash, Scheduled: False */ SELECT 1",
            "/* user_id: 7, query_id: 19, Job ID: abc, Query Hash: hash, Scheduled: False SELECT 1",
        ] {
            assert_eq!(observed_postgres_remark(sql), None, "{sql}");
        }
        for sql in [
            "SELECT 1 -- /* partner: \"metabase\", {} */ Metabase:: userID: 42 queryType: native queryHash: abc",
            "-- /* partner: \"metabase\", {bad json} */ Metabase:: userID: 42 queryType: native queryHash: abc",
            "-- /* partner: \"metabase\", {\"dashboard_id\":null} */ Metabase:: userID: 42 queryType: native queryHash: abc",
            "-- /* partner: \"metabase\", {\"dashboard_id\":null,\"chart_id\":null,\"optional_user_id\":null,\"optional_account_id\":null,\"filter_values\":[]} */ Metabase:: userID: 42 queryType: native queryHash: abc",
        ] {
            assert_eq!(metabase_rs(sql).unwrap(), None, "{sql}");
        }
    }

    #[test]
    fn removes_leading_bi_remark_from_logged_sql() {
        assert_eq!(
            sql_without_observed_postgres_remark(
                "  -- Metabase:: userID: 42 queryType: native queryHash: abc\nSELECT 1"
            ),
            "SELECT 1"
        );
        assert_eq!(
            sql_without_observed_postgres_remark("/* user_id: malformed */ SELECT 1"),
            " SELECT 1"
        );
        assert_eq!(sql_without_observed_postgres_remark("SELECT 1"), "SELECT 1");
    }
}
