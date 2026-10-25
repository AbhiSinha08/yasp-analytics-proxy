//! Raw query context, dialect defaults, BI remarks, and bounded failures.
use serde_json::{Map, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use yasp::{
    hooks::{
        QueryContextError, QueryContextHook, QueryContextRequest, bind_query_context,
        builtin::query_context,
    },
    query::SqlDialect,
};

fn request(sql: &str) -> QueryContextRequest<'_> {
    QueryContextRequest {
        sql,
        authenticated_user: "bi-service",
        frontend_database: "analytics",
        client_id: 7,
        gateway_dialect: SqlDialect::Redshift,
        default_target_dialect: Some(SqlDialect::Postgresql),
        datasets: None,
    }
}

#[test]
fn bi_dialects_are_explicit_and_independent_of_user_policy() {
    for (kind, expected) in [
        ("native", SqlDialect::Postgresql),
        ("MBQL", SqlDialect::Redshift),
        ("unknown", SqlDialect::Redshift),
    ] {
        let sql = format!(
            "-- Metabase:: userID: 7 queryType: {kind} queryHash: abc\nSELECT * FROM users"
        );
        let output = query_context::metabase(request(&sql), None).unwrap();
        assert_eq!(output.dialect, Some(expected));
        assert_eq!(output.values["user_id"], 7);
        let mixed = QueryContextRequest {
            default_target_dialect: None,
            ..request(&sql)
        };
        if kind == "native" {
            assert_eq!(
                query_context::metabase(mixed, None).unwrap_err(),
                QueryContextError::AmbiguousDialect
            );
            assert_eq!(
                query_context::metabase(mixed, Some(SqlDialect::Snowflake))
                    .unwrap()
                    .dialect,
                Some(SqlDialect::Snowflake)
            );
        }
    }
    for sql in [
        "SELECT * FROM users",
        "-- Metabase:: queryType: native\nSELECT 1",
    ] {
        let output = query_context::metabase(request(sql), Some(SqlDialect::Postgresql)).unwrap();
        assert_eq!(output.dialect, Some(SqlDialect::Redshift));
        assert!(output.values.is_empty());
    }
    for id in ["19", "adhoc"] {
        for scheduled in ["True", "False"] {
            let sql = format!(
                "/* user_id: 7, query_id: {id}, Job ID: job, Query Hash: hash, Scheduled: {scheduled} */ SELECT * FROM users"
            );
            let output = query_context::redash(request(&sql), None).unwrap();
            assert_eq!(output.dialect, Some(SqlDialect::Redshift));
            assert_eq!(output.values["scheduled"], scheduled == "True");
            assert_eq!(
                query_context::redash(request(&sql), Some(SqlDialect::Postgresql))
                    .unwrap()
                    .dialect,
                Some(SqlDialect::Postgresql)
            );
        }
    }
    assert_eq!(
        query_context::redash(request("SELECT 1"), Some(SqlDialect::Postgresql))
            .unwrap()
            .dialect,
        Some(SqlDialect::Redshift)
    );
}

#[tokio::test]
async fn async_context_is_bounded_and_failures_propagate() {
    let hook: QueryContextHook = bind_query_context((), |request, _| {
        Box::pin(async move {
            tokio::task::yield_now().await;
            assert_eq!(request.authenticated_user, "bi-service");
            assert_eq!(request.sql, "SELECT 1");
            Ok(json!({"client": request.client_id})
                .as_object()
                .unwrap()
                .clone()
                .into())
        })
    })
    .into();
    let output = hook
        .derive(request("SELECT 1"), Duration::from_secs(1), 256)
        .await
        .unwrap();
    assert_eq!(output.dialect, None);
    assert_eq!(output.values["client"], 7);
    let failed: QueryContextHook = bind_query_context((), |_, _| {
        Box::pin(async { Err(QueryContextError::Failed) })
    })
    .into();
    assert_eq!(
        failed
            .derive(request("SELECT 1"), Duration::from_secs(1), 256)
            .await
            .unwrap_err(),
        QueryContextError::Failed
    );
    let large: QueryContextHook = bind_query_context((), |_, _| {
        Box::pin(async { Ok(Map::from_iter([("value".into(), json!("x".repeat(256)))]).into()) })
    })
    .into();
    assert_eq!(
        large
            .derive(request("SELECT 1"), Duration::from_secs(1), 64)
            .await
            .unwrap_err(),
        QueryContextError::PayloadTooLarge
    );
    let completed = Arc::new(AtomicBool::new(false));
    let slow: QueryContextHook = bind_query_context(completed.clone(), |_, completed| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            completed.store(true, Ordering::SeqCst);
            Ok(Default::default())
        })
    })
    .into();
    assert_eq!(
        slow.derive(request("SELECT 1"), Duration::from_millis(5), 256)
            .await
            .unwrap_err(),
        QueryContextError::TimedOut
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(!completed.load(Ordering::SeqCst));
}
