//! Context callback behavior independent of a database or BI client.

use serde_json::{Map, Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use yasp::{
    hooks::{ContextError, ContextHook, ContextRequest, bind_context},
    query::ParsedQuery,
};

fn request(query: &ParsedQuery, client_id: u64) -> ContextRequest<'_> {
    ContextRequest {
        data_contexts: &yasp::lineage::DataContexts::EMPTY,
        authenticated_user: "bi-service",
        frontend_database: "analytics",
        client_id,
        query_id: query.query_id(),
        query,
        datasets: None,
    }
}

#[tokio::test]
async fn async_context_contract() {
    let query = ParsedQuery::parse("SELECT 1 /* original SQL */").unwrap();
    let hook: ContextHook = bind_context((), |request, _| {
        Box::pin(async move {
            tokio::task::yield_now().await;
            assert_eq!(request.authenticated_user, "bi-service");
            assert_eq!(request.frontend_database, "analytics");
            assert_eq!(request.query_id, request.query.query_id());
            assert_eq!(request.query.sql(), "SELECT 1 /* original SQL */");
            Ok(
                json!({"client": request.client_id, "principal": {"email": "viewer@example.test"}})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
        })
    })
    .into();
    let (first, second) = tokio::join!(
        hook.derive(request(&query, 1), Duration::from_secs(1), 256),
        hook.derive(request(&query, 2), Duration::from_secs(1), 256),
    );
    assert_eq!(first["client"], 1);
    assert_eq!(second["client"], 2);
    assert_eq!(first["principal"], second["principal"]);

    let failed: ContextHook =
        bind_context((), |_, _| Box::pin(async { Err(ContextError::Failed) })).into();
    assert!(
        failed
            .derive(request(&query, 1), Duration::from_secs(1), 256)
            .await
            .is_empty()
    );
    for value in [json!({}), json!({"data": "x".repeat(256)})] {
        let hook: ContextHook = bind_context(value.as_object().unwrap().clone(), |_, state| {
            Box::pin(async move { Ok(state.as_ref().clone()) })
        })
        .into();
        assert!(
            hook.derive(request(&query, 1), Duration::from_secs(1), 32)
                .await
                .is_empty()
        );
    }

    // The context limit counts encoded UTF-8 JSON, including string escapes.
    let hook: ContextHook = bind_context(
        json!({"text": "\n".repeat(16)})
            .as_object()
            .unwrap()
            .clone(),
        |_, state| Box::pin(async move { Ok(state.as_ref().clone()) }),
    )
    .into();
    assert!(
        hook.derive(request(&query, 1), Duration::from_secs(1), 32)
            .await
            .is_empty()
    );
    for (layers, accepted) in [(63, true), (64, false)] {
        let mut nested = Value::Null;
        for _ in 0..layers {
            nested = Value::Array(vec![nested]);
        }
        let hook: ContextHook =
            bind_context(Map::from_iter([("nested".into(), nested)]), |_, state| {
                Box::pin(async move { Ok(state.as_ref().clone()) })
            })
            .into();
        assert_eq!(
            !hook
                .derive(request(&query, 1), Duration::from_secs(1), 1024)
                .await
                .is_empty(),
            accepted
        );
    }

    let completed = Arc::new(AtomicBool::new(false));
    let slow: ContextHook = bind_context(completed.clone(), |_, completed| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            completed.store(true, Ordering::SeqCst);
            Ok(json!({"late": true}).as_object().unwrap().clone())
        })
    })
    .into();
    assert!(
        slow.derive(request(&query, 1), Duration::from_millis(5), 256)
            .await
            .is_empty()
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        !completed.load(Ordering::SeqCst),
        "a timed-out Rust future is dropped"
    );
}
