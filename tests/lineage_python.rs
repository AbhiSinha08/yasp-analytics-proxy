#![cfg(feature = "python")]
mod support;
use serde_json::{Map, json};
use std::{fs, time::Duration};
use yasp::{
    catalog::{ColumnId, RelationId},
    config::PythonSettings,
    hooks::{
        CacheEligibility, QueryContextHook, QueryContextRequest, RoutedSelector, SelectionRequest,
        python::PythonRuntime,
    },
    lineage::{AstDataContext, ColumnLineage, DataContexts, LineageSource},
    query::ParsedQuery,
};

#[tokio::test]
async fn raw_tree_and_lineage_are_fresh_copies_for_every_python_stage() {
    let temporary = support::TempDirectory::new("lineage_python");
    let path = temporary.0.join("hooks.py");
    fs::write(
        &path,
        r#"
def check(request):
    assert 'Query' in request['query_ast']
    source = request['data_contexts']['ast']['columns'][0]['sources'][0]
    assert source['column'] == '"analytics"."public"."users"."id"'
    request['query_ast'].clear()
    request['data_contexts']['ast']['columns'][0]['sources'].clear()

def context(request, params):
    assert "query_ast" not in request and "data_contexts" not in request
    return {"values": {"checked": True}}

def select(request, params):
    check(request)
    assert request['query_context']['checked']
    return {'target':'primary','backend_login':'reader'}

def eligible(request, params):
    check(request)
    return request['query_context']['checked']
"#,
    )
    .unwrap();
    let runtime = PythonRuntime::new(PythonSettings::default()).await.unwrap();
    let context: QueryContextHook = runtime
        .register(&path, "context", Map::new())
        .await
        .unwrap()
        .into();
    let selector: RoutedSelector = runtime
        .register(&path, "select", Map::new())
        .await
        .unwrap()
        .into();
    let eligibility: CacheEligibility = runtime
        .register(&path, "eligible", Map::new())
        .await
        .unwrap()
        .into();
    let query = ParsedQuery::parse("SELECT id FROM users").unwrap();
    let data = DataContexts {
        ast: Some(AstDataContext {
            columns: vec![ColumnLineage {
                ordinal: 0,
                name: Some("id".into()),
                sources: vec![LineageSource::Column {
                    column: ColumnId {
                        relation: RelationId {
                            database: "analytics".into(),
                            schema: "public".into(),
                            name: "users".into(),
                        },
                        name: "id".into(),
                    },
                    path: Vec::new(),
                }],
            }],
            unknown_outputs: false,
        }),
    };
    let derived = context
        .derive(
            QueryContextRequest {
                authenticated_user: "bi",
                frontend_database: "analytics",
                client_id: 1,

                datasets: None,

                sql: (query).sql(),
                gateway_dialect: yasp::query::SqlDialect::Postgresql,
                default_target_dialect: Some(yasp::query::SqlDialect::Postgresql),
            },
            Duration::from_secs(1),
            1024,
        )
        .await
        .unwrap()
        .values;
    assert_eq!(
        derived,
        json!({"checked":true}).as_object().unwrap().clone()
    );
    let request = SelectionRequest {
        authenticated_user: "bi",
        client_id: 1,
        query_id: query.query_id(),
        query: &query,
        query_context: &derived,
        datasets: None,
        async_hook_timeout_ms: 2000,
        data_contexts: &data,
    };
    selector.select(&request).await.unwrap();
    assert!(eligibility.eligible(&request).await.unwrap());
    assert_eq!(data.ast.as_ref().unwrap().columns[0].sources.len(), 1);
    assert!(query.serialized_ast().get("Query").is_some());
    runtime.shutdown();
}
