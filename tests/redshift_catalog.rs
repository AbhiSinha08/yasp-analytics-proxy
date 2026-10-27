//! Native Redshift catalog loading and captured BI discovery over both frontends.
#![cfg(feature = "redshift-tests")]

#[path = "support/redshift_host.rs"]
mod redshift_host;
mod support;

use std::{sync::Arc, time::Duration};
use yasp::{
    backend::PostgresBackend,
    catalog::{Catalog, CatalogError, CatalogSettings, ConstraintKind, RelationId},
    config::{BackendConfig, ClientTlsSettings, PgWireEngine},
    query::SqlDialect,
};

fn backend(fixture: &redshift_host::Fixture, user: &str, password: &str) -> Arc<PostgresBackend> {
    Arc::new(
        PostgresBackend::new(
            BackendConfig::new_with_tls(
                fixture.source.host.clone(),
                fixture.source.port,
                user.into(),
                password.into(),
                fixture.source.identity.database.clone(),
                ClientTlsSettings {
                    ca_file: Some(fixture.ca.clone()),
                    ..Default::default()
                },
            )
            .unwrap()
            .with_engine(PgWireEngine::Redshift),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn native_definitions_refresh_without_postgresql_specific_catalog_facts() {
    let fixture = redshift_host::Fixture::new();
    let source = backend(
        &fixture,
        &fixture.source.identity.user,
        &fixture.source.identity.password,
    );
    let mut settings = CatalogSettings::new(vec!["public".into(), "analytics".into()]);
    settings.refresh_interval = Duration::from_millis(100);
    let catalog = Catalog::redshift(source.clone(), settings).await.unwrap();
    let first = catalog.snapshot().unwrap();
    assert_eq!(first.source.dialect, SqlDialect::Redshift);
    assert_eq!(first.source.principal, fixture.source.identity.user);
    assert!(first.postgres.is_none());
    let id = |schema: &str, name: &str| RelationId {
        database: fixture.source.identity.database.clone(),
        schema: schema.into(),
        name: name.into(),
    };
    let users = &first.relations[&id("public", "users")];
    assert_eq!(users.comment.as_deref(), Some("Demo customers"));
    assert_eq!(
        users.columns[0].comment.as_deref(),
        Some("Customer identifier")
    );
    assert_eq!(users.columns[1].native_type.character_length, Some(100));
    assert!(
        users
            .constraints
            .as_ref()
            .unwrap()
            .iter()
            .any(|key| key.kind == ConstraintKind::PrimaryKey && key.columns == ["id"])
    );
    let orders = &first.relations[&id("analytics", "orders")];
    assert_eq!(orders.columns[2].native_type.precision, Some(30));
    assert_eq!(orders.columns[2].native_type.scale, Some(9));
    assert!(orders.constraints.as_ref().unwrap().iter().any(|key| {
        key.references
            .as_ref()
            .is_some_and(|r| r.relation == id("public", "users") && r.columns == ["id"])
    }));
    let payload = &first.relations[&id("analytics", "events")].columns[1];
    assert_eq!(payload.native_type.name, "super");
    assert!(
        payload.native_type.postgres_oid.is_none(),
        "an emulator domain OID is not a native Redshift SUPER OID"
    );
    assert!(
        first.relations[&id("analytics", "daily_sales")]
            .view_definition
            .as_ref()
            .is_some_and(|sql| sql.contains("orders"))
    );
    support::assert_success(
        &fixture.query("ALTER TABLE public.users ADD COLUMN refreshed integer"),
        "change native definition",
    );
    let next = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = catalog.snapshot().unwrap();
            if snapshot.relations[&id("public", "users")].columns.len() == 5 {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("periodic native catalog refresh");
    assert!(next.generation > first.generation);
    assert_eq!(first.relations[&id("public", "users")].columns.len(), 4);
    catalog.stop_refreshing().await;
    drop(catalog);
    drop(source);
}

#[tokio::test]
async fn source_grants_bounds_and_expiry_keep_unpublished_metadata_out() {
    let fixture = redshift_host::Fixture::new();
    support::assert_success(
        &fixture.query("CREATE TABLE public.owner_only (secret varchar(30))"),
        "owner-only relation",
    );
    let reader = backend(
        &fixture,
        "yasp_reader",
        &std::env::var("YASP_REDSHIFT_READER_PASSWORD")
            .unwrap_or_else(|_| "yasp_reader_password".into()),
    );
    let settings = CatalogSettings::new(vec!["public".into(), "analytics".into()]);
    let catalog = Catalog::redshift(reader.clone(), settings.clone())
        .await
        .unwrap();
    let snapshot = catalog.snapshot().unwrap();
    assert!(snapshot.relations.keys().any(|id| id.name == "users"));
    assert!(!snapshot.relations.keys().any(|id| id.name == "owner_only"));
    let users = snapshot
        .relations
        .values()
        .find(|r| r.id.name == "users")
        .unwrap();
    assert!(
        users
            .constraints
            .as_ref()
            .unwrap()
            .iter()
            .any(|key| key.kind == ConstraintKind::PrimaryKey && key.columns == ["id"])
    );
    catalog.stop_refreshing().await;
    drop(catalog);
    drop(reader);

    let denied = backend(
        &fixture,
        "yasp_denied",
        &std::env::var("YASP_REDSHIFT_DENIED_PASSWORD")
            .unwrap_or_else(|_| "yasp_denied_password".into()),
    );
    let catalog = Catalog::redshift(denied.clone(), settings).await.unwrap();
    assert!(catalog.snapshot().unwrap().relations.is_empty());
    catalog.stop_refreshing().await;
    drop(catalog);
    drop(denied);

    let source = backend(
        &fixture,
        &fixture.source.identity.user,
        &fixture.source.identity.password,
    );
    let mut settings = CatalogSettings::new(vec!["public".into(), "analytics".into()]);
    settings.max_rows = 64;
    settings.refresh_interval = Duration::from_millis(100);
    settings.max_age = Duration::from_millis(500);
    let catalog = Catalog::redshift(source.clone(), settings.clone())
        .await
        .unwrap();
    catalog.stop_refreshing().await;
    let pinned = catalog.snapshot().unwrap();
    let columns = (0..60)
        .map(|i| format!("ADD COLUMN column_{i} integer"))
        .collect::<Vec<_>>()
        .join(", ");
    support::assert_success(
        &fixture.query(&format!("ALTER TABLE public.users {columns}")),
        "exceed native catalog row limit",
    );
    assert_eq!(catalog.refresh().await, Err(CatalogError::TooLarge));
    assert_eq!(catalog.snapshot().unwrap().generation, pinned.generation);
    tokio::time::sleep(Duration::from_millis(550)).await;
    assert!(matches!(catalog.snapshot(), Err(CatalogError::Expired)));
    settings.max_bytes = 1;
    assert!(matches!(
        Catalog::redshift(source.clone(), settings).await,
        Err(CatalogError::TooLarge)
    ));
    drop(catalog);
    drop(source);
}

#[test]
fn captured_bi_discovery_uses_native_redshift_facts_through_both_profiles() {
    for profile in ["postgresql", "redshift"] {
        redshift_host::with_host(profile, |_, _, _, query| {
            let jdbc: serde_json::Value = serde_json::from_str(if profile == "redshift" {
                include_str!("fixtures/jdbc_redshift.json")
            } else {
                include_str!("fixtures/jdbc_postgresql.json")
            })
            .unwrap();
            let result = query(jdbc["queries"][1].as_str().unwrap());
            support::assert_success(&result, "JDBC schema browsing");
            assert!(String::from_utf8_lossy(&result.stdout).contains("analytics"));
            for (index, captured) in jdbc["queries"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .take(9)
                .skip(1)
            {
                support::assert_success(
                    &query(captured.as_str().unwrap()),
                    &format!("DBeaver/JDBC {profile} captured metadata {index}"),
                );
            }

            let metabase: serde_json::Value = serde_json::from_str(if profile == "redshift" {
                include_str!("fixtures/metabase_redshift.json")
            } else {
                include_str!("fixtures/metabase_discovery.json")
            })
            .unwrap();
            let captures = if profile == "redshift" {
                &metabase["queries"]
            } else {
                &metabase
            };
            for (index, captured) in captures.as_array().unwrap().iter().enumerate() {
                let sql = if profile == "redshift" {
                    captured["sql"].as_str().unwrap()
                } else {
                    captured.as_str().unwrap()
                };
                support::assert_success(
                    &query(sql),
                    &format!("Metabase {profile} captured metadata {index}"),
                );
            }
            let filtered =
                include_str!("../src/gateway/metadata/queries/metabase_foreign_keys_filtered.sql");
            let result =
                query(&filtered.replace("'__YASP_SCHEMAS__'", "('analytics'), ('public')"));
            support::assert_success(&result, "Metabase schema-filtered native foreign keys");
            let rows = String::from_utf8_lossy(&result.stdout);
            assert!(
                rows.contains("analytics|orders|user_id|public|users|id"),
                "{rows}"
            );
            let composite: Vec<_> = rows
                .lines()
                .filter(|row| row.starts_with("analytics|type_references|"))
                .collect();
            assert_eq!(
                composite,
                [
                    "analytics|type_references|tenant_id|analytics|metadata_types|tenant_id",
                    "analytics|type_references|type_id|analytics|metadata_types|type_id",
                ]
            );
            let absent = query(&filtered.replace("__YASP_SCHEMAS__", "absent"));
            support::assert_success(&absent, "absent Metabase foreign-key schema");
            assert!(absent.stdout.is_empty());
            let sql = if profile == "redshift" {
                metabase["queries"][2]["sql"].as_str().unwrap()
            } else {
                metabase[2].as_str().unwrap()
            };
            let sql = if profile == "redshift" {
                sql.replace("'public'", "'analytics'")
                    .replace("'users'", "'events'")
            } else {
                sql.replace("IN (('public'))", "IN (('analytics'))")
            };
            let result = query(&sql);
            support::assert_success(
                &result,
                &format!("Metabase {profile} native field discovery"),
            );
            let rows = String::from_utf8_lossy(&result.stdout);
            assert!(
                rows.contains(if profile == "redshift" {
                    "payload|super|"
                } else {
                    "payload|json|"
                }),
                "{rows}"
            );

            let redash = query(if profile == "redshift" {
                include_str!("../src/gateway/metadata/queries/redash_redshift_columns.sql")
            } else {
                include_str!("../src/gateway/metadata/queries/redash_postgresql_columns.sql")
            });
            support::assert_success(&redash, "Redash native autocomplete");
            assert!(String::from_utf8_lossy(&redash.stdout).contains("payload"));

            let superset: serde_json::Value = serde_json::from_str(if profile == "redshift" {
                include_str!("fixtures/superset_redshift.json")
            } else {
                include_str!("fixtures/superset_postgresql.json")
            })
            .unwrap();
            for (index, captured) in superset["queries"].as_array().unwrap().iter().enumerate() {
                let sql = if profile == "redshift" {
                    captured["sql"].as_str().unwrap()
                } else {
                    captured.as_str().unwrap()
                };
                let result = query(sql);
                support::assert_success(
                    &result,
                    &format!("Superset {profile} captured metadata {index}"),
                );
                if index == 0 {
                    assert!(!result.stdout.is_empty());
                }
            }
        });
    }
}
