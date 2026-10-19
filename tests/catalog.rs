//! Schema generations, source spelling, bounded refreshes and stale failures.
mod support;
use std::{sync::Arc, time::Duration};
use yasp::{
    backend::PostgresBackend,
    catalog::{Catalog, CatalogError, CatalogSettings, RelationId},
};

#[tokio::test]
async fn source_schema_refresh_is_atomic_and_old_generations_keep_their_origins() {
    let env = support::TestEnvironment::load();
    let database = support::TestDatabase::create(&env.source(None), "yasp_catalog");
    database.connection.execute("CREATE SCHEMA \"Mixed.Schema's\"; CREATE TABLE \"Mixed.Schema's\".\"T's\" (\"Column.Name\" numeric(20,9) NOT NULL, other text); COMMENT ON COLUMN \"Mixed.Schema's\".\"T's\".\"Column.Name\" IS 'source spelling'");
    let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
    let mut settings = CatalogSettings::new(vec!["Mixed.Schema's".into()]);
    settings.refresh_interval = Duration::from_millis(100);
    let catalog = Catalog::postgres(backend, settings).await.unwrap();
    let old = catalog.snapshot().unwrap();
    let id = RelationId {
        database: database.connection.identity.database.clone(),
        schema: "Mixed.Schema's".into(),
        name: "T's".into(),
    };
    let relation = &old.relations[&id];
    assert_eq!(relation.columns.len(), 2);
    let column = &relation.columns[0];
    assert_eq!(column.id.name, "Column.Name");
    assert_eq!(column.id.relation, id);
    assert_eq!(column.native_type.precision, Some(20));
    assert_eq!(column.native_type.scale, Some(9));
    assert!(!column.nullable);
    assert_eq!(column.comment.as_deref(), Some("source spelling"));
    assert_eq!(old.source.principal, database.connection.identity.user);
    database
        .connection
        .execute("ALTER TABLE \"Mixed.Schema's\".\"T's\" ADD COLUMN added integer");
    let next = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let next = catalog.snapshot().unwrap();
            if next.relations[&id].columns.len() == 3 {
                break next;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("periodic refresh publishes the DDL");
    assert!(next.generation > old.generation);
    assert_eq!(
        next.relations[&id].columns[0].id,
        old.relations[&id].columns[0].id
    );
    assert_eq!(old.relations[&id].columns.len(), 2);
    assert!(catalog.status().last_error.is_none());
    catalog.stop_refreshing().await;
    let generation = catalog.status().generation;
    database
        .connection
        .execute("ALTER TABLE \"Mixed.Schema's\".\"T's\" ADD COLUMN manual_refresh integer");
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(catalog.status().generation, generation);
    catalog.refresh().await.unwrap();
    assert_eq!(catalog.snapshot().unwrap().relations[&id].columns.len(), 4);
}

#[tokio::test]
async fn refresh_failure_retains_last_schema_then_expires_without_publishing_empty() {
    let env = support::TestEnvironment::load();
    let database = support::TestDatabase::create(&env.source(None), "yasp_catalog_bounds");
    database
        .connection
        .execute("CREATE TABLE small_schema (id integer, value text)");
    let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
    let mut settings = CatalogSettings::new(vec!["public".into()]);
    settings.max_rows = 2;
    settings.refresh_interval = Duration::from_millis(100);
    settings.max_age = Duration::from_millis(800);
    let catalog = Catalog::postgres(backend.clone(), settings.clone())
        .await
        .unwrap();
    let pinned = catalog.snapshot().unwrap();
    database
        .connection
        .execute("ALTER TABLE small_schema ADD COLUMN exceeds_bound text");
    assert_eq!(catalog.refresh().await, Err(CatalogError::TooLarge));
    assert_eq!(catalog.snapshot().unwrap().generation, pinned.generation);
    assert_eq!(catalog.status().last_error, Some(CatalogError::TooLarge));
    assert!(!catalog.status().refreshing);
    tokio::time::sleep(Duration::from_millis(850)).await;
    assert!(matches!(catalog.snapshot(), Err(CatalogError::Expired)));
    assert_eq!(pinned.relations.values().next().unwrap().columns.len(), 2);
    settings.max_rows = 100;
    settings.max_bytes = 1;
    assert!(matches!(
        Catalog::postgres(backend, settings).await,
        Err(CatalogError::TooLarge)
    ));
    catalog.stop_refreshing().await;
}

#[tokio::test]
async fn source_roles_keep_catalogs_and_column_grants_separate() {
    let env = support::TestEnvironment::load();
    let database = support::TestDatabase::create(&env.source(None), "yasp_catalog_roles");
    let reader = env
        .source(Some("secondary"))
        .database(&database.connection.identity.database);
    database.connection.execute(&format!("CREATE TABLE permitted (id integer, secret text); CREATE TABLE hidden (id integer); GRANT SELECT (id) ON permitted TO {}", support::quote_ident(&reader.identity.user)));
    let settings = CatalogSettings::new(vec!["public".into()]);
    let owner = Catalog::postgres(
        Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap()),
        settings.clone(),
    )
    .await
    .unwrap();
    let reader = Catalog::postgres(
        Arc::new(PostgresBackend::new(reader.backend_config()).unwrap()),
        settings,
    )
    .await
    .unwrap();
    let owner_schema = owner.snapshot().unwrap();
    let reader_schema = reader.snapshot().unwrap();
    assert_ne!(owner_schema.source, reader_schema.source);
    assert_eq!(owner_schema.relations.len(), 2);
    assert_eq!(reader_schema.relations.len(), 1);
    let permitted = reader_schema.relations.values().next().unwrap();
    assert_eq!(permitted.id.name, "permitted");
    assert_eq!(permitted.selectable, Some(false));
    assert!(
        owner_schema
            .relations
            .values()
            .all(|r| r.selectable == Some(true))
    );
    assert_eq!(
        permitted
            .columns
            .iter()
            .map(|c| c.id.name.as_str())
            .collect::<Vec<_>>(),
        ["id"]
    );
    owner.stop_refreshing().await;
    reader.stop_refreshing().await;
    drop(owner);
    drop(reader);
}
