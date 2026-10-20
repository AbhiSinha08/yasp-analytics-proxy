//! Snapshot-only reflection remains usable without a reachable PostgreSQL source.
mod support;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use yasp::{
    backend::PostgresBackend,
    catalog::{Catalog, CatalogSettings},
    config::GatewaySettings,
    hooks::bind_context,
};

#[tokio::test]
async fn postgres_reflection_pins_source_facts_and_never_falls_back() {
    let env = support::TestEnvironment::load();
    let source = env.source(None);
    let database = support::TestDatabase::create(&source, "yasp_snapshot_gateway");
    database.connection.execute("CREATE TYPE public.mood AS ENUM ('happy','quiet'); CREATE DOMAIN public.positive AS integer CHECK (VALUE > 0); CREATE TABLE public.parent (id integer PRIMARY KEY); CREATE TABLE public.child (id bigint GENERATED ALWAYS AS IDENTITY (START WITH 7 INCREMENT BY 3), parent_id integer REFERENCES public.parent(id), state public.mood, amount public.positive, code text UNIQUE, doubled integer GENERATED ALWAYS AS (parent_id * 2) VIRTUAL, parent2_id integer REFERENCES public.parent(id)); CREATE INDEX child_parent_idx ON public.child (parent_id DESC) WHERE parent_id IS NOT NULL; CREATE VIEW public.child_view AS SELECT parent_id FROM public.child; CREATE TABLE public.partitioned (id integer) PARTITION BY RANGE (id); CREATE SCHEMA excluded; CREATE TABLE excluded.secret (id integer)");
    let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
    let mut settings = CatalogSettings::new(vec!["public".into()]);
    settings.refresh_interval = Duration::from_secs(2);
    settings.max_age = Duration::from_secs(8);
    let catalog = Catalog::postgres(backend.clone(), settings).await.unwrap();
    catalog.stop_refreshing().await;
    let snapshot = catalog.snapshot().unwrap();
    let pg = snapshot.postgres.as_ref().unwrap();
    let child = pg.relations.values().find(|r| r.name == "child").unwrap();
    let oid = child.oid.to_string();
    assert_eq!(
        child.columns[&1].identity_options.as_ref().unwrap()["start"],
        7
    );
    assert_eq!(child.columns[&6].generated, "v");
    assert!(
        child
            .indexes
            .iter()
            .any(|i| i.name == "child_parent_idx" && i.direction.as_deref() == Some("D"))
    );
    assert!(child.constraints.iter().any(|k| k.kind == "f"));
    assert!(pg.types.iter().any(|t| t.name == "positive"));
    assert!(pg.types.iter().any(|t| t.label.as_deref() == Some("happy")));
    assert!(pg.relations.values().all(|r| r.schema == "public"));
    let invoked = Arc::new(AtomicUsize::new(0));
    let calls = invoked.clone();
    let config = env
        .gateway()
        .gateway_config()
        .with_catalog(catalog.clone())
        .with_settings(GatewaySettings {
            query_timeout_ms: 1000,
            ..Default::default()
        })
        .unwrap()
        .with_context_hook(bind_context(calls, |_, calls| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Default::default()) })
        }));
    let server = support::GatewayServer::fixed(config, backend);
    let client = env.gateway().connection(server.address);
    let columns = include_str!("../src/gateway/metadata/queries/sqlalchemy_postgresql_columns.sql")
        .replace("'__YASP_OID__'", &oid);
    let indexes = include_str!("../src/gateway/metadata/queries/sqlalchemy_postgresql_indexes.sql")
        .replace("'__YASP_OID__'", &oid);
    let enums = include_str!("../src/gateway/metadata/queries/sqlalchemy_postgresql_enums.sql");
    let domains = include_str!("../src/gateway/metadata/queries/sqlalchemy_postgresql_domains.sql");
    let tables = include_str!("../src/gateway/metadata/queries/postgresql_tables.sql")
        .replace("__YASP_SCHEMA__", "public")
        .replace("__YASP_TABLE__", "%");
    let keys = include_str!("../src/gateway/metadata/queries/postgresql_foreign_keys.sql")
        .replace("__YASP_SCHEMA__", "public")
        .replace("__YASP_TABLE__", "child");
    for sql in [&columns, &indexes, enums, domains, &keys, &tables] {
        let direct = database.connection.query(sql);
        support::assert_success(&direct, "direct reflection");
        let result = client.query(sql);
        support::assert_success(&result, "snapshot reflection");
        // Identity JSON whitespace and index row ordering are connector formatting details.
        if sql == enums || sql == keys || sql == tables {
            assert_eq!(result.stdout, direct.stdout);
        }
        if sql == domains {
            assert!(
                String::from_utf8_lossy(&result.stdout).contains("positive|integer|t||t|public")
            );
        }
    }
    let rows = client.query(&columns);
    let rows = String::from_utf8_lossy(&rows.stdout);
    assert!(
        rows.contains("positive") && rows.contains("mood") && rows.contains("\"start\":7"),
        "{rows}"
    );
    database
        .connection
        .execute("ALTER TABLE public.child ADD COLUMN refreshed integer");
    assert!(!String::from_utf8_lossy(&client.query(&columns).stdout).contains("refreshed"));
    catalog.refresh().await.unwrap();
    assert!(String::from_utf8_lossy(&client.query(&columns).stdout).contains("refreshed"));
    source.execute(&format!(
        "ALTER DATABASE {} ALLOW_CONNECTIONS false",
        support::quote_ident(&database.connection.identity.database)
    ));
    source.execute(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
        database.connection.identity.database
    ));
    assert!(catalog.refresh().await.is_err());
    for sql in [
        &columns,
        &indexes,
        enums,
        domains,
        "SHOW server_version",
        "SELECT current_schema()",
    ] {
        support::assert_success(&client.query(sql), "snapshot with unavailable source");
    }
    let unknown = client.query("SELECT relname FROM pg_catalog.pg_class");
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("0A000"));
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    let result = client.queries(&[
        "BEGIN",
        "SELECT relname FROM pg_catalog.pg_class",
        "SELECT current_schema()",
        "ROLLBACK",
        "SELECT current_schema()",
    ]);
    assert!(String::from_utf8_lossy(&result.stderr).contains("25P02"));
    tokio::time::sleep(Duration::from_secs(8)).await;
    let expired = client.query(&columns);
    assert!(String::from_utf8_lossy(&expired.stderr).contains("55000"));
    assert_eq!(
        snapshot
            .relations
            .values()
            .find(|r| r.id.name == "child")
            .unwrap()
            .columns
            .len(),
        7
    );
    server.stop();
}

#[tokio::test]
async fn serving_requires_a_matching_catalog() {
    let env = support::TestEnvironment::load();
    let database = support::TestDatabase::create(&env.source(None), "yasp_catalog_startup");
    let backend = Arc::new(PostgresBackend::new(database.connection.backend_config()).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let error = yasp::gateway::serve(
        listener,
        env.gateway().gateway_config(),
        backend.clone(),
        std::future::pending(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("requires a catalog"));
    let other = Arc::new(PostgresBackend::new(env.source(None).backend_config()).unwrap());
    let catalog = Catalog::postgres(other, CatalogSettings::new(vec!["public".into()]))
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let error = yasp::gateway::serve(
        listener,
        env.gateway().gateway_config().with_catalog(catalog.clone()),
        backend,
        std::future::pending(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("identities must match"));
    catalog.stop_refreshing().await;
}
