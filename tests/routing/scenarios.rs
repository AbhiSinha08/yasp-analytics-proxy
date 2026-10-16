use super::*;

#[test]
pub(super) fn routed_gateway_contract() {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    use yasp::{
        backend::PostgresBackends,
        config::{BackendConfig, BackendSettings, GatewayConfig, GatewaySettings},
        hooks::{BackendSelection, SelectionError, bind_select_backend},
    };

    let environment = TestEnvironment::load();
    let identity = environment.gateway();
    let gateway_user = identity.user.clone();
    let gateway_password = identity.password.clone();
    let gateway_database = identity.database.clone();
    let source = environment.source(None);
    let secondary = environment.source(Some("secondary"));
    let source_user = source.identity.user.clone();
    let source_password = source.identity.password.clone();
    let source_host = source.host.clone();
    let source_port = source.port;
    let second_user = secondary.identity.user.clone();
    let second_password = secondary.identity.password.clone();
    assert_ne!(second_user, source_user, "source logins must be distinct");
    let databases = [
        TestDatabase::create(&source, "yasp_route"),
        TestDatabase::create(&source, "yasp_route"),
    ];
    let database_names = databases
        .each_ref()
        .map(|database| database.connection.identity.database.clone());
    for database in &databases {
        database.populate();
    }
    let maintenance = |sql: &str, database: &str| source.database(database).execute(sql);
    let second_id = quote_ident(&second_user);
    maintenance(
        &format!(
            "REVOKE CONNECT ON DATABASE \"{}\" FROM PUBLIC; \
             REVOKE CONNECT ON DATABASE \"{}\" FROM {second_id}",
            database_names[0], database_names[1]
        ),
        "postgres",
    );
    let grants = source.database(&database_names[1]).query(&format!(
        "GRANT CONNECT ON DATABASE \"{}\" TO {second_id}; \
             GRANT USAGE ON SCHEMA public TO {second_id}; \
             GRANT SELECT ON ALL TABLES IN SCHEMA public TO {second_id}",
        database_names[1]
    ));
    assert!(
        grants.status.success(),
        "cannot grant fixture read access to the second source login"
    );
    let role = secondary
        .database(&database_names[1])
        .query("SELECT rolsuper FROM pg_roles WHERE rolname = current_user");
    assert!(
        role.status.success() && String::from_utf8_lossy(&role.stdout).trim() == "f",
        "the second source login must be a non-superuser to verify database ACLs"
    );
    let private_table = source.database(&database_names[1]).query("CREATE TABLE public.yasp_private_probe (secret text); INSERT INTO public.yasp_private_probe VALUES ('owner-only')");
    assert!(
        private_table.status.success(),
        "cannot create the private source ACL probe table"
    );

    let route_one = BackendSelection {
        target: "target_one".into(),
        backend_login: "reader".into(),
    };
    let route_two = BackendSelection {
        target: "target_two".into(),
        backend_login: "reader".into(),
    };
    let route_owner = BackendSelection {
        target: "target_two".into(),
        backend_login: "owner".into(),
    };
    let unreachable = BackendSelection {
        target: "unreachable".into(),
        backend_login: "reader".into(),
    };
    let backend_config = |username: String, password: String, database: String, port| {
        BackendConfig::new(source_host.clone(), port, username, password, database)
            .expect("test backend configuration is valid")
    };
    let make_routes = || {
        BTreeMap::from([
            (
                route_one.clone(),
                backend_config(
                    source_user.clone(),
                    source_password.clone(),
                    database_names[0].clone(),
                    source_port,
                ),
            ),
            (
                route_two.clone(),
                backend_config(
                    second_user.clone(),
                    second_password.clone(),
                    database_names[1].clone(),
                    source_port,
                ),
            ),
            (
                route_owner.clone(),
                backend_config(
                    source_user.clone(),
                    source_password.clone(),
                    database_names[1].clone(),
                    source_port,
                ),
            ),
            (
                unreachable.clone(),
                backend_config(source_user.clone(), "unused".into(), "postgres".into(), 1),
            ),
        ])
    };
    let backends = Arc::new(
        PostgresBackends::new(
            make_routes(),
            BackendSettings {
                max_connections_per_pool: 1,
                max_connections_total: 4,
                acquire_timeout_ms: 60_000,
            },
        )
        .expect("route pools must fit the configured bounds"),
    );
    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_identity_ok = Arc::new(AtomicBool::new(true));
    let selector =
        bind_select_backend(
            (
                callback_count.clone(),
                callback_identity_ok.clone(),
                gateway_user.clone(),
                route_one.clone(),
                route_two.clone(),
                route_owner.clone(),
                unreachable.clone(),
            ),
            |request,
             (
                count,
                identity_ok,
                expected_user,
                route_one,
                route_two,
                route_owner,
                unreachable,
            )| {
                count.fetch_add(1, Ordering::SeqCst);
                if request.authenticated_user != expected_user {
                    identity_ok.store(false, Ordering::SeqCst);
                }
                let sql = request.query.sql().to_ascii_lowercase();
                if sql.contains("route_denied") {
                    Err(SelectionError::Denied)
                } else if sql.contains("route_failed") {
                    Err(SelectionError::Failed)
                } else if sql.contains("route_unknown") {
                    Ok(BackendSelection {
                        target: "not_configured".into(),
                        backend_login: "reader".into(),
                    })
                } else if sql.contains("route_backend_failure") {
                    Ok(unreachable.clone())
                } else if sql.contains("route_two") {
                    Ok(route_two.clone())
                } else if sql.contains("route_owner") {
                    Ok(route_owner.clone())
                } else {
                    Ok(route_one.clone())
                }
            },
        );

    #[cfg(feature = "python")]
    let gateway_password_for_client = gateway_password.clone();
    let gateway = GatewayConfig::new(
        gateway_user.clone(),
        gateway_password,
        gateway_database.clone(),
    )
    .expect("gateway identity is valid")
    .with_settings(GatewaySettings {
        max_sessions: 2,
        ..GatewaySettings::default()
    })
    .expect("test gateway limits are valid")
    .with_schema_discovery(route_one.clone());
    let server = GatewayServer::routed(gateway, backends, selector);
    let address = server.address;
    let client = |sql: &[&str]| identity.connection(address).queries(sql);
    let check = |queries: &[&str], output: &Output| {
        assert!(output.status.success(), "routed query sequence failed");
        for query in queries {
            assert!(
                String::from_utf8_lossy(&output.stdout).contains(query),
                "routed result is missing an expected marker"
            );
        }
    };

    let route_one_marker = format!("{}:{source_user}", database_names[0]);
    let route_two_marker = format!("{}:{second_user}", database_names[1]);
    let result = client(&["SELECT current_database() || ':' || current_user"]);
    check(&[&route_one_marker], &result);
    metadata_discovery(&client, &maintenance, &callback_count, &database_names);
    routing_errors_and_recovery(&client, &check, &route_one_marker, &route_two_marker);
    pool_isolation(
        &client,
        &maintenance,
        &check,
        &database_names,
        &route_one_marker,
        &route_two_marker,
    );
    source_permissions(
        &client,
        &check,
        &secondary,
        &database_names,
        &route_one_marker,
        &callback_count,
    );
    cancellation_discards_backend(&client, address, &identity);

    assert!(
        callback_identity_ok.load(Ordering::SeqCst),
        "selector receives the authenticated frontend identity"
    );

    server.stop();
    #[cfg(feature = "python")]
    python_routing_contract(
        Arc::new(
            PostgresBackends::new(
                make_routes(),
                BackendSettings {
                    max_connections_per_pool: 1,
                    max_connections_total: 4,
                    acquire_timeout_ms: 60_000,
                },
            )
            .unwrap(),
        ),
        route_one,
        route_two,
        &gateway_user,
        &gateway_password_for_client,
        &gateway_database,
        &route_one_marker,
        &route_two_marker,
    );
}

pub(super) fn routing_errors_and_recovery(
    client: &dyn Fn(&[&str]) -> Output,
    check: &dyn Fn(&[&str], &Output),
    route_one_marker: &str,
    route_two_marker: &str,
) {
    let result = client(&[
        "SELECT 'route_two' AS route_two, current_database() || ':' || current_user",
        "SELECT current_database() || ':' || current_user",
    ]);
    check(&[route_two_marker, route_one_marker], &result);

    let granted = client(&["SELECT count(*) FROM public.users /* route_two */"]);
    assert!(
        granted.status.success(),
        "secondary login cannot read granted fixture data"
    );
    assert_eq!(String::from_utf8_lossy(&granted.stdout).trim(), "10");

    let route_errors = [
        ("SELECT 'route_denied'", "42501", "backend selection denied"),
        ("SELECT 'route_failed'", "XX000", "backend selection failed"),
        (
            "SELECT 'route_unknown'",
            "42501",
            "backend selection is not configured",
        ),
        (
            "SELECT 'route_backend_failure'",
            "08006",
            "PostgreSQL connection unavailable",
        ),
    ];
    for (query, code, message) in route_errors {
        let result = client(&[query, "SELECT current_database() || ':' || current_user"]);
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(
            stderr.contains(code),
            "routed failure did not return its SQLSTATE"
        );
        assert!(
            stderr.contains(message),
            "routed failure message was not preserved for {query}: {stderr}"
        );
        assert!(
            String::from_utf8_lossy(&result.stdout).contains(route_one_marker),
            "the same authenticated session did not recover after a routing error"
        );
    }
}
