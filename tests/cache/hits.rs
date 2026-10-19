use super::*;

#[test]
fn both_frontend_profiles_cache_postgres_results_with_separate_scopes() {
    let fixture = CacheScenario::new();
    let tls = std::path::PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
    let mut previous = None;
    for profile in ["postgresql", "redshift", "postgresql"] {
        let cache = Arc::new(
            RedisCache::new(
                settings(&fixture.namespace.name, 60, 5, 196608),
                &fixture.redis_url,
                None,
            )
            .unwrap(),
        );
        let gateway = fixture
            .identity
            .gateway_config()
            .with_cache(cache)
            .with_settings(yasp::config::GatewaySettings {
                protocol: profile.into(),
                tls: yasp::config::GatewayTls {
                    mode: "enable".into(),
                    cert_file: Some(tls.join("server.crt")),
                    key_file: Some(tls.join("server.key")),
                },
                ..Default::default()
            })
            .unwrap();
        let server = start_server(gateway, fixture.backends(), fixture.selector());
        let query = |sql: &str| {
            fixture
                .identity
                .connection(server.address)
                .command()
                .env("PGSSLMODE", "verify-full")
                .env("PGSSLROOTCERT", tls.join("ca.crt"))
                .args(["-c", sql])
                .output()
                .unwrap()
        };
        let first = query("SELECT clock_timestamp()");
        let hit = query(" select clock_timestamp() ;");
        assert_success(&first, "frontend cache miss");
        assert_success(&hit, "frontend cache hit");
        assert_eq!(first.stdout, hit.stdout);
        if let Some(previous) = previous.take() {
            assert_ne!(
                first.stdout, previous,
                "frontend profiles must not share responses"
            );
        }
        previous = Some(first.stdout);
        server.stop();
    }
}

#[test]
fn normalized_hits_expiry_and_source_scope() {
    let fixture = CacheScenario::new();
    let namespace = fixture.namespace.name.clone();
    let redis_url = fixture.redis_url.clone();
    let user = fixture.identity.user.clone();
    let password = fixture.identity.password.clone();
    let database = fixture.identity.database.clone();
    let default = &fixture.route;
    let make_backends = || fixture.backends();
    let selected_login = Arc::new(AtomicBool::new(false));
    let selector_denied = Arc::new(AtomicBool::new(false));
    let selection_count = Arc::new(AtomicUsize::new(0));
    let route = |login: &str| BackendSelection {
        target: default.target.clone(),
        backend_login: login.to_owned(),
    };
    let selector = {
        let selected_login = selected_login.clone();
        let selector_denied = selector_denied.clone();
        let selection_count = selection_count.clone();
        let primary_route = route(&default.backend_login);
        let secondary_route = route("secondary");
        bind_select_backend((), move |_, _| {
            selection_count.fetch_add(1, Ordering::SeqCst);
            if selector_denied.load(Ordering::SeqCst) {
                return Err(yasp::hooks::SelectionError::Denied);
            }
            Ok(if selected_login.load(Ordering::SeqCst) {
                secondary_route.clone()
            } else {
                primary_route.clone()
            })
        })
    };
    let cache = Arc::new(
        RedisCache::new(settings(&namespace, 1, 5, 196608), &redis_url, None)
            .expect("local Redis cache initializes"),
    );
    let gateway = || {
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .expect("frontend identity is valid")
            .with_cache(cache.clone())
    };
    let server = start_server(gateway(), make_backends(), selector);
    let address = server.address;
    let client = |query: &str| psql(query, &address, &user, &password, &database);

    let first = client("SELECT clock_timestamp()");
    let normalized_hit = client("  select  clock_timestamp() ;  ");
    assert_success(&first, "first timestamp query");
    assert_success(&normalized_hit, "normalized timestamp query");
    assert_eq!(stdout(&first).trim(), stdout(&normalized_hit).trim());
    assert_eq!(selection_count.load(Ordering::SeqCst), 2);

    selector_denied.store(true, Ordering::SeqCst);
    let denied = client("SELECT clock_timestamp()");
    assert!(!denied.status.success());
    assert!(String::from_utf8_lossy(&denied.stderr).contains("42501"));
    assert!(
        denied.stdout.is_empty(),
        "denial cannot replay cached output"
    );
    selector_denied.store(false, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(1100));
    let expired = client("SELECT clock_timestamp()");
    assert_success(&expired, "expired timestamp query");
    assert_ne!(stdout(&first).trim(), stdout(&expired).trim());
    assert_eq!(selection_count.load(Ordering::SeqCst), 4);

    for (query, raw_value) in [
        (
            "SELECT clock_timestamp() /* malformed */",
            "not-a-cache-entry".into(),
        ),
        (
            "SELECT clock_timestamp() /* oversized */",
            "x".repeat(65537),
        ),
    ] {
        redis_set(
            &redis_url,
            &cache_key(&namespace, &route(&default.backend_login), query),
            &raw_value,
        );
        let result = client(query);
        assert_success(&result, "query after invalid cache entry");
        assert!(stdout(&result).trim().contains('-'));
    }

    let primary_identity = client("SELECT current_user");
    assert_success(&primary_identity, "primary login query");
    selected_login.store(true, Ordering::SeqCst);
    let secondary_identity = client("SELECT current_user");
    assert_success(&secondary_identity, "secondary login query");
    assert_eq!(
        stdout(&secondary_identity).trim(),
        fixture.secondary.identity.user
    );
    assert_ne!(
        stdout(&primary_identity).trim(),
        stdout(&secondary_identity).trim()
    );
    selected_login.store(false, Ordering::SeqCst);
    let primary_again = client("SELECT current_user");
    assert_success(&primary_again, "primary login after scope change");
    assert_eq!(
        stdout(&primary_identity).trim(),
        stdout(&primary_again).trim()
    );
    assert_eq!(selection_count.load(Ordering::SeqCst), 9);
}

#[test]
fn named_routes_keep_separate_entries_with_identical_source_credentials() {
    let fixture = CacheScenario::new();
    let namespace = fixture.namespace.name.clone();
    let redis_url = fixture.redis_url.clone();
    let user = fixture.identity.user.clone();
    let password = fixture.identity.password.clone();
    let database = fixture.identity.database.clone();
    let first_route = BackendSelection {
        target: "shared_target%".into(),
        backend_login: "analyst".into(),
    };
    let second_route = BackendSelection {
        target: "shared".into(),
        backend_login: "target%_analyst".into(),
    };
    let backends = Arc::new(
        PostgresBackends::new(
            BTreeMap::from([
                (first_route.clone(), fixture.primary.backend_config()),
                (second_route.clone(), fixture.primary.backend_config()),
            ]),
            BackendSettings::default(),
        )
        .expect("two route names may share source credentials"),
    );
    let second_selected = Arc::new(AtomicBool::new(false));
    let selector = {
        let second_selected = second_selected.clone();
        bind_select_backend(
            (first_route.clone(), second_route.clone()),
            move |_, routes| {
                Ok(if second_selected.load(Ordering::SeqCst) {
                    routes.1.clone()
                } else {
                    routes.0.clone()
                })
            },
        )
    };
    let cache = Arc::new(
        RedisCache::new(settings(&namespace, 60, 5, 16384), &redis_url, None)
            .expect("local Redis cache initializes"),
    );
    let server = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .expect("frontend identity is valid")
            .with_schema_discovery(first_route.clone())
            .with_cache(cache),
        backends,
        selector,
    );
    let address = server.address;
    let client = || {
        psql(
            "SELECT clock_timestamp()",
            &address,
            &user,
            &password,
            &database,
        )
    };

    let first = client();
    second_selected.store(true, Ordering::SeqCst);
    let second = client();
    second_selected.store(false, Ordering::SeqCst);
    let first_hit = client();
    second_selected.store(true, Ordering::SeqCst);
    let second_hit = client();
    for result in [&first, &second, &first_hit, &second_hit] {
        assert_success(result, "query on aliased route");
    }
    assert_ne!(stdout(&first).trim(), stdout(&second).trim());
    assert_eq!(stdout(&first), stdout(&first_hit));
    assert_eq!(stdout(&second), stdout(&second_hit));

    let first_key = cache_key(&namespace, &first_route, "SELECT clock_timestamp()");
    let second_key = cache_key(&namespace, &second_route, "SELECT clock_timestamp()");
    assert!(first_key.starts_with(&format!("{namespace}:shared%5Ftarget%25_analyst:")));
    assert!(second_key.starts_with(&format!("{namespace}:shared_target%25%5Fanalyst:")));
    assert_ne!(first_key, second_key);
    assert_ne!(
        redis_get(&redis_url, &first_key),
        redis_get(&redis_url, &second_key)
    );
    server.stop();
}
