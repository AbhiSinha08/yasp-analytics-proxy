use super::*;

#[test]
fn cancelled_fill_and_lowered_row_limit() {
    let fixture = CacheScenario::new();
    let namespace = fixture.namespace.name.clone();
    let redis_url = fixture.redis_url.clone();
    let user = fixture.identity.user.clone();
    let password = fixture.identity.password.clone();
    let database = fixture.identity.database.clone();
    let primary_password = fixture.primary.identity.password.clone();
    let default = &fixture.route;
    let primary = &fixture.primary;
    let route = |login: &str| BackendSelection {
        target: default.target.clone(),
        backend_login: login.into(),
    };
    let make_backends = || fixture.backends();
    let cache =
        Arc::new(RedisCache::new(settings(&namespace, 60, 5, 196608), &redis_url, None).unwrap());
    let server = start_server(
        fixture.identity.gateway_config().with_cache(cache),
        make_backends(),
        fixture.selector(),
    );
    let address = server.address;
    let client = |query: &str| psql(query, &address, &user, &password, &database);
    let cancel_sql = "SELECT pg_sleep(8)";
    let slow = psql_child(cancel_sql, &address, &user, &password, &database);
    let started = Instant::now();
    let activity = format!(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = '{}' AND usename = '{}' AND state = 'active' AND query = '{}'",
        primary.identity.database, primary.identity.user, cancel_sql,
    );
    loop {
        let probe = psql_direct(
            &activity,
            &primary.host,
            primary.port,
            &primary.identity.user,
            &primary_password,
            &primary.identity.database,
        );
        if stdout(&probe).trim() == "1" {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "slow query reached PostgreSQL"
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        Command::new("kill")
            .args(["-INT", &slow.id().to_string()])
            .status()
            .expect("kill is available")
            .success(),
        "SIGINT reaches the active psql client"
    );
    let cancelled = slow.wait_with_output();
    assert!(!cancelled.status.success());
    let cancel_output = format!(
        "{}{}",
        String::from_utf8_lossy(&cancelled.stdout),
        String::from_utf8_lossy(&cancelled.stderr)
    );
    assert!(
        cancel_output.contains("57014"),
        "cancellation output: {cancel_output}"
    );
    assert!(!redis_exists(
        &redis_url,
        &cache_key(&namespace, &fixture.route, cancel_sql)
    ));
    let recovered = client("SELECT 12345");
    assert_success(&recovered, "query after cache-fill cancellation");
    assert_eq!(stdout(&recovered).trim(), "12345");

    let lowered_limit_sql =
        "SELECT i, clock_timestamp() FROM generate_series(1, 5) AS i /* lowered row cap */";
    let before_lowered_limit = client(lowered_limit_sql);
    assert_success(
        &before_lowered_limit,
        "fill five-row entry before lowering cap",
    );
    let lowered_limit_key = cache_key(&namespace, &fixture.route, lowered_limit_sql);
    let original_entry = redis_get(&redis_url, &lowered_limit_key);
    assert!(!original_entry.is_empty());

    server.stop();

    let lowered_cache = Arc::new(
        RedisCache::new(settings(&namespace, 60, 2, 196608), &redis_url, None)
            .expect("lowered row-cap cache initializes"),
    );
    let server = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(lowered_cache),
        make_backends(),
        bind_select_backend(route(&default.backend_login), |_, selected| {
            Ok(selected.clone())
        }),
    );
    let address = server.address;
    let lowered_client = |query: &str| psql(query, &address, &user, &password, &database);
    let after_lowered_limit = lowered_client(lowered_limit_sql);
    assert_success(&after_lowered_limit, "reject entry above lowered row cap");
    assert_ne!(stdout(&before_lowered_limit), stdout(&after_lowered_limit));
    assert_eq!(redis_get(&redis_url, &lowered_limit_key), original_entry);
    server.stop();
}
