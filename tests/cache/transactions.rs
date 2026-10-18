use super::*;

#[test]
fn transactions_pin_routes_preserve_settings_and_bypass_cache() {
    let fixture = CacheScenario::new();
    fixture.primary.execute(
        "CREATE SCHEMA scope_a; CREATE SCHEMA scope_b; \
         CREATE TABLE scope_a.scope_value(value int); INSERT INTO scope_a.scope_value VALUES(11); \
         CREATE TABLE scope_b.scope_value(value int); INSERT INTO scope_b.scope_value VALUES(22); \
         CREATE TABLE public.transaction_private(value int); INSERT INTO public.transaction_private VALUES(99)",
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let owner = fixture.route.clone();
    let reader = BackendSelection {
        target: owner.target.clone(),
        backend_login: "secondary".into(),
    };
    let selector = bind_select_backend(
        (owner.clone(), reader.clone(), calls.clone()),
        |request, (owner, reader, calls)| {
            calls.fetch_add(1, Ordering::SeqCst);
            if request.query.sql().contains("no_selection") {
                return Err(yasp::hooks::SelectionError::Denied);
            }
            Ok(if request.query.sql().contains("secondary_route") {
                reader.clone()
            } else {
                owner.clone()
            })
        },
    );
    let cache = Arc::new(
        RedisCache::new(
            settings(&fixture.namespace.name, 60, 100, 196608),
            &fixture.redis_url,
            None,
        )
        .unwrap(),
    );
    let server = start_server(
        fixture
            .identity
            .gateway_config()
            .with_schema_discovery(reader)
            .with_cache(cache),
        fixture.backends(),
        selector,
    );
    let client = fixture.identity.connection(server.address);
    let empty = client.queries(&[
        "BEGIN /* no_selection */",
        "ROLLBACK /* no_selection */",
        "BEGIN",
        "COMMIT",
        "SHOW TRANSACTION ISOLATION LEVEL",
    ]);
    assert_success(
        &empty,
        "empty transactions and local driver setup do not route",
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let driver = client.queries(&[
        "BEGIN",
        "SELECT version()",
        "SELECT current_schema()",
        "SHOW TRANSACTION ISOLATION LEVEL",
        "ROLLBACK",
        "BEGIN",
        "SELECT 1",
        "ROLLBACK",
    ]);
    assert_success(
        &driver,
        "driver initialization with separate metadata and data roles",
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let pinned = client.queries(&[
        "BEGIN",
        "SELECT pg_backend_pid()",
        "SELECT pg_backend_pid()",
        "COMMIT",
    ]);
    assert_success(
        &pinned,
        "two statements retain one physical source connection",
    );
    let pids: Vec<_> = stdout(&pinned)
        .lines()
        .filter(|line| line.parse::<i32>().is_ok())
        .map(str::to_owned)
        .collect();
    assert_eq!(pids.len(), 2);
    assert_eq!(pids[0], pids[1]);
    for queries in [
        vec![
            "BEGIN",
            "SELECT pg_backend_pid()",
            "SELECT current_user /* secondary_route */",
            "SELECT 1",
            "ROLLBACK",
        ],
        vec!["BEGIN", "SELECT version()", "SELECT 1", "ROLLBACK"],
        vec![
            "BEGIN",
            "SELECT pg_backend_pid()",
            "SELECT version()",
            "ROLLBACK",
        ],
    ] {
        let output = client.queries(&queries);
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("25000"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(stdout(&output).ends_with("ROLLBACK\n"));
    }
    let denial = client.queries(&[
        "BEGIN",
        "SELECT * FROM public.transaction_private /* secondary_route */",
        "SELECT 1",
        "ROLLBACK",
        "SELECT 123",
    ]);
    let errors = String::from_utf8_lossy(&denial.stderr);
    assert!(
        errors.contains("42501") && errors.contains("25P02"),
        "{errors}"
    );
    assert!(stdout(&denial).ends_with("123\n"));

    let sql = "SELECT clock_timestamp()";
    let cached = client.query(sql);
    assert_success(&cached, "seed autocommit result");
    let key = cache_key(&fixture.namespace.name, &owner, sql);
    let bytes = redis_get(&fixture.redis_url, &key);
    let uncached = client.queries(&["BEGIN", sql, "COMMIT"]);
    assert_success(&uncached, "explicit transaction bypasses cached results");
    assert!(!stdout(&uncached).contains(stdout(&cached).trim()));
    assert_eq!(redis_get(&fixture.redis_url, &key), bytes);
    assert_eq!(stdout(&client.query(sql)), stdout(&cached));
    let only_transaction = "SELECT 987654321";
    assert_success(
        &client.queries(&["BEGIN", only_transaction, "COMMIT"]),
        "transaction-only result",
    );
    assert!(!redis_exists(
        &fixture.redis_url,
        &cache_key(&fixture.namespace.name, &owner, only_transaction)
    ));

    let values = client.queries(&[
        "SET search_path = scope_a",
        "SELECT value FROM scope_value",
        "SET search_path = scope_b",
        "SELECT value FROM scope_value",
        "BEGIN",
        "SET search_path = scope_a",
        "SELECT value FROM scope_value",
        "ROLLBACK",
        "SELECT value FROM scope_value",
        "BEGIN",
        "SET search_path = scope_a",
        "COMMIT",
        "SELECT value FROM scope_value",
        "SET application_name = 'first'",
        "SELECT current_setting('application_name') || '_value'",
        "SET application_name = 'second'",
        "SELECT current_setting('application_name') || '_value'",
    ]);
    assert_success(&values, "transactional settings and autocommit cache scope");
    let output = stdout(&values);
    let numbers: Vec<_> = output
        .lines()
        .filter(|line| line.parse::<i32>().is_ok())
        .collect();
    assert_eq!(numbers, ["11", "22", "11", "22", "11"]);
    assert!(output.contains("first_value") && output.contains("second_value"));
    server.stop();
}
