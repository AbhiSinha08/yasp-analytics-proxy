use super::*;

#[test]
fn result_bounds_and_native_values() {
    let fixture = CacheScenario::new();
    let namespace = fixture.namespace.name.clone();
    let redis_url = fixture.redis_url.clone();
    let user = fixture.identity.user.clone();
    let password = fixture.identity.password.clone();
    let database = fixture.identity.database.clone();
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
    let failed_sql = "SELECT 1 / 0";
    let failed = client(failed_sql);
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("22012"));
    let failed_key = cache_key(&namespace, &fixture.route, failed_sql);
    assert!(!redis_exists(&redis_url, &failed_key));

    let oversized_sql = "SELECT repeat('x', 131072)";
    let oversized = client(oversized_sql);
    assert_success(&oversized, "oversized result query");
    assert_eq!(stdout(&oversized).trim().len(), 131072);
    assert!(!redis_exists(
        &redis_url,
        &cache_key(&namespace, &fixture.route, oversized_sql)
    ));

    let wide_sql = "SELECT repeat('x', 8192)";
    let wide = client(wide_sql);
    assert_success(&wide, "wide row below byte budget");
    assert_eq!(stdout(&wide).trim().len(), 8192);
    assert!(redis_exists(
        &redis_url,
        &cache_key(&namespace, &fixture.route, wide_sql)
    ));

    let five_rows_sql = "SELECT i, clock_timestamp() FROM generate_series(1, 5) AS i /* row cap */";
    let five_rows = client(five_rows_sql);
    let five_rows_hit = client(five_rows_sql);
    assert_success(&five_rows, "result at row cap");
    assert_success(&five_rows_hit, "cached result at row cap");
    assert_eq!(stdout(&five_rows).lines().count(), 5);
    assert_eq!(stdout(&five_rows), stdout(&five_rows_hit));
    assert!(redis_exists(
        &redis_url,
        &cache_key(&namespace, &fixture.route, five_rows_sql)
    ));

    let six_rows_sql = "SELECT i FROM generate_series(1, 6) AS i /* over row cap */";
    let six_rows = client(six_rows_sql);
    assert_success(&six_rows, "result above row cap");
    assert_eq!(stdout(&six_rows).lines().count(), 6);
    assert!(!redis_exists(
        &redis_url,
        &cache_key(&namespace, &fixture.route, six_rows_sql)
    ));

    let fidelity_sql = "SELECT NULL::text, 12345678901234567890::numeric, '雪☃'::text, decode('00ff7f', 'hex')::bytea, ARRAY[1,2,3]::integer[]";
    let fidelity_first = client(fidelity_sql);
    let fidelity_second = client(fidelity_sql);
    assert_success(&fidelity_first, "typed cache result first query");
    assert_success(&fidelity_second, "typed cache result second query");
    assert_eq!(fidelity_first.stdout, fidelity_second.stdout);
    assert!(stdout(&fidelity_first).contains("12345678901234567890"));
    assert!(stdout(&fidelity_first).contains("雪☃"));
    assert!(stdout(&fidelity_first).contains(r"\x00ff7f"));
    assert!(stdout(&fidelity_first).contains("{1,2,3}"));
    assert!(redis_exists(
        &redis_url,
        &cache_key(&namespace, &fixture.route, fidelity_sql)
    ));
}
