use super::*;

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
            &format!(
                "{namespace}:{}",
                ParsedQuery::parse(query).unwrap().query_id()
            ),
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
