use super::*;

#[test]
fn context_and_metadata_bypass() {
    let fixture = CacheScenario::new();
    let namespace = fixture.namespace.name.clone();
    let redis_url = fixture.redis_url.clone();
    let user = fixture.identity.user.clone();
    let password = fixture.identity.password.clone();
    let database = fixture.identity.database.clone();
    let primary = &fixture.primary;
    let default = &fixture.route;
    let route = |login: &str| BackendSelection {
        target: default.target.clone(),
        backend_login: login.into(),
    };
    let make_backends = || fixture.backends();
    let context_namespace = format!("{namespace}_context");
    let context_observations = Arc::new(std::sync::Mutex::new(Vec::new()));
    let context_sequence = Arc::new(AtomicUsize::new(0));
    let context_hook = bind_context((), {
        let context_sequence = context_sequence.clone();
        move |request, _state| {
            let snapshot = context_sequence.fetch_add(1, Ordering::SeqCst) + 1;
            let authenticated_user = request.authenticated_user.to_owned();
            Box::pin(async move {
                let mut context = serde_json::Map::new();
                context.insert("snapshot".into(), serde_json::json!(snapshot));
                context.insert(
                    "authenticated_user".into(),
                    serde_json::json!("spoofed-user"),
                );
                context.insert("derived_for".into(), serde_json::json!(authenticated_user));
                Ok(context)
            })
        }
    });
    let context_selector = bind_select_backend(
        (route(&default.backend_login), context_observations.clone()),
        |request, (selected, observations)| {
            let context = request.context;
            observations.lock().unwrap().push((
                "selector",
                context["snapshot"].as_u64().unwrap(),
                request.authenticated_user.to_owned(),
                context["authenticated_user"].as_str().unwrap().to_owned(),
                context["derived_for"].as_str().unwrap().to_owned(),
            ));
            Ok(selected.clone())
        },
    );
    let context_eligibility =
        bind_cache_eligibility(context_observations.clone(), |request, observations| {
            let context = request.context;
            observations.lock().unwrap().push((
                "eligibility",
                context["snapshot"].as_u64().unwrap(),
                request.authenticated_user.to_owned(),
                context["authenticated_user"].as_str().unwrap().to_owned(),
                context["derived_for"].as_str().unwrap().to_owned(),
            ));
            Ok(true)
        });
    let context_cache = Arc::new(
        RedisCache::new(
            settings(&context_namespace, 60, 5, 16384),
            &redis_url,
            Some(context_eligibility.into()),
        )
        .expect("context test cache initializes"),
    );
    let context_gateway = GatewayConfig::new(user.clone(), password.clone(), database.clone())
        .expect("frontend identity is valid")
        .with_cache(context_cache)
        .with_context_hook(context_hook);
    let context_server = start_server(context_gateway, make_backends(), context_selector);
    let context_address = context_server.address;
    let normalized_context_hit =
        |query: &str| psql(query, &context_address, &user, &password, &database);
    let discovery = normalized_context_hit(
        "SELECT datname FROM pg_database WHERE datistemplate=false ORDER BY datname",
    );
    assert_success(&discovery, "discovery with context and cache enabled");
    assert_eq!(stdout(&discovery).trim(), primary.identity.database);
    assert_eq!(
        context_sequence.load(Ordering::SeqCst),
        0,
        "discovery bypasses context"
    );
    assert!(
        context_observations.lock().unwrap().is_empty(),
        "discovery bypasses selection and cache eligibility"
    );
    let captured: Vec<String> =
        serde_json::from_str(include_str!("../fixtures/psql_discovery.json")).unwrap();
    // Describe a native relation that also exists in the maintenance database.
    for query in &captured {
        let query = query.replace("'25009'", "'pg_catalog.pg_class'::pg_catalog.regclass");
        let result = normalized_context_hit(&query);
        assert_success(&result, "psql discovery with cache enabled");
        assert!(
            redis_get(
                &redis_url,
                &cache_key(&context_namespace, &fixture.route, &query)
            )
            .is_empty(),
            "psql metadata must not populate Redis"
        );
    }
    assert_eq!(context_sequence.load(Ordering::SeqCst), 0);
    assert!(
        context_observations.lock().unwrap().is_empty(),
        "psql discovery bypasses context, selection, and cache eligibility"
    );
    let dbeaver: Vec<String> =
        serde_json::from_str(include_str!("../fixtures/dbeaver_discovery.json")).unwrap();
    for query in &dbeaver {
        let result = normalized_context_hit(query);
        assert_success(&result, "DBeaver discovery with cache enabled");
        assert!(
            redis_get(
                &redis_url,
                &cache_key(&context_namespace, &fixture.route, query)
            )
            .is_empty()
        );
    }
    assert_eq!(context_sequence.load(Ordering::SeqCst), 0);
    assert!(context_observations.lock().unwrap().is_empty());
    let information_schema_sql =
        "SELECT feature_id FROM information_schema.sql_features ORDER BY feature_id LIMIT 1";
    let result = normalized_context_hit(information_schema_sql);
    assert_success(&result, "information_schema namespace discovery");
    assert_eq!(context_sequence.load(Ordering::SeqCst), 0);
    assert!(context_observations.lock().unwrap().is_empty());
    assert!(
        redis_get(
            &redis_url,
            &cache_key(&context_namespace, &fixture.route, information_schema_sql)
        )
        .is_empty()
    );
    let context_first = normalized_context_hit("SELECT clock_timestamp()");
    let context_second = normalized_context_hit("  select  clock_timestamp() ;  ");
    assert_success(&context_first, "first context-aware query");
    assert_success(&context_second, "normalized context-aware cache hit");
    assert_eq!(
        stdout(&context_first).trim(),
        stdout(&context_second).trim()
    );
    assert_eq!(context_sequence.load(Ordering::SeqCst), 2);
    assert_eq!(
        *context_observations.lock().unwrap(),
        vec![
            (
                "selector",
                1,
                user.clone(),
                "spoofed-user".into(),
                user.clone()
            ),
            (
                "eligibility",
                1,
                user.clone(),
                "spoofed-user".into(),
                user.clone()
            ),
            (
                "selector",
                2,
                user.clone(),
                "spoofed-user".into(),
                user.clone()
            ),
            (
                "eligibility",
                2,
                user.clone(),
                "spoofed-user".into(),
                user.clone()
            ),
        ]
    );
    assert!(redis_exists(
        &redis_url,
        &cache_key(
            &context_namespace,
            &fixture.route,
            "SELECT clock_timestamp()"
        )
    ));
    context_server.stop();
}
