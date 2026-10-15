//! Exercise cache behavior through a real PostgreSQL client and gateway.

#[cfg(unix)]
#[test]
fn redis_cache_contract() {
    use std::{
        collections::BTreeMap,
        path::Path,
        process::Command,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    use yasp::{
        backend::PostgresBackends,
        cache::RedisCache,
        config::{BackendConfig, BackendSettings, FileConfig, GatewayConfig},
        hooks::{BackendSelection, bind_cache_eligibility, bind_context, bind_select_backend},
        query::ParsedQuery,
    };

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut file_values = std::collections::HashMap::new();
    for item in dotenvy::from_path_iter(root.join(".env"))
        .into_iter()
        .flatten()
    {
        let (key, value) = item.expect("test dotenv must be valid");
        file_values.entry(key).or_insert(value);
    }
    let value = |key: &str| {
        std::env::var(key)
            .ok()
            .or_else(|| file_values.get(key).cloned())
            .unwrap_or_else(|| panic!("required test environment value is missing: {key}"))
    };
    let test_config = FileConfig::read(&root.join("tests/config.yml"))
        .expect("test backend configuration must be valid");
    let default = test_config.default_backend().expect("default route exists");
    let target = &test_config.targets[&default.target];
    let primary = &target.logins[&default.backend_login];
    let secondary = &target.logins["secondary"];
    let user = value("YASP_GATEWAY_USERNAME");
    let password = value("YASP_GATEWAY_PASSWORD");
    let database = value("YASP_GATEWAY_DATABASE");
    let primary_password = value(&primary.password_env);
    let secondary_password = value(&secondary.password_env);
    let redis_url =
        std::env::var("YASP_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let namespace = format!(
        "yasp_cache_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after the epoch")
            .as_nanos()
    );
    let selected_login = Arc::new(AtomicBool::new(false));
    let selector_denied = Arc::new(AtomicBool::new(false));
    let selection_count = Arc::new(AtomicUsize::new(0));
    let route = |login: &str| BackendSelection {
        target: default.target.clone(),
        backend_login: login.to_owned(),
    };
    let make_backends = || {
        Arc::new(
            PostgresBackends::new(
                BTreeMap::from([
                    (
                        route(&default.backend_login),
                        BackendConfig::new(
                            target.host.clone(),
                            target.port,
                            primary.username.clone(),
                            primary_password.clone(),
                            target.database.clone(),
                        )
                        .expect("primary backend config is valid"),
                    ),
                    (
                        route("secondary"),
                        BackendConfig::new(
                            target.host.clone(),
                            target.port,
                            secondary.username.clone(),
                            secondary_password.clone(),
                            target.database.clone(),
                        )
                        .expect("secondary backend config is valid"),
                    ),
                ]),
                BackendSettings::default(),
            )
            .expect("test backend routes fit the default limits"),
        )
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
    let (address, shutdown, server) = start_server(gateway(), make_backends(), selector);
    let client = |query: &str| psql(query, &address, &user, &password, &database);

    let first = client("SELECT clock_timestamp()");
    let normalized_hit = client("  select  clock_timestamp() ;  ");
    assert_success(&first, "first timestamp query");
    assert_success(&normalized_hit, "normalized timestamp query");
    assert_eq!(stdout(&first).trim(), stdout(&normalized_hit).trim());
    assert_eq!(selection_count.load(Ordering::SeqCst), 2);

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
    let (context_address, context_shutdown, context_server) =
        start_server(context_gateway, make_backends(), context_selector);
    let normalized_context_hit =
        |query: &str| psql(query, &context_address, &user, &password, &database);
    let discovery = normalized_context_hit(
        "SELECT datname FROM pg_database WHERE datistemplate=false ORDER BY datname",
    );
    assert_success(&discovery, "discovery with context and cache enabled");
    assert_eq!(stdout(&discovery).trim(), target.database);
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
        serde_json::from_str(include_str!("fixtures/psql_discovery.json")).unwrap();
    // Describe a native relation that also exists in the maintenance database.
    for query in &captured {
        let query = query.replace("'25009'", "'pg_catalog.pg_class'::pg_catalog.regclass");
        let result = normalized_context_hit(&query);
        assert_success(&result, "psql discovery with cache enabled");
        assert!(
            redis_get(&redis_url, &cache_key(&context_namespace, &query)).is_empty(),
            "psql metadata must not populate Redis"
        );
    }
    assert_eq!(context_sequence.load(Ordering::SeqCst), 0);
    assert!(
        context_observations.lock().unwrap().is_empty(),
        "psql discovery bypasses context, selection, and cache eligibility"
    );
    let dbeaver: Vec<String> =
        serde_json::from_str(include_str!("fixtures/dbeaver_discovery.json")).unwrap();
    for query in &dbeaver {
        let result = normalized_context_hit(query);
        assert_success(&result, "DBeaver discovery with cache enabled");
        assert!(redis_get(&redis_url, &cache_key(&context_namespace, query)).is_empty());
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
            &cache_key(&context_namespace, information_schema_sql)
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
        &cache_key(&context_namespace, "SELECT clock_timestamp()")
    ));
    let _ = context_shutdown.send(());
    context_server.join().expect("context gateway joins");
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
    assert_eq!(stdout(&secondary_identity).trim(), secondary.username);
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

    const HEADER_BYTES: usize = 5 + 32;
    let wire_query = "SELECT 8675309";
    let wire_key = format!(
        "{namespace}:{}",
        ParsedQuery::parse(wire_query).unwrap().query_id()
    );
    let wire_fill = client(wire_query);
    assert_success(&wire_fill, "cache wire fixture query");
    assert_eq!(stdout(&wire_fill).trim(), "8675309");
    let mut entry = redis_get(&redis_url, &wire_key);
    assert!(entry.len() > HEADER_BYTES + 7);
    assert_eq!(&entry[..5], b"YASP\x01");
    let mut offset = HEADER_BYTES;
    let mut row_description = false;
    while offset + 5 <= entry.len() {
        let packet_len =
            u32::from_be_bytes(entry[offset + 1..offset + 5].try_into().unwrap()) as usize;
        let packet_end = offset + 1 + packet_len;
        assert!(packet_len >= 4 && packet_end <= entry.len());
        if entry[offset] == b'T' {
            assert_eq!(&entry[offset + 5..offset + 7], &[0, 1]);
            entry[offset + 5..offset + 7].copy_from_slice(&2u16.to_be_bytes());
            row_description = true;
            break;
        }
        offset = packet_end;
    }
    assert!(row_description, "cached response contains RowDescription");
    redis_set_bytes(&redis_url, &wire_key, &entry);
    let wire_miss = client(wire_query);
    assert_success(&wire_miss, "malformed cached response is a miss");
    assert_eq!(stdout(&wire_miss).trim(), "8675309");

    let entry = redis_get(&redis_url, &wire_key);
    let mut offset = HEADER_BYTES;
    while offset + 5 <= entry.len() {
        let packet_len =
            u32::from_be_bytes(entry[offset + 1..offset + 5].try_into().unwrap()) as usize;
        let packet_end = offset + 1 + packet_len;
        assert!(packet_len >= 4 && packet_end <= entry.len());
        if entry[offset] == b'C' {
            assert_eq!(packet_end, entry.len());
            break;
        }
        offset = packet_end;
    }
    assert_eq!(entry.get(offset), Some(&b'C'));
    let tag = format!("SELECT {}1", "0".repeat(30000));
    let mut long_tag_entry = entry[..offset].to_vec();
    long_tag_entry.push(b'C');
    long_tag_entry.extend_from_slice(&((4 + tag.len() + 1) as u32).to_be_bytes());
    long_tag_entry.extend_from_slice(tag.as_bytes());
    long_tag_entry.push(0);
    redis_set_bytes(&redis_url, &wire_key, &long_tag_entry);
    let long_tag_miss = client(wire_query);
    assert_success(&long_tag_miss, "oversized completion tag is a cache miss");
    assert_eq!(stdout(&long_tag_miss).trim(), "8675309");

    let failed_sql = "SELECT 1 / 0";
    let failed = client(failed_sql);
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("22012"));
    let failed_key = cache_key(&namespace, failed_sql);
    assert!(!redis_exists(&redis_url, &failed_key));

    let oversized_sql = "SELECT repeat('x', 131072)";
    let oversized = client(oversized_sql);
    assert_success(&oversized, "oversized result query");
    assert_eq!(stdout(&oversized).trim().len(), 131072);
    assert!(!redis_exists(
        &redis_url,
        &cache_key(&namespace, oversized_sql)
    ));

    let wide_sql = "SELECT repeat('x', 8192)";
    let wide = client(wide_sql);
    assert_success(&wide, "wide row below byte budget");
    assert_eq!(stdout(&wide).trim().len(), 8192);
    assert!(redis_exists(&redis_url, &cache_key(&namespace, wide_sql)));

    let five_rows_sql = "SELECT i, clock_timestamp() FROM generate_series(1, 5) AS i /* row cap */";
    let five_rows = client(five_rows_sql);
    let five_rows_hit = client(five_rows_sql);
    assert_success(&five_rows, "result at row cap");
    assert_success(&five_rows_hit, "cached result at row cap");
    assert_eq!(stdout(&five_rows).lines().count(), 5);
    assert_eq!(stdout(&five_rows), stdout(&five_rows_hit));
    assert!(redis_exists(
        &redis_url,
        &cache_key(&namespace, five_rows_sql)
    ));

    let six_rows_sql = "SELECT i FROM generate_series(1, 6) AS i /* over row cap */";
    let six_rows = client(six_rows_sql);
    assert_success(&six_rows, "result above row cap");
    assert_eq!(stdout(&six_rows).lines().count(), 6);
    assert!(!redis_exists(
        &redis_url,
        &cache_key(&namespace, six_rows_sql)
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
        &cache_key(&namespace, fidelity_sql)
    ));

    let cancel_sql = "SELECT pg_sleep(8)";
    let slow = psql_child(cancel_sql, &address, &user, &password, &database);
    let started = Instant::now();
    let activity = format!(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = '{}' AND usename = '{}' AND state = 'active' AND query = '{}'",
        target.database, primary.username, cancel_sql,
    );
    loop {
        let probe = psql_direct(
            &activity,
            &target.host,
            target.port,
            &primary.username,
            &primary_password,
            &target.database,
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
    let cancelled = slow.wait_with_output().expect("cancelled psql exits");
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
        &cache_key(&namespace, cancel_sql)
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
    let lowered_limit_key = cache_key(&namespace, lowered_limit_sql);
    let original_entry = redis_get(&redis_url, &lowered_limit_key);
    assert!(!original_entry.is_empty());

    let _ = shutdown.send(());
    server.join().expect("gateway joins");

    let lowered_cache = Arc::new(
        RedisCache::new(settings(&namespace, 60, 2, 196608), &redis_url, None)
            .expect("lowered row-cap cache initializes"),
    );
    let (address, shutdown, server) = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(lowered_cache),
        make_backends(),
        bind_select_backend(route(&default.backend_login), |_, selected| {
            Ok(selected.clone())
        }),
    );
    let lowered_client = |query: &str| psql(query, &address, &user, &password, &database);
    let after_lowered_limit = lowered_client(lowered_limit_sql);
    assert_success(&after_lowered_limit, "reject entry above lowered row cap");
    assert_ne!(stdout(&before_lowered_limit), stdout(&after_lowered_limit));
    assert_eq!(redis_get(&redis_url, &lowered_limit_key), original_entry);
    let _ = shutdown.send(());
    server.join().expect("lowered row-cap gateway joins");

    let monitored_sql =
        "SELECT ClOcK_TiMeStAmP() + interval '0 microseconds' /* Pg_StAt_AcTiViTy */";
    let seed_cache = Arc::new(
        RedisCache::new(settings(&namespace, 60, 5, 16384), &redis_url, None)
            .expect("cache seed initializes"),
    );
    let (seed_address, seed_shutdown, seed_server) = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(seed_cache),
        make_backends(),
        bind_select_backend(route(&default.backend_login), |_, selected| {
            Ok(selected.clone())
        }),
    );
    let seeded = psql(monitored_sql, &seed_address, &user, &password, &database);
    assert_success(&seeded, "seed monitored query cache entry");
    let monitored_key = cache_key(&namespace, monitored_sql);
    let seeded_entry = redis_get(&redis_url, &monitored_key);
    let _ = seed_shutdown.send(());
    seed_server.join().expect("cache seed gateway joins");

    let bypass_cache = Arc::new(
        RedisCache::new(
            settings(&namespace, 60, 5, 16384),
            &redis_url,
            Some(
                bind_cache_eligibility(
                    vec!["PG_Stat_Activity".to_owned(), "ClOcK_TiMeStAmP".to_owned()],
                    |request, excluded| {
                        if request.query.sql().contains("eligibility error") {
                            Err(yasp::hooks::CacheEligibilityError)
                        } else {
                            yasp::hooks::builtin::cache_eligibility::exclude_sql_containing(
                                request, excluded,
                            )
                        }
                    },
                )
                .into(),
            ),
        )
        .expect("bypass cache initializes"),
    );
    let (address, shutdown, server) = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(bypass_cache),
        make_backends(),
        bind_select_backend(route(&default.backend_login), |_, selected| {
            Ok(selected.clone())
        }),
    );
    let bypass_client = |query: &str| psql(query, &address, &user, &password, &database);
    let denied_first = bypass_client(monitored_sql);
    let denied_second = bypass_client(monitored_sql);
    assert_success(&denied_first, "matching query one bypasses cache");
    assert_success(&denied_second, "matching query two bypasses cache");
    assert_ne!(stdout(&seeded).trim(), stdout(&denied_first).trim());
    assert_ne!(stdout(&denied_first).trim(), stdout(&denied_second).trim());
    assert_eq!(redis_get(&redis_url, &monitored_key), seeded_entry);
    let ordinary_sql = "SELECT statement_timestamp() /* cache eligible */";
    let ordinary_first = bypass_client(ordinary_sql);
    let ordinary_second = bypass_client(ordinary_sql);
    assert_success(&ordinary_first, "nonmatching query one");
    assert_success(&ordinary_second, "nonmatching query two");
    assert_eq!(
        stdout(&ordinary_first).trim(),
        stdout(&ordinary_second).trim()
    );
    assert!(redis_exists(
        &redis_url,
        &cache_key(&namespace, ordinary_sql)
    ));
    let error_first = bypass_client("SELECT clock_timestamp() /* eligibility error */");
    let error_second = bypass_client("SELECT clock_timestamp() /* eligibility error */");
    assert_success(&error_first, "eligibility error query one");
    assert_success(&error_second, "eligibility error query two");
    assert_ne!(stdout(&error_first).trim(), stdout(&error_second).trim());
    let _ = shutdown.send(());
    server.join().expect("bypass gateway joins");

    let bounded_cache = Arc::new(
        RedisCache::new(settings(&namespace, 60, 5, 1), &redis_url, None)
            .expect("low-budget cache initializes"),
    );
    let (address, shutdown, server) = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(bounded_cache),
        make_backends(),
        bind_select_backend(route(&default.backend_login), |_, selected| {
            Ok(selected.clone())
        }),
    );
    let bounded_client = |query: &str| psql(query, &address, &user, &password, &database);
    let budget_first =
        bounded_client("SELECT clock_timestamp() + interval '0 seconds' /* budget */");
    let budget_second =
        bounded_client("SELECT clock_timestamp() + interval '0 seconds' /* budget */");
    assert_success(&budget_first, "first low-budget query");
    assert_success(&budget_second, "second low-budget query");
    assert_ne!(stdout(&budget_first).trim(), stdout(&budget_second).trim());
    let _ = shutdown.send(());
    server.join().expect("bounded gateway joins");

    let temporary =
        std::net::TcpListener::bind("127.0.0.1:0").expect("temporary loopback port binds");
    let closed = temporary.local_addr().expect("temporary address exists");
    drop(temporary);
    let unavailable_cache = Arc::new(
        RedisCache::new(
            settings(&namespace, 60, 5, 16384),
            &format!("redis://{closed}/"),
            None,
        )
        .expect("unavailable Redis is handled lazily"),
    );
    let (address, shutdown, server) = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(unavailable_cache),
        make_backends(),
        bind_select_backend(route(&default.backend_login), |_, selected| {
            Ok(selected.clone())
        }),
    );
    let unavailable = psql("SELECT 12345", &address, &user, &password, &database);
    assert_success(&unavailable, "query while Redis is unavailable");
    assert_eq!(stdout(&unavailable).trim(), "12345");
    let _ = shutdown.send(());
    server.join().expect("unavailable-cache gateway joins");

    #[cfg(feature = "python")]
    {
        use tokio::runtime::Builder;
        use yasp::{config::PythonSettings, hooks::python::PythonRuntime};

        let python_namespace = format!("{namespace}_python");
        let python_example_namespace = namespace.clone();
        let directory = std::env::temp_dir().join(&python_namespace);
        std::fs::create_dir(&directory).expect("temporary Python directory is created");
        let script = directory.join("eligibility.py");
        std::fs::write(
            &script,
            r#"
def eligibility(request, params):
    if "callback_error" in request["sql"]:
        raise RuntimeError("private test failure")
    if "invalid_bool" in request["sql"]:
        return "yes"
    return True
"#,
        )
        .expect("Python callback is written");
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Python test runtime starts");
        let (python_runtime, callback, example_callback) = runtime.block_on(async {
            let runtime = PythonRuntime::new(PythonSettings::default()).await.unwrap();
            let callback = runtime
                .register(&script, "eligibility", serde_json::Map::new())
                .await
                .unwrap();
            let params = serde_json::Map::from_iter([(
                "excluded_sql_fragments".to_owned(),
                serde_json::json!(["PG_Stat_Activity", "ClOcK_TiMeStAmP"]),
            )]);
            let example_callback = runtime
                .register(
                    &root.join("hooks/examples/cache_eligibility.py"),
                    "cache_eligibility",
                    params,
                )
                .await
                .unwrap();
            (runtime, callback, example_callback)
        });
        let python_cache = Arc::new(
            RedisCache::new(
                settings(&python_namespace, 60, 5, 16384),
                &redis_url,
                Some(callback.into()),
            )
            .expect("Python eligibility cache initializes"),
        );
        let python_gateway = GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(python_cache);
        let (address, shutdown, server) = start_server(
            python_gateway,
            make_backends(),
            bind_select_backend(route(&default.backend_login), |_, selected| {
                Ok(selected.clone())
            }),
        );
        for query in [
            "SELECT clock_timestamp() /* callback_error */",
            "SELECT clock_timestamp() /* invalid_bool */",
        ] {
            let first = psql(query, &address, &user, &password, &database);
            let second = psql(query, &address, &user, &password, &database);
            assert_success(&first, "Python eligibility failure query one");
            assert_success(&second, "Python eligibility failure query two");
            assert_ne!(stdout(&first).trim(), stdout(&second).trim());
        }

        let example_cache = Arc::new(
            RedisCache::new(
                settings(&python_example_namespace, 60, 5, 16384),
                &redis_url,
                Some(example_callback.into()),
            )
            .expect("Python example cache initializes"),
        );
        let (example_address, example_shutdown, example_server) = start_server(
            GatewayConfig::new(user.clone(), password.clone(), database.clone())
                .unwrap()
                .with_cache(example_cache),
            make_backends(),
            bind_select_backend(route(&default.backend_login), |_, selected| {
                Ok(selected.clone())
            }),
        );
        let monitored_query = monitored_sql;
        let monitored_key = cache_key(&python_example_namespace, monitored_query);
        let seeded_entry = redis_get(&redis_url, &monitored_key);

        let first = psql(
            monitored_query,
            &example_address,
            &user,
            &password,
            &database,
        );
        let second = psql(
            monitored_query,
            &example_address,
            &user,
            &password,
            &database,
        );
        assert_success(&first, "repository Python example bypasses cache one");
        assert_success(&second, "repository Python example bypasses cache two");
        assert_ne!(stdout(&seeded).trim(), stdout(&first).trim());
        assert_ne!(stdout(&first).trim(), stdout(&second).trim());
        assert_eq!(redis_get(&redis_url, &monitored_key), seeded_entry);
        let ordinary_query = "SELECT statement_timestamp() /* Python cache eligible */";
        let ordinary_first = psql(
            ordinary_query,
            &example_address,
            &user,
            &password,
            &database,
        );
        let ordinary_second = psql(
            ordinary_query,
            &example_address,
            &user,
            &password,
            &database,
        );
        assert_success(
            &ordinary_first,
            "repository Python example allows cache one",
        );
        assert_success(
            &ordinary_second,
            "repository Python example allows cache two",
        );
        assert_eq!(
            stdout(&ordinary_first).trim(),
            stdout(&ordinary_second).trim()
        );
        assert!(redis_exists(
            &redis_url,
            &cache_key(&python_example_namespace, ordinary_query)
        ));

        let _ = example_shutdown.send(());
        example_server
            .join()
            .expect("Python example cache gateway joins");
        let _ = shutdown.send(());
        server.join().expect("Python cache gateway joins");
        python_runtime.shutdown();
        for query in [
            "SELECT clock_timestamp() /* callback_error */",
            "SELECT clock_timestamp() /* invalid_bool */",
        ] {
            let key = format!(
                "{python_namespace}:{}",
                ParsedQuery::parse(query).unwrap().query_id()
            );
            let _ = Command::new("redis-cli")
                .args(["-u", &redis_url, "DEL", &key])
                .output();
        }
        for query in [monitored_query, ordinary_query] {
            let _ = Command::new("redis-cli")
                .args([
                    "-u",
                    &redis_url,
                    "DEL",
                    &cache_key(&python_example_namespace, query),
                ])
                .output();
        }
        std::fs::remove_dir_all(directory).expect("temporary Python files are removed");
    }

    // Remove only keys created under this unique namespace.
    for query in [
        "SELECT clock_timestamp()",
        "SELECT clock_timestamp() /* malformed */",
        "SELECT clock_timestamp() /* oversized */",
        "SELECT current_user",
        "SELECT 8675309",
        "SELECT 12345",
        "SELECT 1 / 0",
        "SELECT repeat('x', 131072)",
        "SELECT repeat('x', 8192)",
        "SELECT i, clock_timestamp() FROM generate_series(1, 5) AS i /* row cap */",
        "SELECT i FROM generate_series(1, 6) AS i /* over row cap */",
        "SELECT i, clock_timestamp() FROM generate_series(1, 5) AS i /* lowered row cap */",
        "SELECT NULL::text, 12345678901234567890::numeric, '雪☃'::text, decode('00ff7f', 'hex')::bytea, ARRAY[1,2,3]::integer[]",
        "SELECT pg_sleep(8)",
        "SELECT clock_timestamp() /* ineligible */",
        "SELECT clock_timestamp() /* eligibility error */",
        "SELECT ClOcK_TiMeStAmP() + interval '0 microseconds' /* Pg_StAt_AcTiViTy */",
        "SELECT statement_timestamp() /* cache eligible */",
        "SELECT statement_timestamp() /* Python cache eligible */",
        "SELECT clock_timestamp() + interval '0 seconds' /* budget */",
    ] {
        let key = format!(
            "{namespace}:{}",
            ParsedQuery::parse(query).unwrap().query_id()
        );
        let _ = Command::new("redis-cli")
            .args(["-u", &redis_url, "DEL", &key])
            .output();
    }
    let _ = Command::new("redis-cli")
        .args([
            "-u",
            &redis_url,
            "DEL",
            &cache_key(&context_namespace, "SELECT clock_timestamp()"),
        ])
        .output();
}

#[cfg(unix)]
fn settings(
    namespace: &str,
    ttl: u32,
    max_rows: usize,
    budget: usize,
) -> yasp::config::CacheSettings {
    yasp::config::CacheSettings {
        enabled: true,
        url_env: "YASP_TEST_REDIS_URL".into(),
        namespace: namespace.into(),
        ttl_seconds: ttl,
        max_rows_per_query: max_rows,
        max_buffered_bytes_total: budget,
        eligibility: None,
    }
}

#[cfg(unix)]
fn start_server(
    gateway: yasp::config::GatewayConfig,
    backends: std::sync::Arc<yasp::backend::PostgresBackends>,
    selector: impl Into<yasp::hooks::RoutedSelector> + Send + 'static,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    use tokio::runtime::Builder;
    let (startup_tx, startup_rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime starts");
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("ephemeral loopback listener binds");
            let address = listener.local_addr().expect("listener address exists");
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            startup_tx
                .send((address, shutdown_tx))
                .expect("test receives address");
            yasp::gateway::serve_routed(listener, gateway, backends, selector, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("routed gateway stops cleanly");
        });
    });
    let (address, shutdown) = startup_rx.recv().expect("routed gateway starts");
    (address, shutdown, server)
}

#[cfg(unix)]
fn psql(
    query: &str,
    address: &std::net::SocketAddr,
    user: &str,
    password: &str,
    database: &str,
) -> std::process::Output {
    psql_command(
        &address.ip().to_string(),
        address.port(),
        user,
        password,
        database,
    )
    .args(["-c", query])
    .output()
    .expect("psql and libpq are installed")
}

#[cfg(unix)]
fn psql_direct(
    query: &str,
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
) -> std::process::Output {
    psql_command(host, port, user, password, database)
        .args(["-c", query])
        .output()
        .expect("psql and libpq are installed")
}

#[cfg(unix)]
fn psql_child(
    query: &str,
    address: &std::net::SocketAddr,
    user: &str,
    password: &str,
    database: &str,
) -> std::process::Child {
    psql_command(
        &address.ip().to_string(),
        address.port(),
        user,
        password,
        database,
    )
    .args(["-c", query])
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .spawn()
    .expect("psql and libpq are installed")
}

#[cfg(unix)]
fn psql_command(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
) -> std::process::Command {
    let mut command = std::process::Command::new("psql");
    command
        .args([
            "-X",
            "-w",
            "-At",
            "-v",
            "VERBOSITY=verbose",
            "-v",
            "ON_ERROR_STOP=1",
        ])
        .env("PGPASSFILE", "/dev/null")
        .env("PGSSLMODE", "disable")
        .env("PGCONNECT_TIMEOUT", "3")
        .env("PGHOST", host)
        .env("PGPORT", port.to_string())
        .env("PGUSER", user)
        .env("PGPASSWORD", password)
        .env("PGDATABASE", database);
    command
}

#[cfg(unix)]
fn redis_set(url: &str, key: &str, value: &str) {
    let output = std::process::Command::new("redis-cli")
        .args(["-u", url, "SET", key, value])
        .output()
        .expect("redis-cli is installed");
    assert!(
        output.status.success(),
        "could not seed an invalid Redis entry"
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "OK");
}

#[cfg(unix)]
fn cache_key(namespace: &str, sql: &str) -> String {
    format!(
        "{namespace}:{}",
        yasp::query::ParsedQuery::parse(sql).unwrap().query_id()
    )
}

#[cfg(unix)]
fn redis_exists(url: &str, key: &str) -> bool {
    let output = std::process::Command::new("redis-cli")
        .args(["-u", url, "EXISTS", key])
        .output()
        .expect("redis-cli is installed");
    assert!(output.status.success(), "Redis EXISTS succeeds");
    String::from_utf8_lossy(&output.stdout).trim() == "1"
}

#[cfg(unix)]
fn redis_get(url: &str, key: &str) -> Vec<u8> {
    let output = std::process::Command::new("redis-cli")
        .args(["-u", url, "--raw", "GET", key])
        .output()
        .expect("redis-cli is installed");
    assert!(
        output.status.success(),
        "could not read the test cache entry"
    );
    let mut bytes = output.stdout;
    assert_eq!(bytes.pop(), Some(b'\n'));
    bytes
}

#[cfg(unix)]
fn redis_set_bytes(url: &str, key: &str, value: &[u8]) {
    use std::io::Write;
    let mut child = std::process::Command::new("redis-cli")
        .args(["-u", url, "-x", "SET", key])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("redis-cli is installed");
    child
        .stdin
        .take()
        .expect("Redis input is piped")
        .write_all(value)
        .expect("malformed cache packet reaches Redis");
    let output = child.wait_with_output().expect("Redis SET finishes");
    assert!(
        output.status.success(),
        "could not write the test cache entry"
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "OK");
}

#[cfg(unix)]
fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[cfg(unix)]
fn assert_success(output: &std::process::Output, description: &str) {
    assert!(
        output.status.success(),
        "{description} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
