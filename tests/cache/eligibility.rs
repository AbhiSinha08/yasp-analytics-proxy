use super::*;

#[test]
fn eligibility_budgets_and_redis_outage() {
    let fixture = CacheScenario::new();
    let namespace = fixture.namespace.name.clone();
    let redis_url = fixture.redis_url.clone();
    let user = fixture.identity.user.clone();
    let password = fixture.identity.password.clone();
    let database = fixture.identity.database.clone();
    let default = &fixture.route;
    let route = |login: &str| BackendSelection {
        target: default.target.clone(),
        backend_login: login.into(),
    };
    let make_backends = || fixture.backends();
    let monitored_sql =
        "SELECT ClOcK_TiMeStAmP() + interval '0 microseconds' /* Pg_StAt_AcTiViTy */";
    let seed_cache = Arc::new(
        RedisCache::new(settings(&namespace, 60, 5, 16384), &redis_url, None)
            .expect("cache seed initializes"),
    );
    let seed_server = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(seed_cache),
        make_backends(),
        bind_select_backend(route(&default.backend_login), |_, selected| {
            Ok(selected.clone())
        }),
    );
    let seed_address = seed_server.address;
    let seeded = psql(monitored_sql, &seed_address, &user, &password, &database);
    assert_success(&seeded, "seed monitored query cache entry");
    let monitored_key = cache_key(&namespace, monitored_sql);
    let seeded_entry = redis_get(&redis_url, &monitored_key);
    seed_server.stop();

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
    let server = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(bypass_cache),
        make_backends(),
        bind_select_backend(route(&default.backend_login), |_, selected| {
            Ok(selected.clone())
        }),
    );
    let address = server.address;
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
    server.stop();

    let bounded_cache = Arc::new(
        RedisCache::new(settings(&namespace, 60, 5, 1), &redis_url, None)
            .expect("low-budget cache initializes"),
    );
    let server = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(bounded_cache),
        make_backends(),
        bind_select_backend(route(&default.backend_login), |_, selected| {
            Ok(selected.clone())
        }),
    );
    let address = server.address;
    let bounded_client = |query: &str| psql(query, &address, &user, &password, &database);
    let budget_first =
        bounded_client("SELECT clock_timestamp() + interval '0 seconds' /* budget */");
    let budget_second =
        bounded_client("SELECT clock_timestamp() + interval '0 seconds' /* budget */");
    assert_success(&budget_first, "first low-budget query");
    assert_success(&budget_second, "second low-budget query");
    assert_ne!(stdout(&budget_first).trim(), stdout(&budget_second).trim());
    server.stop();

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
    let server = start_server(
        GatewayConfig::new(user.clone(), password.clone(), database.clone())
            .unwrap()
            .with_cache(unavailable_cache),
        make_backends(),
        bind_select_backend(route(&default.backend_login), |_, selected| {
            Ok(selected.clone())
        }),
    );
    let address = server.address;
    let unavailable = psql("SELECT 12345", &address, &user, &password, &database);
    assert_success(&unavailable, "query while Redis is unavailable");
    assert_eq!(stdout(&unavailable).trim(), "12345");
    server.stop();

    #[cfg(feature = "python")]
    {
        use tokio::runtime::Builder;
        use yasp::{config::PythonSettings, hooks::python::PythonRuntime};

        let python_namespace = format!("{namespace}_python");
        let python_example_namespace = namespace.clone();
        let temporary = support::TempDirectory::new("yasp_cache_python");
        let directory = &temporary.0;
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
                    &support::root().join("hooks/examples/cache_eligibility.py"),
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
        let server = start_server(
            python_gateway,
            make_backends(),
            bind_select_backend(route(&default.backend_login), |_, selected| {
                Ok(selected.clone())
            }),
        );
        let address = server.address;
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
        let example_server = start_server(
            GatewayConfig::new(user.clone(), password.clone(), database.clone())
                .unwrap()
                .with_cache(example_cache),
            make_backends(),
            bind_select_backend(route(&default.backend_login), |_, selected| {
                Ok(selected.clone())
            }),
        );
        let example_address = example_server.address;
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

        example_server.stop();
        server.stop();
        python_runtime.shutdown();
    }
}
