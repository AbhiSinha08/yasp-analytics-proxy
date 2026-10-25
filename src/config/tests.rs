use super::*;
use std::path::Path;

fn make_config(hooks: &str, targets: &str, backend: &str) -> FileConfig {
    serde_saphyr::from_str(&format!(
        "version: 1\nschema_discovery: {{target: primary, catalog: {{schemas: [public]}}}}\nhooks:\n  select_backend: {hooks}\nbackend:\n{backend}\ntargets:\n{targets}"
    ))
    .unwrap()
}

const ONE_TARGET: &str = "  primary:\n    engine: postgresql\n    host: localhost\n    port: 5432\n    database: analytics\n    tls:\n      mode: disable\n    logins:\n      reader:\n        username: reader\n        password_env: READER_PASSWORD";

#[test]
fn query_modification_references_validate_before_startup() {
    let config = make_config("builtin.select_backend.passthrough", ONE_TARGET, "  {}");
    assert!(config.hooks.modify_query.is_none());
    let base = serde_json::to_value(config).unwrap();
    for reference in [
        serde_json::json!("app.rewrite"),
        serde_json::json!({"type":"rust","callback":"app.rewrite","params":{"table":"orders"}}),
        serde_json::json!({"type":"python","path":"rewrite.py","callback":"modify","params":{}}),
    ] {
        let mut value = base.clone();
        value["hooks"]["modify_query"] = reference;
        let config: FileConfig = serde_json::from_value(value).unwrap();
        config.validate().unwrap();
    }
    for reference in [
        serde_json::json!(""),
        serde_json::json!({"type":"rust","callback":" "}),
        serde_json::json!({"type":"python","path":"","callback":"modify"}),
        serde_json::json!({"type":"python","path":"rewrite.py","callback":""}),
    ] {
        let mut value = base.clone();
        value["hooks"]["modify_query"] = reference;
        let config: FileConfig = serde_json::from_value(value).unwrap();
        assert!(config.validate().is_err());
    }
}

#[test]
fn optional_data_contexts_validate_nested_decision_model_configuration() {
    let yaml = format!(
        "version: 1\nschema_discovery: {{catalog: {{schemas: [public]}}}}\ntargets:\n{ONE_TARGET}\n"
    );
    for suffix in ["", "data_contexts: {}\n"] {
        let config: FileConfig = serde_saphyr::from_str(&(yaml.clone() + suffix)).unwrap();
        config.validate().unwrap();
        assert!(config.data_contexts.decision_model.is_none());
    }
    let config: FileConfig = serde_saphyr::from_str(
        &(yaml
            + "data_contexts:\n  decision_model:\n    endpoint: http://127.0.0.1:8001/v1/systemone\n    questions:\n      email: Detect email addresses\n"),
    )
    .unwrap();
    config.validate().unwrap();
    let model = config.data_contexts.decision_model.as_ref().unwrap();
    assert_eq!(model.sample_size, 5);
    assert!(model.include_sql);
    assert_eq!(model.questions["email"], "Detect email addresses");

    let value = serde_json::to_value(&config).unwrap();
    assert!(value.get("decision_model").is_none());
    let round_trip: FileConfig = serde_json::from_value(value.clone()).unwrap();
    round_trip.validate().unwrap();

    let mut custom = value.clone();
    custom["data_contexts"]["ast"] = serde_json::json!({"enabled":true,"namespace":"custom:ast"});
    custom["data_contexts"]["decision_model"]["namespace"] = "custom:decisions".into();
    custom["data_contexts"]["decision_model"]["include_sql"] = false.into();
    assert!(
        !serde_json::from_value::<FileConfig>(custom.clone())
            .unwrap()
            .data_contexts
            .decision_model
            .unwrap()
            .include_sql
    );
    let mut wrong_type = custom.clone();
    wrong_type["data_contexts"]["decision_model"]["include_sql"] = "false".into();
    assert!(serde_json::from_value::<FileConfig>(wrong_type).is_err());
    serde_json::from_value::<FileConfig>(custom.clone())
        .unwrap()
        .validate()
        .unwrap();
    for name in ["ast", "decision_model"] {
        for namespace in ["", "   ", "bad\0namespace"] {
            let mut invalid = custom.clone();
            invalid["data_contexts"][name]["namespace"] = namespace.into();
            assert!(
                serde_json::from_value::<FileConfig>(invalid)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
    }
    let mut collision = custom.clone();
    collision["cache"]["enabled"] = true.into();
    collision["data_contexts"]["ast"]["namespace"] = collision["cache"]["namespace"].clone();
    assert!(matches!(
        serde_json::from_value::<FileConfig>(collision)
            .unwrap()
            .validate(),
        Err(ConfigError::InvalidCacheNamespaces)
    ));
    let mut nested_ast = value.clone();
    nested_ast["data_contexts"]["decision_model"]["ast"] = serde_json::json!({"enabled":true});
    assert!(serde_json::from_value::<FileConfig>(nested_ast).is_err());
    let mut invalid = value.clone();
    invalid["data_contexts"]["decision_model"]["sample_size"] = 0.into();
    assert!(matches!(
        serde_json::from_value::<FileConfig>(invalid)
            .unwrap()
            .validate(),
        Err(ConfigError::InvalidDecisionModel)
    ));
    let mut unknown = value.clone();
    unknown["data_contexts"]["unknown"] = true.into();
    assert!(serde_json::from_value::<FileConfig>(unknown).is_err());
    let mut top_level = value;
    top_level["decision_model"] = top_level["data_contexts"]["decision_model"].clone();
    top_level.as_object_mut().unwrap().remove("data_contexts");
    assert!(serde_json::from_value::<FileConfig>(top_level).is_err());
}

#[test]
fn plaintext_remote_connections_require_no_certificate_material() {
    let mut gateway = GatewaySettings {
        listen: "0.0.0.0:6432".parse().unwrap(),
        ..Default::default()
    };
    assert_eq!(gateway.tls.mode, "disable");
    gateway.validate().unwrap();
    GatewayConfig::new("bi".into(), "password".into(), "analytics".into())
        .unwrap()
        .with_settings(gateway.clone())
        .unwrap();
    gateway.tls.cert_file = Some("unused.pem".into());
    assert!(
        gateway.validate().is_err(),
        "disabled TLS rejects ignored certificate fields"
    );
    for host in ["10.0.1.20", "postgres.internal"] {
        let config = make_config(
            "app.selector",
            &ONE_TARGET.replace("localhost", host),
            "  {}",
        );
        config.validate().unwrap();
        BackendConfig::new(
            host.into(),
            5432,
            "reader".into(),
            "password".into(),
            "analytics".into(),
        )
        .unwrap();
    }
}

#[test]
fn tls_modes_and_target_certificate_fields_use_one_nested_schema() {
    let mut config = make_config("app.selector", ONE_TARGET, "  {}");
    let tls: TargetTls = serde_saphyr::from_str(
        "mode: enable\nca_file: ca.pem\ncert_file: client.pem\nkey_file: client-key.pem",
    )
    .unwrap();
    config.targets.get_mut("primary").unwrap().tls = tls;
    config.validate().unwrap();
    let value = serde_json::to_value(&config).unwrap();
    let tls = &value["targets"]["primary"]["tls"];
    assert_eq!(tls["mode"], "enable");
    assert_eq!(tls["ca_file"], "ca.pem");
    assert_eq!(tls["cert_file"], "client.pem");
    assert_eq!(tls["key_file"], "client-key.pem");
    let round_trip: FileConfig = serde_json::from_value(value.clone()).unwrap();
    round_trip.validate().unwrap();
    let mut old_field = value.clone();
    old_field["targets"]["primary"]["tls_mode"] = "disable".into();
    assert!(serde_json::from_value::<FileConfig>(old_field).is_err());
    let mut unknown_field = value;
    unknown_field["targets"]["primary"]["tls"]["unknown"] = true.into();
    assert!(serde_json::from_value::<FileConfig>(unknown_field).is_err());
}

#[test]
fn discovery_default_follows_declaration_order_and_checks_explicit_pairs() {
    let yaml = format!(
        "version: 1\nschema_discovery: {{catalog: {{schemas: [public]}}}}\nhooks: {{select_backend: app.selector}}\ntargets:\n{}",
        ONE_TARGET.replace("reader:\n", "z_first:\n")
    ) + "\n      a_second:\n        username: second\n        password_env: SECOND_PASSWORD";
    let mut config: FileConfig = serde_saphyr::from_str(&yaml).unwrap();
    config.validate().unwrap();
    assert_eq!(config.discovery_backend().unwrap().backend_login, "z_first");
    let round_trip: FileConfig =
        serde_saphyr::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
    assert_eq!(
        round_trip.discovery_backend().unwrap().backend_login,
        "z_first"
    );
    config.schema_discovery.backend_login = Some("a_second".into());
    assert_eq!(
        config.discovery_backend().unwrap().backend_login,
        "a_second"
    );
    config.schema_discovery.backend_login = Some("absent".into());
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidDiscoveryBackend)
    ));
    config.schema_discovery.backend_login = None;
    let target: TargetSettings = serde_saphyr::from_str("engine: postgresql\nhost: localhost\nport: 5432\ndatabase: other\ntls:\n  mode: disable\nlogins:\n  reader: {username: reader, password_env: OTHER_PASSWORD}").unwrap();
    config.targets.insert("other".into(), target);
    config.backend.max_connections_total = 24;
    assert!(matches!(
        config.validate(),
        Err(ConfigError::MissingDiscoveryTarget)
    ));
    config.schema_discovery.target = Some("primary".into());
    config.validate().unwrap();
}

#[test]
fn catalog_configuration_requires_bounded_explicit_schema_scope() {
    let config = make_config("app.selector", ONE_TARGET, "  {}");
    let mut missing = make_config("app.selector", ONE_TARGET, "  {}");
    missing.schema_discovery.catalog = None;
    assert!(matches!(
        missing.validate(),
        Err(ConfigError::InvalidCatalog)
    ));
    let mut value = serde_json::to_value(&config).unwrap();
    value["schema_discovery"]["catalog"] = serde_json::json!({"schemas": ["Mixed.Schema's"]});
    let config: FileConfig = serde_json::from_value(value.clone()).unwrap();
    config.validate().unwrap();
    let settings = config.schema_discovery.catalog.unwrap().runtime().unwrap();
    assert_eq!(settings.schemas, ["Mixed.Schema's"]);
    assert_eq!(settings.refresh_interval.as_secs(), 60);
    for invalid in [
        serde_json::json!({}),
        serde_json::json!({"schemas": []}),
        serde_json::json!({"schemas": ["public"], "refresh_interval_seconds": 0}),
        serde_json::json!({"schemas": ["public"], "refresh_timeout_ms": 0}),
        serde_json::json!({"schemas": ["public"], "max_age_seconds": 1}),
        serde_json::json!({"schemas": ["public"], "max_rows": 0}),
        serde_json::json!({"schemas": ["public"], "max_bytes": 67108865}),
    ] {
        value["schema_discovery"]["catalog"] = invalid;
        let config: FileConfig = serde_json::from_value(value.clone()).unwrap();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidCatalog)
        ));
    }
    value["schema_discovery"]["catalog"] = serde_json::json!({"schemas": ["public"], "unknown": 1});
    assert!(serde_json::from_value::<FileConfig>(value).is_err());
}

#[test]
fn cache_settings_and_bounds_are_validated() {
    let mut cache = CacheSettings::default();
    cache.validate().unwrap();
    assert_eq!(cache.ttl_seconds, 3600);
    cache.enabled = true;
    cache.validate().unwrap();
    for bad in [
        CacheSettings {
            ttl_seconds: 0,
            ..cache.clone()
        },
        CacheSettings {
            max_rows_per_query: 0,
            ..cache.clone()
        },
        CacheSettings {
            max_buffered_bytes_total: 0,
            ..cache.clone()
        },
        CacheSettings {
            namespace: String::new(),
            ..cache.clone()
        },
    ] {
        assert!(bad.validate().is_err());
    }
    cache.max_buffered_bytes_total = 1;
    cache.validate().unwrap(); // A small budget bypasses caching rather than execution.
}

#[test]
fn application_context_dataset_settings_are_bounded_and_well_formed() {
    let settings = ApplicationContextsSettings::default();
    settings.validate().unwrap();
    assert_eq!(settings.url_env, "YASP_CONTEXT_REDIS_URL");
    assert_eq!(settings.namespace, "yasp:datasets");
    assert_eq!(settings.refresh_timeout_ms, 30_000);
    assert_eq!(settings.max_dataset_bytes, 16_777_216);
    assert_eq!(settings.max_lookup_bytes, 65_536);
    assert!(settings.datasets.is_empty());

    let mut config = make_config("app.selector", ONE_TARGET, "  {}");
    config
        .application_contexts
        .datasets
        .insert("missing_loader".into(), DatasetSettings::default());
    assert!(config.validate().is_err());
    config.application_contexts.datasets.clear();
    config.application_contexts.datasets.insert(
        "email_locations".into(),
        DatasetSettings {
            refresh_interval_seconds: 0,
            loader: SelectorSettings::Rust("example.datasets.email_locations".into()),
        },
    );
    config.validate().unwrap();

    config.application_contexts.datasets.insert(
        "unsafe/name".into(),
        DatasetSettings {
            refresh_interval_seconds: 0,
            loader: SelectorSettings::Rust("example.datasets.email_locations".into()),
        },
    );
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidApplicationContexts)
    ));

    config.application_contexts.datasets.remove("unsafe/name");
    config.application_contexts.max_lookup_bytes =
        config.application_contexts.max_dataset_bytes + 1;
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidApplicationContexts)
    ));

    config.application_contexts.max_lookup_bytes = 65_536;
    config.application_contexts.datasets.insert(
        "email_locations".into(),
        DatasetSettings {
            refresh_interval_seconds: 0,
            loader: SelectorSettings::Rust(String::new()),
        },
    );
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidValue(_))
    ));

    config.application_contexts.datasets.insert(
        "email_locations".into(),
        DatasetSettings {
            refresh_interval_seconds: 0,
            loader: SelectorSettings::Python(PythonHookSettings {
                kind: PythonHookKind::Python,
                path: "".into(),
                callback: String::new(),
                params: serde_json::Map::new(),
            }),
        },
    );
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidValue(_))
    ));
}

#[test]
fn example_is_loadable_without_secrets_or_database_access() {
    let config =
        FileConfig::read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("config/example.yml"))
            .unwrap();
    assert_eq!(config.cache.namespace, "yasp:cache");
    assert_eq!(config.application_contexts.namespace, "yasp:datasets");
    let selected = config.default_backend().unwrap();
    assert_eq!(selected.target, "primary");
    assert_eq!(selected.backend_login, "restricted");
    assert_eq!(
        config
            .targets
            .values()
            .map(|target| target.logins.len())
            .sum::<usize>(),
        4
    );
}

#[test]
fn application_params_are_preserved_for_both_languages() {
    let params = serde_json::json!({"rbac": {"group": [true, null, 4]}, "label": "custom"});
    for reference in [
        "{type: rust, callback: app.selector, params: {rbac: {group: [true, null, 4]}, label: custom}}",
        "{type: python, path: ../app.py, callback: custom, params: {rbac: {group: [true, null, 4]}, label: custom}}",
    ] {
        let config = make_config(reference, ONE_TARGET, "  {}");
        config.validate().unwrap();
        let preserved = match config.hooks.select_backend {
            SelectorSettings::RustReference(hook) => hook.params,
            SelectorSettings::Python(hook) => hook.params,
            _ => panic!("object reference expected"),
        };
        assert_eq!(serde_json::Value::Object(preserved), params);
    }
    for reference in [
        "{type: rust, callback: builtin.select_backend.passthrough}",
        "{type: rust, callback: builtin.select_backend.passthrough, params: {}}",
    ] {
        let config = make_config(reference, ONE_TARGET, "  {}");
        config.validate().unwrap();
        assert_eq!(config.default_backend().unwrap().backend_login, "reader");
    }
    let config: FileConfig = serde_saphyr::from_str(&format!(
        "version: 1\nschema_discovery: {{catalog: {{schemas: [public]}}}}\ntargets:\n{ONE_TARGET}"
    ))
    .unwrap();
    config.validate().unwrap();
    assert_eq!(config.default_backend().unwrap().target, "primary");
}

#[test]
fn rejects_malformed_references_and_unsupported_sections() {
    for reference in [
        "{type: rust, callback: builtin.select_backend.passthrough, path: app.py}",
        "{type: rust, callback: app.selector, params: [1]}",
        "{type: python, path: app.py, callback: custom, params: [1]}",
        "{type: python, callback: custom}",
    ] {
        assert!(
            serde_saphyr::from_str::<HooksSettings>(&format!("select_backend: {reference}"))
                .is_err()
        );
    }
    for section in [
        "routing: {default_backend: {target: primary, backend_login: reader}}",
        "principals: {}",
        "policy: {result_masks: []}",
        "hooks: {result_processing: {enabled: false}}",
        "hooks: {python: {worker_count: 1}}",
    ] {
        assert!(
            serde_saphyr::from_str::<FileConfig>(&format!(
                "version: 1\ntargets:\n{ONE_TARGET}\n{section}"
            ))
            .is_err()
        );
    }
    let partial = make_config(
        "{type: rust, callback: builtin.select_backend.passthrough, params: {target: primary}}",
        ONE_TARGET,
        "  {}",
    );
    partial.validate().unwrap();
    assert_eq!(
        partial.default_decision().unwrap().target.as_deref(),
        Some("primary")
    );
    for params in [
        "{target: 1, backend_login: reader}",
        "{target: primary, backend_login: reader, extra: true}",
    ] {
        let config = make_config(
            &format!(
                "{{type: rust, callback: builtin.select_backend.passthrough, params: {params}}}"
            ),
            ONE_TARGET,
            "  {}",
        );
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidSelectorParams)
        ));
    }
    for params in [
        "{target: '', backend_login: reader}",
        "{target: primary, backend_login: ' '}",
    ] {
        let config = make_config(
            &format!(
                "{{type: rust, callback: builtin.select_backend.passthrough, params: {params}}}"
            ),
            ONE_TARGET,
            "  {}",
        );
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidValue(_))
        ));
    }
}

#[test]
fn python_reference_and_runtime_bounds() {
    let reference =
        "{type: python, path: ../app.py, callback: custom, params: {nested: [true, null, 4]}}";
    let mut config = make_config(
        reference,
        ONE_TARGET,
        "  max_connections_per_pool: 8\n  max_connections_total: 8",
    );
    assert!(config.validate().is_ok());
    let SelectorSettings::Python(hook) = &config.hooks.select_backend else {
        panic!("Python reference expected")
    };
    assert_eq!(hook.path, Path::new("../app.py"));
    assert_eq!(hook.callback, "custom");
    config.hooks.python.queue_max_bytes = config.hooks.python.max_payload_bytes - 1;
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidPythonLimit)
    ));
    for reference in [
        "{type: python, path: app.py, callback: custom, unexpected: true}",
        "{type: unknown, path: app.py, callback: custom}",
    ] {
        assert!(
            serde_saphyr::from_str::<HooksSettings>(&format!("select_backend: {reference}"))
                .is_err()
        );
    }
}

#[test]
fn defaults_infer_one_pair_and_shared_cap_may_be_below_pool_maxima() {
    let config = make_config(
        "builtin.select_backend.passthrough",
        ONE_TARGET,
        "  max_connections_per_pool: 8\n  max_connections_total: 8",
    );
    assert_eq!(config.default_backend().unwrap().target, "primary");
    assert!(config.validate().is_ok());

    let config = make_config(
        "builtin.select_backend.passthrough",
        ONE_TARGET,
        "  max_connections_per_pool: 8\n  max_connections_total: 7",
    );
    assert!(config.validate().is_ok());
}

#[test]
fn compute_topology_validates_partial_discovery_scope_and_login_overrides() {
    let target = "  primary:\n    engine: postgresql\n    database: analytics\n    tls:\n      mode: disable\n    logins:\n      reader:\n        username: reader\n        password_env: READER_PASSWORD\n        allowed_computes: [small_a, small_b]\n        overrides:\n          small_b: {username: reader_b, password_env: READER_B_PASSWORD}\n    compute:\n      default_type: small\n      types:\n        small:\n          max_in_flight: 2\n          max_connections: 4\n          instances:\n            - {id: small_a, host: localhost, port: 5433}\n            - {id: small_b, host: localhost, port: 5434}";
    let ambiguous = make_config("custom.selector", target, "  max_connections_total: 1");
    assert!(matches!(
        ambiguous.validate(),
        Err(ConfigError::MissingDiscoveryCompute)
    ));

    let selected = format!(
        "version: 1\nschema_discovery: {{target: primary, compute: small_b, catalog: {{schemas: [public]}}}}\nhooks: {{select_backend: custom.selector}}\nbackend: {{max_connections_total: 1}}\ntargets:\n{target}"
    );
    let selected: FileConfig = serde_saphyr::from_str(&selected).unwrap();
    selected.validate().unwrap();
    assert_eq!(
        selected.discovery_compute().unwrap().as_deref(),
        Some("small_b")
    );

    let partial: FileConfig = serde_saphyr::from_str(&format!(
        "version: 1\nschema_discovery: {{target: primary, compute: small_a, catalog: {{schemas: [public]}}}}\nhooks:\n  select_backend: {{type: rust, callback: builtin.select_backend.passthrough, params: {{target: primary, compute_type: small}}}}\ntargets:\n{target}"
    ))
    .unwrap();
    partial.validate().unwrap();
    assert_eq!(
        partial.default_decision().unwrap().compute_type.as_deref(),
        Some("small")
    );

    let invalid = make_config(
        "{type: rust, callback: builtin.select_backend.passthrough, params: {target: primary, compute_type: missing}}",
        target,
        "  {}",
    );
    assert!(matches!(
        invalid.validate(),
        Err(ConfigError::InvalidDefaultBackend)
    ));

    let invalid = target.replace(
        "allowed_computes: [small_a, small_b]",
        "allowed_computes: [missing]",
    );
    let invalid = make_config("custom.selector", &invalid, "  {}");
    assert!(matches!(
        invalid.validate(),
        Err(ConfigError::InvalidComputeTopology)
    ));

    let invalid = target.replace(
        "small_b: {username: reader_b, password_env: READER_B_PASSWORD}",
        "other: {username: x, password_env: X}",
    );
    let invalid = make_config("custom.selector", &invalid, "  {}");
    assert!(matches!(
        invalid.validate(),
        Err(ConfigError::InvalidComputeTopology)
    ));

    let unreachable = target
        .replace("allowed_computes: [small_a, small_b]", "allowed_computes: [small_a]")
        .replace(
            "        small:\n          max_in_flight: 2\n          max_connections: 4\n          instances:",
            "        large:\n          max_in_flight: 2\n          max_connections: 4\n          instances:\n            - {id: large_a, host: localhost, port: 5435}\n        small:\n          max_in_flight: 2\n          max_connections: 4\n          instances:",
        );
    let unreachable = make_config("custom.selector", &unreachable, "  {}");
    assert!(matches!(
        unreachable.validate(),
        Err(ConfigError::InvalidComputeTopology)
    ));
}

#[test]
fn passthrough_partial_fields_resolve_and_validate_after_inference() {
    let target = "  primary:\n    engine: postgresql\n    database: analytics\n    tls:\n      mode: disable\n    logins:\n      reader: {username: reader, password_env: READER_PASSWORD}\n    compute:\n      types:\n        small:\n          max_in_flight: 2\n          max_connections: 4\n          instances:\n            - {id: small_a, host: localhost, port: 5433}\n            - {id: small_b, host: localhost, port: 5434}";
    let config_text = |params: &str| {
        format!(
            "version: 1\nschema_discovery: {{target: primary, compute: small_a, catalog: {{schemas: [public]}}}}\nhooks:\n  select_backend: {{type: rust, callback: builtin.select_backend.passthrough, params: {params}}}\ntargets:\n{target}"
        )
    };

    let config: FileConfig = serde_saphyr::from_str(&config_text("{compute_type: small}")).unwrap();
    config.validate().unwrap();
    assert_eq!(config.default_backend().unwrap().target, "primary");
    assert_eq!(config.default_backend().unwrap().backend_login, "reader");

    for params in ["{backend_login: missing}", "{compute_type: missing}"] {
        let config: FileConfig = serde_saphyr::from_str(&config_text(params)).unwrap();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidDefaultBackend)
        ));
    }

    let ordinary = make_config(
        "{type: rust, callback: builtin.select_backend.passthrough, params: {compute_type: small}}",
        ONE_TARGET,
        "  {}",
    );
    assert!(matches!(
        ordinary.default_decision(),
        Err(ConfigError::InvalidDefaultBackend)
    ));
}

#[test]
fn passthrough_requires_unambiguous_or_explicit_default() {
    let two_targets = format!(
        "{ONE_TARGET}\n  reporting:\n    engine: postgresql\n    host: localhost\n    port: 5432\n    database: reports\n    tls:\n      mode: disable\n    logins:\n      reader:\n        username: reader\n        password_env: REPORTS_PASSWORD"
    );
    let config = make_config(
        "builtin.select_backend.passthrough",
        &two_targets,
        "  max_connections_per_pool: 8\n  max_connections_total: 16",
    );
    assert!(matches!(
        config.validate(),
        Err(ConfigError::MissingDefaultBackend)
    ));

    let config = make_config(
        "{type: rust, callback: builtin.select_backend.passthrough, params: {target: reporting, backend_login: reader}}",
        &two_targets,
        "  max_connections_per_pool: 8\n  max_connections_total: 16",
    );
    assert!(config.validate().is_ok());
    assert_eq!(config.default_backend().unwrap().target, "reporting");

    let config = make_config(
        "custom.selector",
        &two_targets,
        "  max_connections_per_pool: 8\n  max_connections_total: 16",
    );
    assert!(config.validate().is_ok());
}

#[test]
fn rejects_invalid_default_and_non_postgresql_target() {
    let config = make_config(
        "{type: rust, callback: builtin.select_backend.passthrough, params: {target: missing, backend_login: reader}}",
        ONE_TARGET,
        "  max_connections_per_pool: 8\n  max_connections_total: 8",
    );
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidDefaultBackend)
    ));

    let config = make_config(
        "custom.selector",
        &ONE_TARGET.replace("engine: postgresql", "engine: mysql"),
        "  max_connections_per_pool: 8\n  max_connections_total: 8",
    );
    assert!(matches!(
        config.validate(),
        Err(ConfigError::UnsupportedTarget)
    ));
}

#[test]
fn snowflake_yaml_requires_its_feature_catalog_and_engine_specific_identity() {
    let mut config = make_config(
        "custom.selector",
        r#"  primary:
    engine: snowflake
    database: YASP_DEMO
    snowflake:
      account: local
      schema: PUBLIC
      warehouse: LOCAL
      emulator: 127.0.0.1:64616
    logins:
      reader:
        username: fake
        private_key_file: snowflake.p8
        role: PUBLIC"#,
        "  {}",
    );
    config.schema_discovery.catalog = None;
    if !cfg!(feature = "snowflake") {
        assert!(matches!(
            config.validate(),
            Err(ConfigError::SnowflakeDisabled)
        ));
        return;
    }
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidCatalog)
    ));
    config.schema_discovery.catalog = Some(SchemaCatalogSettings {
        schemas: vec!["PUBLIC".into()],
        ..Default::default()
    });
    config.validate().unwrap();
    assert_eq!(config.discovery_backend().unwrap().backend_login, "reader");
    config.targets.get_mut("primary").unwrap().host = "localhost".into();
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidSnowflake)
    ));
}
