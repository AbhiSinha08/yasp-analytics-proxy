use super::*;
use std::path::Path;

fn make_config(hooks: &str, targets: &str, backend: &str) -> FileConfig {
    serde_saphyr::from_str(&format!(
        "version: 1\nschema_discovery: {{target: primary}}\nhooks:\n  select_backend: {hooks}\nbackend:\n{backend}\ntargets:\n{targets}"
    ))
    .unwrap()
}

const ONE_TARGET: &str = "  primary:\n    engine: postgresql\n    host: localhost\n    port: 5432\n    database: analytics\n    tls_mode: disable\n    logins:\n      reader:\n        username: reader\n        password_env: READER_PASSWORD";

#[test]
fn discovery_default_follows_declaration_order_and_checks_explicit_pairs() {
    let yaml = format!(
        "version: 1\nhooks: {{select_backend: app.selector}}\ntargets:\n{}",
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
    let target: TargetSettings = serde_saphyr::from_str("engine: postgresql\nhost: localhost\nport: 5432\ndatabase: other\ntls_mode: disable\nlogins:\n  reader: {username: reader, password_env: OTHER_PASSWORD}").unwrap();
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
fn example_is_loadable_without_secrets_or_database_access() {
    let config =
        FileConfig::read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("config/example.yml"))
            .unwrap();
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
    let config: FileConfig =
        serde_saphyr::from_str(&format!("version: 1\ntargets:\n{ONE_TARGET}")).unwrap();
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
    for params in [
        "{target: primary}",
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
fn defaults_infer_one_pair_and_reject_pool_overcommit() {
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
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidPoolLimit)
    ));
}

#[test]
fn passthrough_requires_unambiguous_or_explicit_default() {
    let two_targets = format!(
        "{ONE_TARGET}\n  reporting:\n    engine: postgresql\n    host: localhost\n    port: 5432\n    database: reports\n    tls_mode: disable\n    logins:\n      reader:\n        username: reader\n        password_env: REPORTS_PASSWORD"
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
