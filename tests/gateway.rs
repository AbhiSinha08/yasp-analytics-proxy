//! Exercise the compiled host through real PostgreSQL clients and TCP frames.

#[cfg(unix)]
mod support;

#[cfg(unix)]
#[test]
fn gateway_contract() {
    use std::{io::Write, process::Stdio};
    use yasp::config::{BackendConfig, GatewayConfig};

    let environment = support::TestEnvironment::load();
    let config = &environment.config;
    let route = config
        .default_backend()
        .expect("test default must be configured");
    let target = &config.targets[&route.target];
    let login = &target.logins[&route.backend_login];
    let value = |key: &str| environment.value(key);
    let gateway_username = value("YASP_GATEWAY_USERNAME");
    let gateway_database = value("YASP_GATEWAY_DATABASE");
    let gateway_password = value("YASP_GATEWAY_PASSWORD");
    let target_password = value(&login.password_env);
    GatewayConfig::new(
        gateway_username.clone(),
        gateway_password.clone(),
        gateway_database.clone(),
    )
    .expect("test gateway identity must be valid")
    .with_settings(config.gateway.clone())
    .expect("test gateway settings must be valid before database provisioning");
    BackendConfig::new(
        target.host.clone(),
        target.port,
        login.username.clone(),
        target_password.clone(),
        target.database.clone(),
    )
    .expect("test target settings must be valid before database provisioning");
    let mut command = std::process::Command::new("python3");
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/gateway_check.py"
        ))
        .arg(env!("CARGO_BIN_EXE_yasp"))
        .env("YASP_GATEWAY_USERNAME", &gateway_username)
        .env("YASP_GATEWAY_DATABASE", &gateway_database)
        .env("YASP_GATEWAY_PASSWORD", gateway_password)
        .env(&login.password_env, target_password)
        .stdin(Stdio::piped());
    for target in config.targets.values() {
        for login in target.logins.values() {
            command.env(&login.password_env, value(&login.password_env));
        }
    }
    let mut process = support::ChildGuard::new(
        command
            .spawn()
            .expect("gateway checks require Python 3 and psql/libpq"),
    );
    process
        .child()
        .stdin
        .take()
        .expect("test input is piped")
        .write_all(
            serde_json::json!({
                "config": config,
                "default_backend": route,
                "python_enabled": cfg!(feature = "python"),
                "gateway": {
                    "username": gateway_username,
                    "database": gateway_database,
                    "password_env": "YASP_GATEWAY_PASSWORD"
                }
            })
            .to_string()
            .as_bytes(),
        )
        .expect("test configuration must reach the Python harness");
    let status = process.child().wait().expect("gateway checks must finish");
    assert!(status.success(), "gateway integration checks failed");
}
