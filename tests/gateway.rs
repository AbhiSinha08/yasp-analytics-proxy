//! Exercise the compiled host through real PostgreSQL clients and TCP frames.

#[cfg(unix)]
#[test]
fn gateway_contract() {
    use std::{collections::HashMap, io::Write, path::PathBuf, process::Stdio};
    use yasp::config::{BackendConfig, FileConfig, GatewayConfig};

    let path = std::env::var_os("YASP_TEST_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/config.yml")));
    let config = FileConfig::read(&path).expect("test configuration must be valid YAML");
    let route = config
        .default_backend()
        .expect("test default must be configured");
    let target = &config.targets[&route.target];
    let login = &target.logins[&route.backend_login];
    let file_values: HashMap<String, String> =
        match dotenvy::from_path_iter(concat!(env!("CARGO_MANIFEST_DIR"), "/.env")) {
            Ok(values) => {
                let mut file_values = HashMap::new();
                for value in values {
                    let (key, value) =
                        value.unwrap_or_else(|_| panic!("test secrets file must be valid dotenv"));
                    file_values.entry(key).or_insert(value);
                }
                file_values
            }
            Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                HashMap::new()
            }
            Err(_) => panic!("cannot read test secrets file"),
        };
    let value = |key: &str| match std::env::var(key) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => file_values
            .get(key)
            .cloned()
            .unwrap_or_else(|| panic!("a configured test environment variable is missing")),
        Err(_) => panic!("test environment variables must contain Unicode text"),
    };
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
    let mut process = command
        .spawn()
        .expect("gateway checks require Python 3 and psql/libpq");
    process
        .stdin
        .take()
        .expect("test input is piped")
        .write_all(
            serde_json::json!({
                "config": config,
                "default_backend": route,
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
    let status = process.wait().expect("gateway checks must finish");
    assert!(status.success(), "gateway integration checks failed");
}
