use super::{assert_success, cleanup_result, root, unique_name};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    process::{Command, Output},
};
use yasp::config::{BackendConfig, FileConfig, GatewayConfig};

pub struct TestEnvironment {
    pub config: FileConfig,
    values: HashMap<String, String>,
}

impl TestEnvironment {
    pub fn load() -> Self {
        let path = std::env::var_os("YASP_TEST_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| root().join("tests/config.yml"));
        let config = FileConfig::read(&path).expect("test configuration must be valid");
        let mut values = HashMap::new();
        match dotenvy::from_path_iter(root().join(".env")) {
            Ok(items) => {
                for item in items {
                    let (key, value) = item.expect("test dotenv must be valid");
                    values.entry(key).or_insert(value);
                }
            }
            Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("cannot read test dotenv: {error}"),
        }
        Self { config, values }
    }

    pub fn value(&self, key: &str) -> String {
        match std::env::var(key) {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => self
                .values
                .get(key)
                .cloned()
                .unwrap_or_else(|| panic!("required test environment value is missing: {key}")),
            Err(_) => panic!("test environment value must be Unicode: {key}"),
        }
    }

    pub fn gateway(&self) -> Identity {
        Identity {
            user: self.value("YASP_GATEWAY_USERNAME"),
            password: self.value("YASP_GATEWAY_PASSWORD"),
            database: self.value("YASP_GATEWAY_DATABASE"),
        }
    }

    pub fn source(&self, login_name: Option<&str>) -> PostgresConnection {
        let route = self
            .config
            .default_backend()
            .expect("test default route exists");
        let target = &self.config.targets[&route.target];
        let login = &target.logins[login_name.unwrap_or(&route.backend_login)];
        PostgresConnection {
            host: target.host.clone(),
            port: target.port,
            identity: Identity {
                user: login.username.clone(),
                password: self.value(&login.password_env),
                database: target.database.clone(),
            },
        }
    }
}

#[derive(Clone)]
pub struct Identity {
    pub user: String,
    pub password: String,
    pub database: String,
}

impl Identity {
    pub fn gateway_config(&self) -> GatewayConfig {
        GatewayConfig::new(
            self.user.clone(),
            self.password.clone(),
            self.database.clone(),
        )
        .expect("test gateway identity is valid")
    }

    pub fn connection(&self, address: SocketAddr) -> PostgresConnection {
        PostgresConnection {
            host: address.ip().to_string(),
            port: address.port(),
            identity: self.clone(),
        }
    }
}

#[derive(Clone)]
pub struct PostgresConnection {
    pub host: String,
    pub port: u16,
    pub identity: Identity,
}

impl PostgresConnection {
    pub fn database(&self, database: &str) -> Self {
        let mut connection = self.clone();
        connection.identity.database = database.into();
        connection
    }

    pub fn backend_config(&self) -> BackendConfig {
        BackendConfig::new(
            self.host.clone(),
            self.port,
            self.identity.user.clone(),
            self.identity.password.clone(),
            self.identity.database.clone(),
        )
        .expect("test PostgreSQL connection is valid")
    }

    pub fn command(&self) -> Command {
        let mut command = Command::new("psql");
        command
            .args(["-X", "-w", "-At", "-v", "VERBOSITY=verbose"])
            .env("PGPASSFILE", "/dev/null")
            .env("PSQL_HISTORY", "/dev/null")
            .env("PGSSLMODE", "disable")
            .env("PGCONNECT_TIMEOUT", "3")
            .env("PGHOST", &self.host)
            .env("PGPORT", self.port.to_string())
            .env("PGUSER", &self.identity.user)
            .env("PGPASSWORD", &self.identity.password)
            .env("PGDATABASE", &self.identity.database);
        command
    }

    pub fn query(&self, sql: &str) -> Output {
        self.command()
            .args(["-v", "ON_ERROR_STOP=1", "-c", sql])
            .output()
            .expect("psql and libpq are installed")
    }

    pub fn queries(&self, sql: &[&str]) -> Output {
        let mut command = self.command();
        for query in sql {
            command.args(["-c", query]);
        }
        command.output().expect("psql and libpq are installed")
    }

    pub fn execute(&self, sql: &str) -> Output {
        let output = self.query(sql);
        assert_success(&output, "PostgreSQL fixture operation");
        output
    }
}

pub struct TestDatabase {
    pub connection: PostgresConnection,
    maintenance: PostgresConnection,
}

impl TestDatabase {
    pub fn create(maintenance: &PostgresConnection, prefix: &str) -> Self {
        let name = unique_name(prefix);
        // Register cleanup before provisioning so a fixture failure drops the database.
        let database = Self {
            connection: maintenance.database(&name),
            maintenance: maintenance.clone(),
        };
        maintenance.execute(&format!(
            "CREATE DATABASE {} TEMPLATE template0",
            quote_ident(&name)
        ));
        database
    }

    pub fn populate(&self) {
        let output = self
            .connection
            .command()
            .args(["-v", "ON_ERROR_STOP=1", "-f"])
            .arg(root().join("tests/fixtures/read_forwarding.sql"))
            .output()
            .expect("psql is installed");
        assert_success(&output, "populate disposable PostgreSQL database");
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let output = self.maintenance.query(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            quote_ident(&self.connection.identity.database)
        ));
        cleanup_result(
            output.status.success(),
            "drop disposable PostgreSQL database",
        );
    }
}

pub fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
