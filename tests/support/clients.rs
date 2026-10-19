//! Owned external driver workload, shared by JDBC and SQLAlchemy acceptance.
#![allow(dead_code)] // Each integration-test binary uses its own workload.
use super::support;
use std::{
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
};
pub(super) fn run_client(
    profile: &str,
    identity: &support::Identity,
    address: std::net::SocketAddr,
    schema: &str,
    table: &str,
    sqlalchemy: bool,
) -> serde_json::Value {
    run_workload(
        profile,
        identity,
        address,
        schema,
        table,
        sqlalchemy,
        include_bytes!("../fixtures/clients/jdbc_probe.clj"),
    )
}

pub(super) fn run_metabase(
    profile: &str,
    identity: &support::Identity,
    address: std::net::SocketAddr,
) -> serde_json::Value {
    run_workload(
        profile,
        identity,
        address,
        "PUBLIC",
        "USERS",
        false,
        include_bytes!("../fixtures/clients/metabase_connect.clj"),
    )
}

fn run_workload(
    profile: &str,
    identity: &support::Identity,
    address: std::net::SocketAddr,
    schema: &str,
    table: &str,
    sqlalchemy: bool,
    script: &[u8],
) -> serde_json::Value {
    let directory = PathBuf::from(std::env::var_os("YASP_TEST_TLS_DIR").unwrap());
    let image = std::env::var("YASP_JDBC_IMAGE").unwrap();
    let name = support::unique_name("yasp-jdbc");
    let mut command = Command::new("docker");
    command.args([
        "run",
        "--rm",
        "--pull=never",
        "-i",
        "--name",
        &name,
        "--label",
        &format!(
            "yasp.compatibility={}",
            std::env::var("YASP_JDBC_RUN").unwrap()
        ),
    ]);
    for key in [
        "YASP_JDBC_PROFILE",
        "YASP_JDBC_USER",
        "YASP_JDBC_PASSWORD",
        "YASP_JDBC_HOST",
        "YASP_JDBC_PORT",
        "YASP_JDBC_DATABASE",
        "YASP_JDBC_SCHEMA",
        "YASP_JDBC_TABLE",
    ] {
        command.args(["--env", key]);
    }
    command.args([
        "--mount",
        &format!(
            "type=bind,src={},dst=/tmp/yasp-fixture-ca.crt,readonly",
            directory.join("ca.crt").display()
        ),
    ]);
    if sqlalchemy {
        command.args([
            "--entrypoint",
            "python",
            &std::env::var("YASP_SQLALCHEMY_IMAGE").unwrap(),
            "/app/probe.py",
        ]);
    } else {
        command.args([
            "--entrypoint",
            "java",
            &image,
            "-Xmx256m",
            "-cp",
            "/app/metabase.jar",
            "clojure.main",
            "-",
        ]);
    }
    command
        .env("YASP_JDBC_SCHEMA", schema)
        .env("YASP_JDBC_TABLE", table)
        .env("YASP_JDBC_PROFILE", profile)
        .env("YASP_JDBC_USER", &identity.user)
        .env("YASP_JDBC_PASSWORD", &identity.password)
        .env("YASP_JDBC_DATABASE", &identity.database)
        .env("YASP_JDBC_PORT", address.port().to_string())
        .env(
            "YASP_JDBC_HOST",
            std::env::var("YASP_JDBC_HOST").unwrap_or_else(|_| "host.docker.internal".into()),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("Docker client starts");
    if !sqlalchemy {
        child.stdin.take().unwrap().write_all(script).unwrap();
    }
    let result = child.wait_with_output().unwrap();
    // The name belongs exclusively to this run; --rm normally removed it.
    let _ = Command::new("docker").args(["rm", "-f", &name]).output();
    let stdout = String::from_utf8_lossy(&result.stdout);
    let report = stdout
        .lines()
        .find_map(|line| line.strip_prefix("RESULT "))
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap());
    report.unwrap_or_else(
        || serde_json::json!({"connected":false, "error":String::from_utf8_lossy(&result.stderr)}),
    )
}
