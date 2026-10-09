//! Exercise the compiled host through real PostgreSQL clients and TCP frames.

#[cfg(unix)]
#[test]
fn gateway_contract() {
    let status = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/gateway_check.py"
        ))
        .arg(env!("CARGO_BIN_EXE_yasp"))
        .status()
        .expect("gateway checks require Python 3 and psql/libpq");
    assert!(status.success(), "gateway integration checks failed");
}
