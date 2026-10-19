#![cfg(feature = "snowflake")]
//! Data and definition persistence use a dedicated owned volume, never the demo.
#[test]
fn emulator_data_and_metadata_survive_container_recreation() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/snowflake/persistence.py"
        ))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "persistent Snowflake fixture: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
