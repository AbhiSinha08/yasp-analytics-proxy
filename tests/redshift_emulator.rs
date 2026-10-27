#![cfg(feature = "redshift-tests")]
//! Durable local data is checked separately from native Redshift conformance.

#[test]
fn persisted_data_and_metadata_survive_recreation_without_hiding_navigation_gaps() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/redshift/persistence.py"
        ))
        .arg("--baseline")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Redshift persistence baseline: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["baseline_passed"], true);
    let observations = report["observations"].as_array().unwrap();
    for required in ["data", "metadata", "super_data", "super_navigation"] {
        let observation = observations
            .iter()
            .find(|item| item["case"] == required)
            .expect("each persistence capability must be exercised");
        if required != "super_navigation" {
            assert_eq!(observation["passed"], true, "{observation}");
        }
    }
    assert_eq!(
        report["passed"],
        observations.iter().all(|item| item["passed"] == true),
        "full conformance must include the native navigation result"
    );
}
