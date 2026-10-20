use super::*;

pub(super) fn pool_isolation(
    client: &(dyn Fn(&[&str]) -> Output + Sync),
    maintenance: &dyn Fn(&str, &str) -> Output,
    check: &dyn Fn(&[&str], &Output),
    database_names: &[String; 2],
    route_one_marker: &str,
    route_two_marker: &str,
) {
    let pids = client(&[
        "SELECT pg_backend_pid() AS source_pid",
        "SELECT 'route_two' AS route_two, pg_backend_pid()",
        "SELECT 'route_owner' AS route_owner, pg_backend_pid()",
        "SELECT pg_backend_pid() AS source_pid",
    ]);
    assert!(pids.status.success(), "routed pool reuse query failed");
    let pids: Vec<_> = String::from_utf8_lossy(&pids.stdout)
        .lines()
        .map(|line| line.rsplit('|').next().unwrap_or(line).to_owned())
        .collect();
    assert_eq!(pids.len(), 4, "each routed query returned one backend PID");
    assert_eq!(
        pids[0], pids[3],
        "the first route reuses its own pool connection"
    );
    assert_ne!(
        pids[0], pids[1],
        "the two routes have isolated pool connections"
    );
    assert_ne!(
        pids[1], pids[2],
        "different login profiles on the same target have separate pools"
    );

    thread::scope(|scope| {
        let held = scope.spawn(|| client(&["SELECT pg_sleep(12)"]));
        let started = Instant::now();
        loop {
            let active = maintenance(
                &format!(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = '{}' AND query = 'SELECT pg_sleep(12)' AND state = 'active'",
                    database_names[0]
                ),
                "postgres",
            );
            if String::from_utf8_lossy(&active.stdout).trim() == "1" {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "route query did not occupy its pool"
            );
            thread::sleep(Duration::from_millis(30));
        }
        let started = Instant::now();
        let other = client(&["SELECT 'route_two', current_database() || ':' || current_user"]);
        check(&[route_two_marker], &other);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "one saturated route blocked a different pool"
        );
        let started = Instant::now();
        let exhausted = client(&[
            "SELECT 1",
            "SELECT current_database() || ':' || current_user",
        ]);
        assert!(
            String::from_utf8_lossy(&exhausted.stderr).contains("53300"),
            "route capacity was not enforced"
        );
        assert!(started.elapsed() >= Duration::from_secs(9));
        assert!(
            String::from_utf8_lossy(&exhausted.stdout).contains(route_one_marker),
            "session did not recover after pool exhaustion"
        );
        assert!(held.join().unwrap().status.success());
    });
}
