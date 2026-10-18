use super::*;

pub(super) fn metadata_discovery(
    client: &dyn Fn(&[&str]) -> Output,
    maintenance: &dyn Fn(&str, &str) -> Output,
    callback_count: &AtomicUsize,
    database_names: &[String; 2],
) {
    // Metadata has its own configured source, even when the ordinary selector denies it.
    let before = callback_count.load(Ordering::SeqCst);
    let metadata = client(&[
        "SET application_name='discovery test'",
        "SET search_path=public",
        "SELECT datname FROM pg_database WHERE datistemplate=false ORDER BY datname /* route_denied */",
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name /* route_denied */",
        "SELECT feature_id FROM information_schema.sql_features ORDER BY feature_id /* route_denied */",
        "SELECT * FROM information_schema.view_table_usage WHERE EXISTS (SELECT 1 FROM information_schema.constraint_table_usage) /* route_denied */",
    ]);
    assert!(
        metadata.status.success(),
        "{}",
        String::from_utf8_lossy(&metadata.stderr)
    );
    let metadata_output = String::from_utf8_lossy(&metadata.stdout);
    assert!(metadata_output.contains(&database_names[0]));
    assert!(!metadata_output.contains(&database_names[1]));
    assert!(!metadata_output.lines().any(|line| line == "postgres"));
    assert_eq!(
        callback_count.load(Ordering::SeqCst),
        before,
        "discovery and setup bypass selection"
    );
    // Replay the psql 18 database/schema/table/describe queries against the
    // discovery source. A routing denial marker catches any classifier fallback.
    let user_oid = maintenance("SELECT 'public.users'::regclass::oid", &database_names[0]);
    let user_oid = String::from_utf8_lossy(&user_oid.stdout).trim().to_owned();
    let captured: Vec<String> =
        serde_json::from_str(include_str!("../fixtures/psql_discovery.json")).unwrap();
    for (index, query) in captured.iter().enumerate() {
        let query = query.replace("'25009'", &format!("'{user_oid}'"));
        let forwarded = client(&[&format!("{query}\n/* route_denied */")]);
        assert!(
            forwarded.status.success(),
            "psql discovery query {}: {}",
            index + 1,
            String::from_utf8_lossy(&forwarded.stderr)
        );
        let direct_query = if index == 0 {
            query.replace(
                "\nORDER BY 1;",
                "\nWHERE d.datname = current_database()\nORDER BY 1;",
            )
        } else {
            query
        };
        let direct = maintenance(&direct_query, &database_names[0]);
        assert_eq!(
            forwarded.stdout,
            direct.stdout,
            "psql discovery query {} must preserve native metadata",
            index + 1
        );
    }
    assert_eq!(
        callback_count.load(Ordering::SeqCst),
        before,
        "all captured psql discovery queries bypass selection"
    );
    let dbeaver: Vec<String> =
        serde_json::from_str(include_str!("../fixtures/dbeaver_discovery.json")).unwrap();
    for (index, query) in dbeaver.iter().enumerate() {
        let forwarded = client(&[&format!("{query}\n/* route_denied */")]);
        assert!(
            forwarded.status.success(),
            "DBeaver discovery query {}: {}",
            index + 1,
            String::from_utf8_lossy(&forwarded.stderr)
        );
        let direct = maintenance(query, &database_names[0]);
        assert_eq!(
            forwarded.stdout,
            direct.stdout,
            "DBeaver discovery query {}",
            index + 1
        );
    }
    assert_eq!(callback_count.load(Ordering::SeqCst), before);
    let metabase: Vec<String> =
        serde_json::from_str(include_str!("../fixtures/metabase_discovery.json")).unwrap();
    for (index, query) in metabase.iter().enumerate() {
        let forwarded = client(&[&format!("{query}\n/* route_denied */")]);
        assert!(
            forwarded.status.success(),
            "Metabase discovery query {}: {}",
            index + 1,
            String::from_utf8_lossy(&forwarded.stderr)
        );
        let direct = maintenance(query, &database_names[0]);
        let sorted = |output: &Output| {
            let mut lines: Vec<_> = String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(str::to_owned)
                .collect();
            lines.sort();
            lines
        };
        assert_eq!(
            sorted(&forwarded),
            sorted(&direct),
            "Metabase discovery query {}",
            index + 1
        );
    }
    assert_eq!(callback_count.load(Ordering::SeqCst), before);
    let mixed_queries = [
        "SELECT u.id FROM pg_class c CROSS JOIN public.users u LIMIT 1 /* route_denied */",
        "SELECT id FROM public.users LIMIT 10000 /* route_denied */",
        "SELECT u.email FROM pg_extension e CROSS JOIN public.users u LIMIT 1 /* route_denied */",
        "SELECT word FROM pg_get_keywords() WHERE EXISTS (SELECT 1 FROM public.users) /* route_denied */",
        "SELECT current_schema(),session_user,(SELECT email FROM public.users LIMIT 1) /* route_denied */",
        "SELECT pg_read_file('/etc/passwd') FROM pg_extension /* route_denied */",
        "SELECT pg_stat_get_activity(NULL) FROM pg_extension /* route_denied */",
        "SELECT most_common_vals::text FROM pg_stats /* route_denied */",
        "WITH t AS (SELECT * FROM public.users) SELECT * FROM t JOIN pg_class ON true /* route_denied */",
    ];
    let mixed = client(&mixed_queries);
    assert!(String::from_utf8_lossy(&mixed.stderr).contains("42501"));
    assert_eq!(
        callback_count.load(Ordering::SeqCst),
        before + mixed_queries.len(),
        "mixed reads and samples use ordinary selection"
    );
}
