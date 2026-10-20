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
        "SHOW server_version",
        "SELECT current_catalog",
    ]);
    assert!(
        metadata.status.success(),
        "{}",
        String::from_utf8_lossy(&metadata.stderr)
    );
    assert!(String::from_utf8_lossy(&metadata.stdout).contains(&database_names[0]));
    let user_oid = maintenance("SELECT 'public.users'::regclass::oid", &database_names[0]);
    let user_oid = String::from_utf8_lossy(&user_oid.stdout).trim().to_owned();
    for fixture in [
        include_str!("../fixtures/psql_discovery.json"),
        include_str!("../fixtures/dbeaver_discovery.json"),
        include_str!("../fixtures/postgres_metadata_variants.json"),
    ] {
        let queries: Vec<String> = serde_json::from_str(fixture).unwrap();
        for query in queries {
            let query = query.replace("'25009'", &format!("'{user_oid}'"));
            let result = client(&[&format!("{query}\n/* route_denied */")]);
            if !result.status.success() {
                assert!(
                    String::from_utf8_lossy(&result.stderr).contains("0A000"),
                    "{query}: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
            }
        }
    }
    let metabase: Vec<String> =
        serde_json::from_str(include_str!("../fixtures/metabase_discovery.json")).unwrap();
    for query in metabase {
        let result = client(&[&format!("{query}\n/* route_denied */")]);
        assert!(
            result.status.success(),
            "{query}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert_eq!(
        callback_count.load(Ordering::SeqCst),
        before,
        "captured and unsupported discovery bypass selection"
    );
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
        "SELECT version() UNION ALL TABLE public.users /* route_denied */",
        "WITH hidden AS (TABLE public.users) SELECT * FROM hidden CROSS JOIN pg_class /* route_denied */",
        "SELECT * FROM pg_class WHERE EXISTS (TABLE public.users) /* route_denied */",
        "SELECT * FROM pg_class CROSS JOIN current_setting('role') s /* route_denied */",
    ];
    let mixed = client(&mixed_queries);
    assert!(String::from_utf8_lossy(&mixed.stderr).contains("42501"));
    assert_eq!(
        callback_count.load(Ordering::SeqCst),
        before + mixed_queries.len(),
        "mixed reads and samples use ordinary selection"
    );
}
