//! Replay SQLAlchemy Redshift reflection used by Superset through the host.
#[path = "support/catalog_host.rs"]
mod catalog_host;
mod support;
use support::assert_success;

#[test]
fn redshift_superset_reflection_uses_discovery_and_preserves_source_facts() {
    catalog_host::with_catalog_host(None, |database, query| {
        database.connection.execute("CREATE TABLE public.purchases (id integer PRIMARY KEY, user_id integer REFERENCES public.users(id), amount numeric(30,9) DEFAULT 123.456789012); COMMENT ON COLUMN public.purchases.amount IS 'exact amount'; CREATE VIEW public.user_names AS SELECT id, full_name FROM public.users");
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/superset_redshift.json")).unwrap();
        for captured in fixture["queries"].as_array().unwrap() {
            let sql = captured["sql"].as_str().unwrap();
            let result = query(sql);
            assert_success(&result, captured["kind"].as_str().unwrap());
            let rows = String::from_utf8(result.stdout).unwrap();
            match captured["kind"].as_str().unwrap() {
                "relations" => assert!(
                    rows.contains("|public|") && rows.contains("|users|"),
                    "{rows}"
                ),
                "columns" => assert!(rows.contains("public|users|id||integer|"), "{rows}"),
                "constraints" => assert!(
                    rows.contains("public|users|p|users_pkey|{1}|1|id|PRIMARY KEY (id)|"),
                    "{rows}"
                ),
                _ => unreachable!(),
            }
            if captured["table"].is_null() {
                match captured["kind"].as_str().unwrap() {
                    "relations" => assert!(
                        rows.contains("|user_names|") && rows.contains("FROM users"),
                        "{rows}"
                    ),
                    "columns" => assert!(
                        rows.contains("numeric(30,9)")
                            && rows.contains("123.456789012")
                            && rows.contains("exact amount"),
                        "{rows}"
                    ),
                    "constraints" => assert!(
                        rows.contains("FOREIGN KEY (user_id) REFERENCES users(id)"),
                        "{rows}"
                    ),
                    _ => unreachable!(),
                }
            }
            if sql.contains("'public'") {
                let absent = query(&sql.replace("'public'", "'absent'"));
                assert_success(&absent, "absent schema");
                assert!(absent.stdout.is_empty());
                let injected =
                    query(&sql.replace("'public'", "'x''; SELECT full_name FROM public.users --'"));
                assert_success(&injected, "literal filter containing SQL");
                assert!(injected.stdout.is_empty());
            }
            for altered in [
                format!("{sql}; SELECT * FROM public.users"),
                sql.replace("pg_catalog.pg_class", "public.users"),
            ] {
                assert!(!query(&altered).status.success());
            }
        }
        assert!(!query("SELECT * FROM public.users").status.success());
    });
}
