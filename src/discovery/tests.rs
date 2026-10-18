use super::*;
use crate::query::ParsedQuery;
#[test]
fn psql_discovery_queries_use_the_catalog_route() {
    let queries: Vec<String> =
        serde_json::from_str(include_str!("../../tests/fixtures/psql_discovery.json")).unwrap();
    let missed: Vec<_> = queries
        .iter()
        .enumerate()
        .filter(|(_, sql)| {
            classify(&ParsedQuery::parse(sql).unwrap()) != Some(DiscoveryKind::Catalog)
        })
        .map(|(i, _)| i + 1)
        .collect();
    assert!(
        missed.is_empty(),
        "psql discovery queries missed: {missed:?}"
    );
}

#[test]
fn metabase_schema_sync_uses_discovery_without_application_access() {
    let queries: Vec<String> =
        serde_json::from_str(include_str!("../../tests/fixtures/metabase_discovery.json")).unwrap();
    for sql in queries.iter().map(String::as_str).chain([
        "WITH a AS (SELECT relname FROM pg_class), b AS (SELECT * FROM a) SELECT * FROM b",
        "WITH \"Meta\" AS (SELECT relname FROM pg_class) SELECT * FROM \"Meta\"",
        "WITH a AS (SELECT relname FROM pg_class) SELECT * FROM (WITH b AS (SELECT * FROM a) SELECT * FROM b) s",
    ]) {
        assert_eq!(classify(&ParsedQuery::parse(sql).unwrap()), Some(DiscoveryKind::Catalog), "{sql}");
    }
    for sql in [
        "WITH t AS (SELECT * FROM users) SELECT * FROM t JOIN pg_class ON true",
        "WITH t AS (SELECT * FROM pg_class) SELECT * FROM t JOIN users ON true",
        "WITH t AS (SELECT * FROM pg_class), unused AS (SELECT * FROM users) SELECT * FROM t",
        "WITH t AS (SELECT * FROM pg_class) SELECT public.secret() FROM t",
        "WITH t AS (SELECT * FROM users), users AS (SELECT * FROM pg_class) SELECT * FROM t",
        "WITH users AS (SELECT * FROM users UNION SELECT * FROM pg_class) SELECT * FROM users",
        "SELECT * FROM (WITH users AS (SELECT * FROM pg_class) SELECT * FROM users) s JOIN users ON true",
        "WITH \"Meta\" AS (SELECT * FROM pg_class) SELECT * FROM meta",
        "WITH t AS (SELECT * FROM pg_class) SELECT * FROM public.t",
        "WITH pg_class AS (SELECT * FROM pg_namespace) SELECT * FROM pg_class",
        "WITH RECURSIVE t AS (SELECT * FROM pg_class) SELECT * FROM t",
        "WITH t AS (DELETE FROM users RETURNING *) SELECT * FROM t",
        "SELECT format('%s', (SELECT email FROM users)) FROM pg_class",
        "SELECT public.format('%s', relname) FROM pg_class",
        "SELECT * FROM public.pg_stat_user_tables",
        "SELECT * FROM pg_stat_user_tables WHERE EXISTS (SELECT 1 FROM users)",
        "SELECT * FROM pg_stats",
    ] {
        assert!(
            classify(&ParsedQuery::parse(sql).unwrap()).is_none(),
            "{sql}"
        );
    }
}

#[test]
fn dbeaver_discovery_queries_bypass_ordinary_routing() {
    let queries: Vec<String> =
        serde_json::from_str(include_str!("../../tests/fixtures/dbeaver_discovery.json")).unwrap();
    let missed: Vec<_> = queries
        .iter()
        .enumerate()
        .filter(|(_, sql)| classify(&ParsedQuery::parse(sql).unwrap()).is_none())
        .map(|(i, _)| i + 1)
        .collect();
    assert!(
        missed.is_empty(),
        "DBeaver discovery queries missed: {missed:?}"
    );
}

#[test]
fn psycopg2_driver_discovery_stays_on_its_configured_source() {
    let queries: Vec<String> =
        serde_json::from_str(include_str!("../../tests/fixtures/psycopg2_discovery.json")).unwrap();
    for sql in queries {
        assert!(
            classify(&ParsedQuery::parse(&sql).unwrap()).is_some(),
            "{sql}"
        );
    }
    for sql in [
        "SELECT json_build_object('value', (SELECT email FROM users)) FROM pg_class",
        "SELECT public.json_build_object('value', relname) FROM pg_class",
        "SELECT json_build_object('value', public.secret()) FROM pg_class",
    ] {
        assert!(
            classify(&ParsedQuery::parse(sql).unwrap()).is_none(),
            "{sql}"
        );
    }
}

#[test]
fn discovery_classification_does_not_grant_application_access() {
    for sql in [
        "SELECT datname FROM pg_database WHERE datistemplate=false ORDER BY datname",
        "SELECT nspname FROM pg_catalog.pg_namespace",
        "SELECT c.column_name, a.atttypid::regtype FROM information_schema.columns c JOIN pg_attribute a ON a.attname=c.column_name",
        "SELECT pg_catalog.format_type(a.atttypid,a.atttypmod), pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid=d.adrelid",
        "SELECT c.relname FROM pg_class c WHERE EXISTS(SELECT 1 FROM pg_namespace n WHERE n.oid=c.relnamespace)",
        "SELECT * FROM pg_constraint c LEFT JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(n,pos) ON true",
        "SHOW server_version",
        "SELECT relname FROM pg_class WHERE has_table_privilege(current_user,oid,'SELECT')",
        "SELECT rolname FROM pg_roles WHERE rolname=session_user",
        "SELECT version()",
        "SELECT current_setting('server_version_num')",
        "SELECT * FROM information_schema.sql_features",
        "SELECT * FROM information_schema.view_table_usage v JOIN information_schema.constraint_table_usage c ON c.table_name=v.table_name",
        "SELECT * FROM information_schema.columns c WHERE EXISTS (SELECT 1 FROM information_schema.column_domain_usage d WHERE d.column_name=c.column_name)",
        "SELECT current_schema() AS schema, session_user AS login",
        "SELECT current_database(),current_schema(),current_user,session_user",
        "SELECT version(),session_user",
        "SELECT word FROM pg_get_keywords() WHERE catcode='R' ORDER BY word",
        "SELECT array_agg(k.word) FROM (SELECT word FROM pg_catalog.pg_get_keywords()) k",
        "SELECT pg_get_partkeydef(c.oid),pg_get_partition_constraintdef(c.oid) FROM pg_class c",
        "SELECT pg_get_ruledef(r.oid) FROM pg_rewrite r",
        "SELECT pg_get_serial_sequence('public.users','id')",
        "SELECT c.opcname,f.opfname FROM pg_opclass c JOIN pg_opfamily f ON f.oid=c.opcfamily",
        "SELECT o.oprname,a.amopstrategy FROM pg_operator o JOIN pg_amop a ON a.amopopr=o.oid",
        "SELECT p.amproc FROM pg_amproc p",
        "SELECT c.castsource,r.rngtypid FROM pg_cast c JOIN pg_range r ON r.rngsubtype=c.castsource",
        "SELECT p.proname,l.lanname FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang JOIN pg_aggregate a ON a.aggfnoid=p.oid",
    ] {
        assert!(
            classify(&ParsedQuery::parse(sql).unwrap()).is_some(),
            "{sql}"
        );
    }
    for sql in [
        "SELECT * FROM public.users LIMIT 10000",
        "SELECT * FROM users LIMIT 0",
        "SELECT c.relname,u.email FROM pg_class c CROSS JOIN public.users u",
        "SELECT * FROM pg_class WHERE EXISTS(SELECT 1 FROM public.users)",
        "SELECT public.secret() FROM pg_class",
        "SELECT \"current_user\"() FROM pg_class",
        "SELECT pg_sleep(1) FROM pg_class",
        "SELECT set_config('application_name','x',false) FROM pg_class",
        "SELECT 'x'::public.evil FROM pg_class",
        "SELECT 'x'::evil[] FROM pg_class",
        "SELECT * FROM public.pg_class",
        "SELECT * FROM \"PG_CLASS\"",
        "SELECT 'pg_catalog.pg_class'",
        "WITH pg_class AS (SELECT * FROM users) SELECT * FROM pg_class",
        "SELECT current_setting('role')",
        "SELECT pg_terminate_backend(1) FROM pg_class",
        "SELECT current_database() || ':' || current_user",
        "SELECT * FROM pg_class UNION SELECT * FROM users",
        "SELECT relname FROM pg_class WHERE relname OPERATOR(public.~) 'users'",
        "SELECT relname OPERATOR(pg_catalog.||) 'users' FROM pg_class",
        "SELECT relname FROM pg_class WHERE relname OPERATOR(~) 'users'",
        "SELECT pg_get_userbyid(c.relowner), u.email FROM pg_class c JOIN public.users u ON true",
        "SELECT pg_table_size(c.oid) FROM pg_class c WHERE EXISTS (SELECT 1 FROM public.users)",
        "SELECT * FROM pg_policy WHERE EXISTS (SELECT 1 FROM public.users)",
        "SELECT * FROM pg_statistic_ext_data",
        "SELECT * FROM pg_statistic",
        "SELECT * FROM pg_stats",
        "SELECT * FROM pg_publication p JOIN public.users u ON true",
        "SELECT * FROM pg_trigger t, public.pg_partition_ancestors(t.tgrelid) a",
        "SELECT * FROM pg_trigger t, pg_catalog.pg_partition_ancestors(public.secret()) a",
        "SELECT ARRAY(SELECT email FROM public.users) FROM pg_policy",
        "SELECT ARRAY(SELECT public.secret() FROM pg_roles) FROM pg_policy",
        "SELECT public.\"array\"(rolname) FROM pg_roles",
        "SELECT * FROM information_schema.sql_features f JOIN public.users u ON true",
        "SELECT * FROM information_schema.view_table_usage WHERE EXISTS (SELECT 1 FROM public.users)",
        "SELECT public.secret() FROM information_schema.sql_features",
        "SELECT current_user",
        "SELECT session_user",
        "SELECT current_schema(), (SELECT email FROM public.users LIMIT 1), session_user",
        "SELECT word,email FROM pg_catalog.pg_get_keywords() k CROSS JOIN public.users",
        "SELECT pg_get_keywords(), ARRAY(SELECT email FROM public.users)",
        "SELECT public.pg_get_keywords()",
        "SELECT * FROM public.pg_extension",
        "SELECT pg_get_partkeydef(public.secret()) FROM pg_class",
        "SELECT pg_stat_get_activity(NULL) FROM pg_extension",
        "SELECT pg_read_file('/etc/passwd') FROM pg_extension",
        "SELECT pg_read_binary_file('/etc/passwd') FROM pg_opclass",
        "SELECT lo_get(1234) FROM pg_extension",
        "SELECT currval(pg_get_serial_sequence('public.users','id'))",
        "SELECT query FROM pg_stat_activity",
        "SELECT * FROM pg_largeobject",
        "SELECT * FROM pg_authid",
    ] {
        let query = ParsedQuery::parse(sql)
            .unwrap_or_else(|error| panic!("cannot parse regression SQL {sql}: {error}"));
        assert!(classify(&query).is_none(), "{sql}");
    }
}
