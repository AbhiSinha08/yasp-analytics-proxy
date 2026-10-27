use super::*;

#[test]
fn redshift_super_and_partiql_keep_native_sql_and_read_only_guards() {
    for sql in [
        "SELECT JSON_SERIALIZE(JSON_PARSE('{\"n\":12345678901234567890.123456789}'))",
        "SELECT u.profile.customer.name, u.profile['items'][0].sku FROM users u",
        "SELECT item.sku, idx FROM users u, u.profile.items AS item AT idx",
        "SELECT child.sku FROM users u, u.profile.orders AS parent, parent.items AS child",
        "SELECT attr, val.name FROM users u, UNPIVOT u.profile.objects AS val AT attr",
        "WITH c AS (SELECT profile AS doc FROM users) SELECT item.sku FROM c, c.doc.items AS item",
        "SELECT item.sku FROM users u, UNNEST(u.profile.items) AS expanded(item)",
    ] {
        let parsed = ParsedQuery::parse_with_dialect(sql, 1_048_576, SqlDialect::Redshift).unwrap();
        parsed.validate_read_only().unwrap();
        assert_eq!(parsed.sql(), sql);
    }
    for sql in [
        "SELECT * INTO copied FROM users u, u.profile.items AS item",
        "WITH hidden AS (DELETE FROM users RETURNING profile) SELECT item FROM hidden h, h.profile.items AS item",
        "SELECT SET_CONFIG('statement_timeout', '0', false) FROM users u, u.profile.items AS item",
        "SELECT item FROM users u, u.profile.items AS item; DELETE FROM users",
    ] {
        assert!(
            !ParsedQuery::parse_with_dialect(sql, 1_048_576, SqlDialect::Redshift)
                .is_ok_and(|query| query.validate_read_only().is_ok()),
            "{sql}"
        );
    }
}

#[test]
fn inspect_the_preserved_tree_including_nested_writes() {
    for sql in [
        "/* context */ WITH r AS (SELECT 1 AS n) SELECT n FROM r",
        "SELECT '; DELETE' AS value UNION ALL SELECT 'safe'",
        "SHOW ALL",
        "SHOW TIME ZONE",
        "SELECT pg_catalog.set_config('application_name', 'label', false)",
        "SELECT u & \"column\" FROM bits",
    ] {
        let parsed = ParsedQuery::parse(sql).unwrap();
        parsed.validate_read_only().unwrap();
        assert_eq!(parsed.sql(), sql);
    }
    for sql in [
        "WITH r AS (DELETE FROM users RETURNING *) SELECT * FROM r",
        "WITH r AS (UPDATE users SET email = 'x' RETURNING *) SELECT * FROM r",
        "SELECT * FROM (SELECT * INTO stolen FROM users) s",
        "SELECT * FROM users FOR UPDATE",
        "SET ROLE local",
        "BEGIN",
        "SELECT set_config('DateStyle', 'SQL, DMY', false)",
        "SELECT pg_catalog.\"set_config\"('client_encoding', 'LATIN1', false)",
        "SELECT SET_CONFIG(lower('application_name'), 'label', false)",
        r#"SELECT U&"set\005fconfig"('DateStyle', 'SQL, DMY', false)"#,
    ] {
        assert!(
            !ParsedQuery::parse(sql).is_ok_and(|q| q.validate_read_only().is_ok()),
            "{sql}"
        );
    }
    assert!(matches!(
        ParsedQuery::parse("SELECT 1; SELECT 2"),
        Err(QueryError::MultipleStatements)
    ));
}

#[test]
fn query_id_normalizes_only_safe_formatting() {
    let id = |sql| ParsedQuery::parse(sql).unwrap().query_id().to_owned();
    assert_eq!(
        id("SELECT a FROM t;"),
        id(" select  a\nfrom t ; -- comment\n")
    );
    assert_eq!(id("SELECT a FROM t"), id("SELECT a /* comment */ FROM t"));
    assert_eq!(
        id("SELECT '雪' /* note */ FROM t;"),
        id(" select '雪'\r\n/* 雪 */ from t ;")
    );
    assert_ne!(id("SELECT 'A'"), id("SELECT 'a'"));
    assert_ne!(id("SELECT 'é'"), id("SELECT 'É'"));
    assert_ne!(id("SELECT \"Column\""), id("SELECT \"column\""));
    assert_ne!(id(r"SELECT E'it\'s'"), id("SELECT E'it''s'"));
    assert_ne!(id("SELECT ab"), id("SELECT a b"));
    let hash = |sql| {
        let dialect = PostgreSqlDialect {};
        let tokens = Tokenizer::new(&dialect, sql)
            .tokenize_with_location()
            .unwrap();
        query_id(sql, &tokens)
    };
    assert_ne!(hash("SELECT 'a'\n'b'"), hash("SELECT 'a' 'b'"));
    assert_ne!(hash("SELECT 'a'\r\n'b'"), hash("SELECT 'a' 'b'"));
    assert!(
        id("SELECT 1")
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    );
    assert_eq!(id("SELECT 1").len(), 64);
}

#[test]
fn reject_flat_trees_before_they_can_overflow_on_inspection_or_drop() {
    for sql in [
        format!("SELECT {}", vec!["1"; 100_000].join("+")),
        vec!["SELECT 1"; 50_000].join(" UNION ALL "),
    ] {
        assert!(matches!(
            ParsedQuery::parse(&sql),
            Err(QueryError::TooComplex)
        ));
    }
    let sql = format!("SELECT {}", vec!["1"; 2048].join("+"));
    let query = ParsedQuery::parse(&sql).unwrap();
    query.validate_read_only().unwrap();
    drop(query);
    assert!(
        ParsedQuery::parse(&format!("SELECT {}1{}", "(".repeat(100), ")".repeat(100))).is_err()
    );
}
