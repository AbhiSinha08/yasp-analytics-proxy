//! PostgreSQL facts in the result shapes expected by SQLAlchemy's Redshift driver.
use crate::{
    discovery::redshift::{Kind, Request},
    query::{ParsedQuery, QueryError},
};

pub(super) fn prepare(request: &Request) -> Result<ParsedQuery, QueryError> {
    let (select, joins, order) = match request.kind {
        Kind::Relations => (
            r#"CASE WHEN c.relkind = 'p' THEN 'r' ELSE c.relkind::text END AS relkind,
            n.oid AS schema_oid, n.nspname AS schema, c.oid AS rel_oid, c.relname,
            NULL::text AS diststyle, c.relowner AS owner_id, u.rolname AS owner_name,
            CASE WHEN c.relkind IN ('v', 'm') THEN rtrim(pg_catalog.pg_get_viewdef(c.oid, true), ';') END AS view_definition,
            pg_catalog.array_to_string(c.relacl, chr(10)) AS privileges"#,
            "JOIN pg_catalog.pg_roles u ON u.oid = c.relowner",
            "relkind, schema_oid, schema",
        ),
        Kind::Columns => (
            r#"n.nspname AS schema, c.relname AS table_name, att.attname AS name,
            NULL::text AS encode, pg_catalog.format_type(att.atttypid, att.atttypmod) AS type,
            NULL::boolean AS distkey, NULL::integer AS sortkey, att.attnotnull AS "notnull",
            pg_catalog.col_description(c.oid, att.attnum) AS comment,
            pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS adsrc, att.attnum,
            pg_catalog.format_type(att.atttypid, att.atttypmod) AS format_type,
            pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS "default",
            n.oid AS schema_oid, c.oid AS table_oid"#,
            "JOIN pg_catalog.pg_attribute att ON att.attrelid = c.oid AND att.attnum > 0 AND NOT att.attisdropped
            AND pg_catalog.has_column_privilege(c.oid, att.attnum, 'SELECT')
            LEFT JOIN pg_catalog.pg_attrdef ad ON (ad.adrelid, ad.adnum) = (att.attrelid, att.attnum)",
            "schema, table_name, attnum",
        ),
        Kind::Constraints => (
            r#"n.nspname AS schema, c.relname AS table_name, t.contype, t.conname, t.conkey,
            a.attnum, a.attname, pg_catalog.pg_get_constraintdef(t.oid, true)::varchar(512) AS condef,
            n.oid AS schema_oid, c.oid AS rel_oid"#,
            "JOIN pg_catalog.pg_constraint t ON t.conrelid = c.oid
            JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(t.conkey)
            AND pg_catalog.has_column_privilege(c.oid, a.attnum, 'SELECT')",
            "schema, table_name",
        ),
    };
    let mut sql = format!(
        "SELECT {select} FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace {joins}
        WHERE c.relkind IN ('r','p','v','m','f') AND n.nspname !~ '^pg_'
        AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
        AND (pg_catalog.has_table_privilege(c.oid, 'SELECT') OR pg_catalog.has_any_column_privilege(c.oid, 'SELECT'))"
    );
    for (column, value) in [
        ("n.nspname", &request.schema),
        ("c.relname", &request.table),
    ] {
        if let Some(value) = value {
            sql.push_str(&format!(" AND {column} = '{}'", value.replace('\'', "''")));
        }
    }
    sql.push_str(&format!(" ORDER BY {order}"));
    ParsedQuery::parse(&sql)
}
