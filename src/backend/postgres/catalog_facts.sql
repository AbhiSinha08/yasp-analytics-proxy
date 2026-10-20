WITH visible AS MATERIALIZED (
    SELECT c.*, n.nspname, n.oid AS schema_oid
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN (__SCHEMAS__) AND c.relkind IN ('r','p','v','m','f')
    AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
    AND (pg_catalog.has_table_privilege(c.oid, 'SELECT') OR pg_catalog.has_any_column_privilege(c.oid, 'SELECT'))
), facts AS (
SELECT 'profile' AS kind, 0::oid AS oid, jsonb_build_object(
    'server_version', current_setting('server_version'), 'server_version_num', current_setting('server_version_num'), 'version', version(),
    'database', current_database(), 'current_schema', current_schema(), 'max_index_keys', current_setting('max_index_keys')) AS value
UNION ALL
SELECT 'relation', c.oid, jsonb_build_object(
    'oid', c.oid::bigint, 'schema_oid', c.schema_oid::bigint, 'database', current_database(), 'schema', c.nspname,
    'name', c.relname, 'kind', c.relkind::text, 'owner_id', c.relowner::bigint,
    'owner_name', pg_catalog.pg_get_userbyid(c.relowner), 'privileges', array_to_string(c.relacl, chr(10)),
    'privileges_update', pg_catalog.has_any_column_privilege(c.oid, 'UPDATE'),
    'privileges_select', pg_catalog.has_any_column_privilege(c.oid, 'SELECT'),
    'privileges_insert', pg_catalog.has_any_column_privilege(c.oid, 'INSERT'),
    'privileges_delete', pg_catalog.has_table_privilege(c.oid, 'DELETE'),
    'estimated_rows', (SELECT nullif(s.n_live_tup,0) FROM pg_catalog.pg_stat_user_tables s WHERE s.relid = c.oid),
    'view_definition', CASE WHEN c.relkind IN ('v','m') AND pg_catalog.has_table_privilege(c.oid, 'SELECT') THEN rtrim(pg_catalog.pg_get_viewdef(c.oid, true), ';') END)
FROM visible c
UNION ALL
SELECT 'column', c.oid, jsonb_build_object('name', a.attname, 'ordinal', a.attnum,
    'information_schema_type', (SELECT ic.data_type FROM information_schema.columns ic WHERE ic.table_schema = c.nspname AND ic.table_name = c.relname AND ic.column_name = a.attname),
    'formatted_type', pg_catalog.format_type(a.atttypid, a.atttypmod), 'not_null', a.attnotnull,
    'generated', a.attgenerated::text, 'length', a.attlen, 'type_modifier', t.typtypmod,
    'base_type', t.typbasetype::bigint, 'type_kind', t.typtype::text, 'identity', a.attidentity::text,
    'identity_options', (SELECT jsonb_build_object('always', a.attidentity = 'a', 'start', s.seqstart,
        'increment', s.seqincrement, 'minvalue', s.seqmin, 'maxvalue', s.seqmax, 'cache', s.seqcache, 'cycle', s.seqcycle)
        FROM pg_catalog.pg_sequence s WHERE a.attidentity <> '' AND
        s.seqrelid = pg_catalog.pg_get_serial_sequence(c.oid::regclass::text, a.attname)::regclass::oid))
FROM visible c JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
WHERE a.attnum > 0 AND NOT a.attisdropped AND pg_catalog.has_column_privilege(c.oid, a.attnum, 'SELECT')
UNION ALL
SELECT 'constraint', c.oid, jsonb_build_object('name', k.conname, 'kind', k.contype::text,
    'columns', ARRAY(SELECT a.attname FROM unnest(k.conkey) WITH ORDINALITY x(num, pos)
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = x.num ORDER BY pos),
    'ordinals', coalesce(k.conkey, '{}'::smallint[]), 'definition', pg_catalog.pg_get_constraintdef(k.oid, true),
    'foreign_schema', rn.nspname, 'foreign_table', rc.relname,
    'foreign_columns', ARRAY(SELECT a.attname FROM unnest(k.confkey) WITH ORDINALITY x(num, pos)
        JOIN pg_catalog.pg_attribute a ON a.attrelid = k.confrelid AND a.attnum = x.num ORDER BY pos),
    'foreign_constraint', (SELECT p.conname FROM pg_catalog.pg_constraint p
        WHERE p.conrelid = k.confrelid AND p.contype IN ('p','u') AND p.conindid = k.conindid LIMIT 1),
    'update_rule', k.confupdtype::text, 'delete_rule', k.confdeltype::text,
    'deferrable', k.condeferrable, 'deferred', k.condeferred)
FROM visible c JOIN pg_catalog.pg_constraint k ON k.conrelid = c.oid
LEFT JOIN pg_catalog.pg_class rc ON rc.oid = k.confrelid
LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
WHERE k.contype IN ('p','u','f','c')
AND NOT EXISTS (SELECT 1 FROM unnest(k.conkey) num WHERE NOT pg_catalog.has_column_privilege(c.oid, num, 'SELECT'))
AND (k.contype <> 'f' OR (rn.nspname IN (__SCHEMAS__)
    AND pg_catalog.has_schema_privilege(rn.oid, 'USAGE')
    AND NOT EXISTS (SELECT 1 FROM unnest(k.confkey) num WHERE NOT pg_catalog.has_column_privilege(k.confrelid, num, 'SELECT'))))
UNION ALL
SELECT 'index', c.oid, jsonb_build_object('name', i.relname, 'unique', ix.indisunique,
    'primary', ix.indisprimary, 'clustered', ix.indisclustered, 'method', am.amname,
    'expressions', ix.indexprs::text, 'keys', ix.indkey::text, 'options', ix.indoption::text,
    'relation_options', i.reloptions::text, 'predicate', pg_catalog.pg_get_expr(ix.indpred, ix.indrelid),
    'key_count', ix.indnkeyatts, 'constraint_relation', (SELECT k.conrelid::bigint FROM pg_catalog.pg_constraint k
        WHERE k.conindid = ix.indexrelid AND k.conrelid = c.oid AND k.contype IN ('p','u','x') LIMIT 1),
    'ordinal', x.pos, 'column_ordinal', a.attnum, 'column_name', a.attname,
    'definition', trim(both '"' from pg_catalog.pg_get_indexdef(ix.indexrelid, x.pos::int, false)),
    'direction', CASE WHEN am.amname = 'btree' THEN CASE WHEN ix.indoption[x.pos-1] & 1 = 1 THEN 'D' ELSE 'A' END END,
    'cardinality', i.reltuples::text, 'pages', i.relpages::text, 'comment', pg_catalog.obj_description(i.oid, 'pg_class'))
FROM visible c JOIN pg_catalog.pg_index ix ON ix.indrelid = c.oid
JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid JOIN pg_catalog.pg_am am ON am.oid = i.relam
CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY x(num, pos)
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = x.num
WHERE pg_catalog.has_table_privilege(c.oid, 'SELECT')
UNION ALL
SELECT 'type', t.oid, jsonb_build_object('name', t.typname, 'schema', n.nspname,
    'oid', t.oid::bigint, 'array_oid', t.typarray::bigint, 'kind', t.typtype::text,
    'base_type', pg_catalog.format_type(t.typbasetype, t.typtypmod),
    'nullable', NOT t.typnotnull, 'default', t.typdefault,
    'visible', pg_catalog.pg_type_is_visible(t.oid), 'label', e.enumlabel, 'label_order', e.oid::bigint)
FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
LEFT JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
WHERE n.nspname IN (__SCHEMAS__) AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
AND pg_catalog.has_type_privilege(t.oid, 'USAGE') AND (t.typtype IN ('d','e') OR t.typname = 'hstore')
)
SELECT kind, oid::text, value::text FROM facts ORDER BY CASE kind WHEN 'profile' THEN 0 WHEN 'relation' THEN 1 ELSE 2 END, kind, oid LIMIT __LIMIT__
