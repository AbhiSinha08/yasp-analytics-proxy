WITH tables AS (
    SELECT DISTINCT table_name,
                    table_schema,
                    column_name,
                    data_type,
                    ordinal_position AS pos
    FROM svv_columns
    WHERE table_schema NOT IN ('pg_internal','pg_catalog','information_schema')
    AND table_schema NOT LIKE 'pg_temp_%'
)
SELECT table_name, table_schema, column_name, data_type
FROM tables
WHERE
    HAS_SCHEMA_PRIVILEGE(table_schema, 'USAGE') AND
    (
        table_schema IN (SELECT schemaname FROM SVV_EXTERNAL_SCHEMAS) OR
        HAS_TABLE_PRIVILEGE('"' || table_schema || '"."' || table_name || '"', 'SELECT')
    )
ORDER BY table_name, pos
