SELECT current_database(), n.nspname, t.table_name,
       CASE t.table_type WHEN 'BASE TABLE' THEN 'TABLE' ELSE t.table_type END,
       COALESCE(t.remarks, rd.description), c.column_name,
       CAST(c.ordinal_position AS varchar),
       CASE WHEN ty.typname IN ('super', 'varbyte') THEN ty.typname
            WHEN c.data_type = 'USER-DEFINED' THEN ty.typname ELSE c.data_type END,
       c.is_nullable, c.column_default, COALESCE(c.remarks, cd.description),
       CAST(c.character_maximum_length AS varchar), CAST(c.numeric_precision AS varchar),
       CAST(c.numeric_scale AS varchar), NULL, NULL, NULL, NULL, NULL,
       CAST(cl.oid AS varchar), NULL, NULL
FROM pg_catalog.pg_namespace n
LEFT JOIN svv_tables t ON t.table_catalog = current_database() AND t.table_schema = n.nspname
LEFT JOIN svv_columns c ON c.table_catalog = t.table_catalog
     AND c.table_schema = t.table_schema AND c.table_name = t.table_name
LEFT JOIN pg_catalog.pg_class cl ON cl.relnamespace = n.oid AND cl.relname = t.table_name
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = cl.oid AND a.attname = c.column_name
     AND a.attnum > 0 AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_type ty ON ty.oid = a.atttypid
LEFT JOIN pg_catalog.pg_description rd ON rd.objoid = cl.oid AND rd.objsubid = 0
LEFT JOIN pg_catalog.pg_description cd ON cd.objoid = cl.oid AND cd.objsubid = a.attnum
WHERE n.nspname IN (__SCHEMAS__)
ORDER BY n.nspname, t.table_name, c.ordinal_position
LIMIT __LIMIT__
