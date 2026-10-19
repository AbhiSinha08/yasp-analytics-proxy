SELECT fk_ns.nspname AS "fk-table-schema", fk_table.relname AS "fk-table-name", fk_column.attname AS "fk-column-name",
pk_ns.nspname AS "pk-table-schema", pk_table.relname AS "pk-table-name", pk_column.attname AS "pk-column-name"
FROM pg_constraint c
JOIN pg_class fk_table ON c.conrelid=fk_table.oid
JOIN pg_namespace fk_ns ON c.connamespace=fk_ns.oid
JOIN pg_attribute fk_column ON c.conrelid=fk_column.attrelid
JOIN pg_class pk_table ON c.confrelid=pk_table.oid
JOIN pg_namespace pk_ns ON pk_table.relnamespace=pk_ns.oid
JOIN pg_attribute pk_column ON c.confrelid=pk_column.attrelid
WHERE fk_ns.nspname !~ '^information_schema|catalog_history|pg_' AND c.contype='f'::char
AND fk_column.attnum=ANY(c.conkey) AND pk_column.attnum=ANY(c.confkey)
ORDER BY "fk-table-schema", "fk-table-name" 
