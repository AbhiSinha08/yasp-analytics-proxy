SELECT tmp."table-schema", tmp."table-name",
trim(both '"' from pg_catalog.pg_get_indexdef(tmp.ci_oid, tmp.pos, false)) AS "field-name"
FROM (SELECT n.nspname AS "table-schema", ct.relname AS "table-name", ci.oid AS ci_oid,
(information_schema._pg_expandarray(i.indkey)).n AS pos
FROM pg_catalog.pg_class ct JOIN pg_catalog.pg_namespace n ON ct.relnamespace=n.oid
JOIN pg_catalog.pg_index i ON ct.oid=i.indrelid
JOIN pg_catalog.pg_class ci ON ci.oid=i.indexrelid
WHERE pg_catalog.pg_get_expr(i.indpred, i.indrelid) IS NULL
AND n.nspname !~ '^information_schema|catalog_history|pg_') tmp
WHERE tmp.pos=1 ORDER BY 1, 2, 3
