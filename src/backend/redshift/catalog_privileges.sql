SELECT current_database(), n.nspname, c.relname,
       CASE WHEN has_schema_privilege(quote_ident(n.nspname), 'USAGE') THEN 'YES' ELSE 'NO' END,
       CASE WHEN c.oid IS NULL THEN NULL
            WHEN NOT has_schema_privilege(quote_ident(n.nspname), 'USAGE') THEN 'NO'
            WHEN has_table_privilege(quote_ident(n.nspname) || '.' || quote_ident(c.relname), 'SELECT')
            THEN 'YES' ELSE 'NO' END
FROM pg_catalog.pg_namespace n
LEFT JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
WHERE n.nspname IN (__SCHEMAS__)
ORDER BY n.nspname, c.relname
LIMIT __LIMIT__
