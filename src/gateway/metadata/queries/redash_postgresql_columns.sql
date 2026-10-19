
        SELECT s.nspname AS table_schema,
               c.relname AS table_name,
               a.attname AS column_name,
               NULL AS data_type
        FROM pg_class c
        JOIN pg_namespace s
        ON c.relnamespace = s.oid
        AND s.nspname NOT IN ('pg_catalog', 'information_schema')
        JOIN pg_attribute a
        ON a.attrelid = c.oid
        AND a.attnum > 0
        AND NOT a.attisdropped
        WHERE c.relkind = 'm'
        AND has_table_privilege(quote_ident(s.nspname) || '.' || quote_ident(c.relname), 'select')
        AND has_schema_privilege(s.nspname, 'usage')

        UNION

        SELECT table_schema,
               table_name,
               column_name,
               data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        AND has_table_privilege(quote_ident(table_schema) || '.' || quote_ident(table_name), 'select')
        AND has_schema_privilege(table_schema, 'usage')
        
