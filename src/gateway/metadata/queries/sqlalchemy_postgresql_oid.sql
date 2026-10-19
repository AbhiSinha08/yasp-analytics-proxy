
            SELECT c.oid
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE (n.nspname = '__YASP_SCHEMA__')
            AND c.relname = '__YASP_TABLE__' AND c.relkind in
            ('r', 'v', 'm', 'f', 'p')
        
