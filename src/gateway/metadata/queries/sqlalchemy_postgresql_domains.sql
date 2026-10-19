
            SELECT t.typname as "name",
               pg_catalog.format_type(t.typbasetype, t.typtypmod) as "attype",
               not t.typnotnull as "nullable",
               t.typdefault as "default",
               pg_catalog.pg_type_is_visible(t.oid) as "visible",
               n.nspname as "schema"
            FROM pg_catalog.pg_type t
               LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typtype = 'd'
        
