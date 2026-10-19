
            SELECT t.typname as "name",
               -- no enum defaults in 8.4 at least
               -- t.typdefault as "default",
               pg_catalog.pg_type_is_visible(t.oid) as "visible",
               n.nspname as "schema",
               e.enumlabel as "label"
            FROM pg_catalog.pg_type t
                 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                 LEFT JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
            WHERE t.typtype = 'e'
        ORDER BY "schema", "name", e.oid
