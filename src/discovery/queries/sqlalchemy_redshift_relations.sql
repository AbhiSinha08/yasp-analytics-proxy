SELECT
          c.relkind,
          n.oid as "schema_oid",
          n.nspname as "schema",
          c.oid as "rel_oid",
          c.relname,
          CASE c.reldiststyle
            WHEN 0 THEN 'EVEN' WHEN 1 THEN 'KEY' WHEN 8 THEN 'ALL' END
            AS "diststyle",
          c.relowner AS "owner_id",
          u.usename AS "owner_name",
          TRIM(TRAILING ';' FROM pg_catalog.pg_get_viewdef(c.oid, true))
            AS "view_definition",
          pg_catalog.array_to_string(c.relacl, '
') AS "privileges"
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner
        WHERE c.relkind IN ('r', 'v', 'm', 'S', 'f')
          AND n.nspname !~ '^pg_' AND schema = '__YASP_SCHEMA__' AND relname = '__YASP_TABLE__'
        UNION
        SELECT
            'r' AS "relkind",
            s.esoid AS "schema_oid",
            s.schemaname AS "schema",
            null AS "rel_oid",
            t.tablename AS "relname",
            null AS "diststyle",
            s.esowner AS "owner_id",
            u.usename AS "owner_name",
            null AS "view_definition",
            null AS "privileges"
        FROM
            svv_external_tables t
            JOIN svv_external_schemas s ON s.schemaname = t.schemaname
            JOIN pg_catalog.pg_user u ON u.usesysid = s.esowner
        where 1 AND schema = '__YASP_SCHEMA__' AND relname = '__YASP_TABLE__'
        ORDER BY "relkind", "schema_oid", "schema";
