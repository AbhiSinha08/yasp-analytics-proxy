SELECT
          n.nspname as "schema",
          c.relname as "table_name",
          t.contype,
          t.conname,
          t.conkey,
          a.attnum,
          a.attname,
          pg_catalog.pg_get_constraintdef(t.oid, true)::varchar(512) as condef,
          n.oid as "schema_oid",
          c.oid as "rel_oid"
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n
          ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_constraint t
          ON t.conrelid = c.oid
        JOIN pg_catalog.pg_attribute a
          ON t.conrelid = a.attrelid AND a.attnum = ANY(t.conkey)
        WHERE n.nspname !~ '^pg_' AND schema = '__YASP_SCHEMA__' AND table_name = '__YASP_TABLE__'
        UNION
        SELECT
            s.schemaname AS "schema",
            c.tablename AS "table_name",
            'p' as "contype",
            c.tablename || '_pkey' as "conname",
            array[1::SMALLINT] as "conkey",
            1 as "attnum",
            c.columnname as "attname",
            'PRIMARY KEY (' || c.columnname  || ')'::VARCHAR(512) as "condef",
            s.esoid AS "schema_oid",
            null AS "rel_oid"
        FROM
            svv_external_columns c
            JOIN svv_external_schemas s ON s.schemaname = c.schemaname
        where 1 AND schema = '__YASP_SCHEMA__' AND table_name = '__YASP_TABLE__'
        ORDER BY "schema", "table_name"
