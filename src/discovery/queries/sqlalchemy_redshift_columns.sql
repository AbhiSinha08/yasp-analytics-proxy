SELECT
        n.nspname as "schema",
        c.relname as "table_name",
        att.attname as "name",
        format_encoding(att.attencodingtype::integer) as "encode",
        format_type(att.atttypid, att.atttypmod) as "type",
        att.attisdistkey as "distkey",
        att.attsortkeyord as "sortkey",
        att.attnotnull as "notnull",
        pg_catalog.col_description(att.attrelid, att.attnum)
        as "comment",
        adsrc,
        attnum,
        pg_catalog.format_type(att.atttypid, att.atttypmod),
        pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS DEFAULT,
        n.oid as "schema_oid",
        c.oid as "table_oid"
    FROM pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_namespace n
        ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute att
        ON att.attrelid = c.oid
    LEFT JOIN pg_catalog.pg_attrdef ad
        ON (att.attrelid, att.attnum) = (ad.adrelid, ad.adnum)
    WHERE n.nspname !~ '^pg_'
        AND att.attnum > 0
        AND NOT att.attisdropped
        AND schema = '__YASP_SCHEMA__' AND table_name = '__YASP_TABLE__'
    UNION
    SELECT
        view_schema as "schema",
        view_name as "table_name",
        col_name as "name",
        null as "encode",
        col_type as "type",
        null as "distkey",
        0 as "sortkey",
        null as "notnull",
        null as "comment",
        null as "adsrc",
        null as "attnum",
        col_type as "format_type",
        null as "default",
        null as "schema_oid",
        null as "table_oid"
    FROM pg_get_late_binding_view_cols() cols(
        view_schema name,
        view_name name,
        col_name name,
        col_type varchar,
        col_num int)
    WHERE 1 AND schema = '__YASP_SCHEMA__' AND table_name = '__YASP_TABLE__'
    UNION
    SELECT c.schemaname AS "schema",
        c.tablename AS "table_name",
        c.columnname AS "name",
        null AS "encode",
        -- Spectrum represents data types differently.
        -- Standardize, so we can infer types.
        CASE
            WHEN c.external_type = 'int' THEN 'integer'
            WHEN c.external_type = 'float' THEN 'real'
            WHEN c.external_type = 'double' THEN 'double precision'
            WHEN c.external_type = 'timestamp'
            THEN 'timestamp without time zone'
            WHEN c.external_type ilike 'varchar%'
            THEN replace(c.external_type, 'varchar', 'character varying')
            WHEN c.external_type ilike 'decimal%'
            THEN replace(c.external_type, 'decimal', 'numeric')
            ELSE
            replace(
            replace(
                replace(c.external_type, 'decimal', 'numeric'),
                'char', 'character'),
            'varchar', 'character varying')
            END
            AS "type",
        false AS "distkey",
        0 AS "sortkey",
        null AS "notnull",
        null as "comment",
        null AS "adsrc",
        c.columnnum AS "attnum",
        CASE
            WHEN c.external_type = 'int' THEN 'integer'
            WHEN c.external_type = 'float' THEN 'real'
            WHEN c.external_type = 'double' THEN 'double precision'
            WHEN c.external_type = 'timestamp'
            THEN 'timestamp without time zone'
            WHEN c.external_type ilike 'varchar%'
            THEN replace(c.external_type, 'varchar', 'character varying')
            WHEN c.external_type ilike 'decimal%'
            THEN replace(c.external_type, 'decimal', 'numeric')
            ELSE
            replace(
            replace(
                replace(c.external_type, 'decimal', 'numeric'),
                'char', 'character'),
            'varchar', 'character varying')
            END
            AS "format_type",
        null AS "default",
        s.esoid AS "schema_oid",
        null AS "table_oid"
    FROM svv_external_columns c
    JOIN svv_external_schemas s ON s.schemaname = c.schemaname
    WHERE 1 AND schema = '__YASP_SCHEMA__' AND table_name = '__YASP_TABLE__'
    ORDER BY "schema", "table_name", "attnum";
