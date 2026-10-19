
            SELECT
                pgd.description as table_comment
            FROM
                pg_catalog.pg_description pgd
            WHERE
                pgd.objsubid = 0 AND
                pgd.objoid = '__YASP_OID__'
        
