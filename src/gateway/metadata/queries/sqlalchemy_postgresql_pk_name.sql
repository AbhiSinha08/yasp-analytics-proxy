
        SELECT conname
           FROM  pg_catalog.pg_constraint r
           WHERE r.conrelid = '__YASP_OID__' AND r.contype = 'p'
           ORDER BY 1
        
