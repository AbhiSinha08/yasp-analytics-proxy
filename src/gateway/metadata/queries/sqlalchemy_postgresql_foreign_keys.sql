
          SELECT r.conname,
                pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
                n.nspname as conschema
          FROM  pg_catalog.pg_constraint r,
                pg_namespace n,
                pg_class c

          WHERE r.conrelid = '__YASP_OID__' AND
                r.contype = 'f' AND
                c.oid = confrelid AND
                n.oid = c.relnamespace
          ORDER BY 1
        
