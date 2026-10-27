SELECT current_database(), n.nspname, cl.relname, con.conname,
       CASE con.contype WHEN 'p' THEN 'PRIMARY KEY' WHEN 'u' THEN 'UNIQUE'
            WHEN 'f' THEN 'FOREIGN KEY' END,
       a.attname, CAST(pos.n AS varchar),
       CASE WHEN con.contype = 'f' THEN current_database() ELSE NULL END,
       pn.nspname, pc.relname, pa.attname, referenced_key.relname,
       CASE WHEN con.contype = 'f' THEN
         CASE con.confupdtype WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL'
              WHEN 'd' THEN 'SET DEFAULT' WHEN 'r' THEN 'RESTRICT'
              WHEN 'p' THEN 'RESTRICT' WHEN 'a' THEN 'NO ACTION' END ELSE NULL END,
       CASE WHEN con.contype = 'f' THEN
         CASE con.confdeltype WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL'
              WHEN 'd' THEN 'SET DEFAULT' WHEN 'r' THEN 'RESTRICT'
              WHEN 'p' THEN 'RESTRICT' WHEN 'a' THEN 'NO ACTION' END ELSE NULL END
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cl ON cl.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = cl.relnamespace
CROSS JOIN pg_catalog.generate_series(1, 32) pos(n)
JOIN pg_catalog.pg_attribute a ON a.attrelid = cl.oid AND a.attnum = con.conkey[(pos.n)]
LEFT JOIN pg_catalog.pg_class pc ON pc.oid = con.confrelid
LEFT JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
LEFT JOIN pg_catalog.pg_attribute pa ON pa.attrelid = pc.oid AND pa.attnum = con.confkey[(pos.n)]
LEFT JOIN (
  SELECT dep.objid, pkic.relname
  FROM pg_catalog.pg_depend dep
  JOIN pg_catalog.pg_class pkic ON pkic.oid = dep.refobjid AND pkic.relkind = 'i'
  WHERE dep.classid = 'pg_constraint'::regclass::oid
    AND dep.refclassid = 'pg_class'::regclass::oid
) referenced_key ON referenced_key.objid = con.oid AND con.contype = 'f'
WHERE n.nspname IN (__SCHEMAS__) AND con.contype IN ('p', 'u', 'f')
ORDER BY n.nspname, cl.relname, con.conname, pos.n
LIMIT __LIMIT__
