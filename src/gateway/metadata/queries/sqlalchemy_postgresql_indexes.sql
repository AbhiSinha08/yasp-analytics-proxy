
              SELECT
                  i.relname as relname,
                  ix.indisunique, ix.indexprs,
                  a.attname, a.attnum, c.conrelid, ix.indkey::varchar,
                  ix.indoption::varchar, i.reloptions, am.amname,
                  pg_get_expr(ix.indpred, ix.indrelid),
                  ix.indnkeyatts as indnkeyatts
              FROM
                  pg_class t
                        join pg_index ix on t.oid = ix.indrelid
                        join pg_class i on i.oid = ix.indexrelid
                        left outer join
                            pg_attribute a
                            on t.oid = a.attrelid and a.attnum = ANY(ix.indkey)
                        left outer join
                            pg_constraint c
                            on (ix.indrelid = c.conrelid and
                                ix.indexrelid = c.conindid and
                                c.contype in ('p', 'u', 'x'))
                        left outer join
                            pg_am am
                            on i.relam = am.oid
              WHERE
                  t.relkind IN ('r', 'v', 'f', 'm', 'p')
                  and t.oid = '__YASP_OID__'
                  and ix.indisprimary = 'f'
              ORDER BY
                  t.relname,
                  i.relname
            
