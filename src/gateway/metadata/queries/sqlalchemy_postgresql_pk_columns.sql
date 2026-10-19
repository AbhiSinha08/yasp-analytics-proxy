
                SELECT a.attname
                FROM pg_attribute a JOIN (
                    SELECT unnest(ix.indkey) attnum,
                           generate_subscripts(ix.indkey, 1) ord
                    FROM pg_index ix
                    WHERE ix.indrelid = '__YASP_OID__' AND ix.indisprimary
                    ) k ON a.attnum=k.attnum
                WHERE a.attrelid = '__YASP_OID__'
                ORDER BY k.ord
            
