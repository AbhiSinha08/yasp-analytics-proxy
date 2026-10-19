SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '__YASP_SCHEMA__' AND c.relkind in ('r', 'p')
