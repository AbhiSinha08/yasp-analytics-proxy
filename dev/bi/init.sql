\set ON_ERROR_STOP on

SELECT format('CREATE DATABASE %I OWNER local', name)
FROM (VALUES ('metabase'), ('superset'), ('redash')) AS databases(name)
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = name)
\gexec
