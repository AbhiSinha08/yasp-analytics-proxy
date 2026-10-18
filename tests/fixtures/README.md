# Database fixtures

The test harness creates a fresh database from PostgreSQL's `template0`, loads the
SQL fixture into it, and drops that database during teardown. The fixture contains
test-only synthetic records; it does not require a retained application database.

The configured test login needs `CREATEDB` permission. A non-superuser account is
sufficient. The harness only drops the database it created for that run and does
not alter PostgreSQL roles. Its target password is read from
`YASP_TEST_TARGET_PASSWORD`; frontend credentials come from
`YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD`, and `YASP_GATEWAY_DATABASE`.

`psycopg2_discovery.json` contains nine rendered startup and reflection queries
captured using SQLAlchemy 1.4.54 and psycopg2 2.9.13. It covers
hstore lookup, server/schema probes, and schema, table, column, domain, and enum
reflection. The gateway replay adds transaction controls, the isolation-level
probe, and ping. These fixtures cover that installed driver sequence.

`metabase_discovery.json` contains three reported Metabase schema-sync queries
for table privileges, tables with estimated row counts, and column metadata,
plus foreign-key and index queries rendered from its
[PostgreSQL driver](https://github.com/metabase/metabase/blob/master/src/metabase/driver/postgres.clj).

`postgres_metadata_variants.json` covers column/type metadata derived from
[PostgreSQL JDBC](https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/jdbc/PgDatabaseMetaData.java),
constraint/collation metadata derived from
[SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy/blob/main/lib/sqlalchemy/dialects/postgresql/base.py),
and PostgreSQL schema/version, privilege, and database-listing syntax variants.
Routing tests compare source responses; cache tests verify context, selection,
cache eligibility, and Redis bypass. These source-derived regressions do not
establish full compatibility with those clients.
