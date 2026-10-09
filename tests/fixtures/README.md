# Database fixtures

The test harness creates a fresh database from PostgreSQL's `template0`, loads the
SQL fixture into it, and drops that database during teardown. The fixture contains
test-only synthetic records; it does not require a retained application database.

The configured test login needs `CREATEDB` permission. A non-superuser account is
sufficient. The harness only drops the database it created for that run and does
not alter PostgreSQL roles. Its target password is read from
`YASP_TEST_TARGET_PASSWORD`; frontend credentials come from
`YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD`, and `YASP_GATEWAY_DATABASE`.
