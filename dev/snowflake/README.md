# Snowflake SQL API development environment

This runs [nnnkkk7/snowflake-emulator](https://github.com/nnnkkk7/snowflake-emulator)
with DuckDB storage behind the public `/api/v2/statements` API. The Dockerfile
pins upstream commit `3ced3e8b0f2b69f3ff2c6141784f4faf9666ec11`, verifies its archive
checksum, and pins the Go and Python base images. No native driver or driver SHA
is required by YASP.

Run from the repository root:

```sh
docker compose -f dev/snowflake/compose.yml up --build --wait
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out dev/snowflake/local-key.p8
chmod 600 dev/snowflake/local-key.p8
cargo run --features snowflake -- --config config/snowflake-demo.yml
```

Generate the key once; subsequent starts reuse it. The ignored local key is a
fixture credential. The emulator ignores authentication, but YASP still submits
signed key-pair JWTs through the same code path used for real Snowflake. The host
reads the normal `.env` gateway login/database and TLS configuration. There is no
Snowflake password. See [gateway client settings](../../docs/configuration.md#jdbc-clients)
for TLS trust and simple-query settings; the demo uses port **6433**, the Redshift
profile, and the gateway database label (normally `development`). The source
`YASP_DEMO` database name is independent of that gateway label. Set
`gateway.protocol: postgresql` to exercise the other profile.

HTTP is published only on `127.0.0.1:64616`. Databases and staging files live in
the Compose-owned `sql-api-data` volume at `/data`. This volume is separate from
any previous emulator's volume. Starting this setup does not remove existing data.

The first startup seeds `YASP_DEMO.PUBLIC` and `YASP_DEMO.ANALYTICS` through the SQL
API. A seed version is written last. Restarts retain user-added data. Definitions
include customers, products, orders, support tickets, a sales view, exact decimals,
NULLs, binary and temporal values, comments, quoted columns, and declared keys.
The composite-key relationship uses two tables in `ANALYTICS`.

Load additional SQL through the same network interface:

```sh
docker compose -f dev/snowflake/compose.yml exec -T snowflake python sql.py < extra.sql
docker compose -f dev/snowflake/compose.yml exec snowflake python sql.py --health
docker compose -f dev/snowflake/compose.yml down
```

To explicitly discard this environment's data:

```sh
docker compose -f dev/snowflake/compose.yml down --volumes
```

## Emulator corrections and limits

`patch_emulator.py` and `emulator-compat.go` apply fixture-only corrections during
the Docker build. The pinned upstream version ignores the requested schema,
serializes numeric/boolean cells as JSON scalars, uses incompatible temporal/binary
formats, misclassifies leading comments/CTEs, and lacks Snowflake's catalog columns and
`SHOW … KEYS`. Its MySQL SQL printer can also corrupt quoted identifiers. The
corrections preserve supported SQL and expose actual DuckDB definitions, comments,
views and declared constraints through Snowflake-shaped metadata. No YASP adapter
contains emulator SQL rewrites or seeded table definitions.

The fixture serializes requests to keep its shared DuckDB schema context isolated.
It executes synchronously and buffers results server-side; it does not validate
real Snowflake concurrency, asynchronous execution, result partitioning, grants,
warehouse isolation, or JWT enforcement. Those HTTP contracts have separate focused
adapter tests. DuckDB discards VARCHAR length limits, so the emulator honestly
returns unknown lengths. Exact declarations and authentication still need real
Snowflake acceptance. This environment does not establish complete Snowflake SQL
semantics.

`python3 scripts/test.py --full` uses unique disposable Docker containers/volumes,
creates temporary signing keys, and runs the normal functional/client test matrix.
It never touches this persistent development volume. Persistence tests recreate a
container and compare data plus metadata through HTTP.
