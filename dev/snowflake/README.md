# Local Snowflake backend

This environment runs Fakesnow 0.11.22 over HTTP with DuckDB storage. It exercises
YASP's Snowflake ADBC connection, not a direct DuckDB backend. Docker Desktop must
be available from WSL. The native YASP driver currently supports Linux x86-64/WSL.

## Start and connect

From the repository root:

```sh
docker compose -f dev/snowflake/compose.yml up --build --wait
python3 scripts/snowflake/build_native_driver.py
export YASP_SNOWFLAKE_DRIVER_SHA256=$(python3 -c 'import json; print(json.load(open("target/compatibility/native-build/build.json"))["library_sha256"])')
export YASP_SNOWFLAKE_PASSWORD=snow
cargo run --locked --features snowflake -- --config config/snowflake-demo.yml
```

The native build needs a C/C++ compiler and `patch`; its pinned Go toolchain,
source archives, checksums, license files and output stay under `target/`.
The demo uses the normal `YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD` and
`YASP_GATEWAY_DATABASE` variables (or `.env`) and the existing local TLS certificates
under `config/local-tls`. Follow [TLS setup](../../docs/tls.md) if they are absent.

Connect BI clients to YASP on port **6433**, using the gateway identity and database
label. Containers use `host.docker.internal`. Trust the gateway CA as described in
[JDBC/client configuration](../../docs/configuration.md#jdbc-clients).

With the usual local `.env`, enter the following in Metabase:

| Metabase field | Value |
| --- | --- |
| Database type | Amazon Redshift |
| Host | `host.docker.internal` |
| Port | `6433` |
| Database name | `development` (the value of `YASP_GATEWAY_DATABASE`) |
| Username/password | The values of `YASP_GATEWAY_USERNAME` / `YASP_GATEWAY_PASSWORD` |
| SSL | Enabled |
| Additional JDBC options | `preferQueryMode=simple&compression=off&ApplicationName=yasp&ssl=true&sslmode=verify-full&sslrootcert=/tmp/yasp-ca.crt` |

`/tmp/yasp-ca.crt` must exist inside Metabase's container. `YASP_DEMO`, `fake` and
`snow` are backend connection coordinates; Metabase authenticates with the gateway
identity and database label above.

Change `gateway.protocol` in the demo YAML to `postgresql` to test that profile.
JDBC requires `preferQueryMode=simple`; Redshift also requires `compression=off`.
Do not pass JDBC options to psycopg2; use its libpq SSL options instead.

The source database is `YASP_DEMO`, with `PUBLIC` users/products/orders/support
and `ANALYTICS` daily sales, type examples and a composite foreign key. Snowflake
source spelling is preserved. The marker table `_YASP_SEED` records seed version 1.

Native and frontend-generated data SQL execute unchanged. Unmarked editor SQL
uses Snowflake grammar after recognized frontend setup and metadata handling.
Explicit Metabase native remarks use Snowflake; MBQL retains the frontend grammar
for parsing and follows ordinary routing to Snowflake. YASP does not translate
data SQL or reject it based on its input dialect. Snowflake reports unsupported SQL;
successful execution does not establish identical semantics across dialects.
After upgrading metadata handlers, use **Sync database schema now** in Metabase's
database administration page to populate fields for tables it already discovered.

```sql
SELECT ID, FULL_NAME FROM PUBLIC.USERS ORDER BY ID;
SELECT AMOUNT, "MixedCase", PAYLOAD FROM ANALYTICS.METADATA_TYPES;
```

## Result caching

Set `cache.enabled: true` in the demo YAML and provide `YASP_REDIS_URL` through the
environment or `.env`. Ordinary autocommit data queries use the shared engine cache
through either frontend. Entries use `yasp:cache:snowflake_reader:<query-id>` with
this demo's namespace and route. Metadata and explicit transactions bypass it.
For ordinary psycopg2/SQLAlchemy BI reads, enable autocommit as described in
[client configuration](../../docs/configuration.md#jdbc-clients).

## Add data and preserve it

Send SQL directly to the emulator, through its Snowflake HTTP interface. YASP's
application path remains read-only.

```sh
printf "INSERT INTO PUBLIC.USERS (ID,FULL_NAME) VALUES (100,'Demo user');\n" |
  docker compose -f dev/snowflake/compose.yml exec -T snowflake python sql.py

docker compose -f dev/snowflake/compose.yml exec -T snowflake python sql.py < my-data.sql
```

Use `USE SCHEMA YASP_DEMO.PUBLIC` or `USE SCHEMA YASP_DEMO.ANALYTICS` before adding
unqualified objects. Multiple SQL statements are accepted by this loading tool.
Do not open the DuckDB files while the server owns them.

The named volume preserves data, comments, declared string lengths, views and
keys across container recreation. Seeding retries unfinished initialization and
skips a completed seed; it does not reset additional data on restart.

```sh
docker compose -f dev/snowflake/compose.yml down
docker compose -f dev/snowflake/compose.yml up --wait
```

To deliberately erase this demo's data and reseed:

```sh
docker compose -f dev/snowflake/compose.yml down --volumes
docker compose -f dev/snowflake/compose.yml up --wait
```

The listener is published on numeric loopback `127.0.0.1:64616`. Fake credentials
are `fake` / `snow`, account `fakesnow`, warehouse `LOCAL`, role `PUBLIC`. This
emulator has no authentication or grants. These role names do not test access
isolation. Real Snowflake acceptance is still required for grants, authentication,
warehouse behavior, cancellation and SQL semantics.

## Tests and fixture corrections

```sh
python3 scripts/test.py --full
python3 scripts/test.py -- --test snowflake --features snowflake
python3 scripts/test.py -- --test jdbc --all-features
```

The runner prepares native artifacts and disposable network fixtures automatically.
It uses separate ephemeral storage, plus a uniquely owned volume for the persistence
scenario. It never runs tests against the developer volume.

Pinned corrections under `tests/support/snowflake` implement actual query abort,
session deletion, shared persistent metadata, view discovery, declared-key responses, column comments
and DuckDB's local foreign-key resolution. Test-only Arrow chunk stalls exercise
fetch cancellation and backpressure; they are absent from the developer server.
Dependency versions and the base image digest are pinned. Optional connector
comparisons remain under `tests/diagnostics/snowflake`.
