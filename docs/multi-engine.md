# Multi-engine compatibility

YASP provides PostgreSQL and experimental Redshift profiles on shared pgwire
transport. PostgreSQL and optional Snowflake backends share routing, read-only
inspection, transactions, deadlines and cancellation. Snowflake executes native
and frontend-generated data SQL unchanged; dialect translation remains separate work.

## Supported contract

The [client inventory](../tests/fixtures/clients/clients.json) pins Metabase
v0.64.2.8, PostgreSQL JDBC 42.7.12 and Redshift JDBC 2.2.5. JDBC uses simple-query
mode; Redshift compression is disabled and password authentication requires TLS.
psycopg2 clients use libpq SSL parameters. See [client setup](configuration.md#jdbc-clients).

Functional tests exercise both profiles, native Snowflake values/transactions,
JDBC definition discovery, Metabase's actual connection validation and captured sync SQL, Redash autocomplete and
SQLAlchemy reflection. This is a captured compatibility surface, not arbitrary
PostgreSQL/Redshift SQL transparency. Full Metabase visual-query exploration and
real Snowflake authentication/grant/semantic acceptance remain separate gates.

## Input and execution

The host supplies typed input options before parsing. Explicit Metabase native
remarks select the configured native dialect; explicit MBQL retains the frontend
dialect. Missing/unknown remarks use the frontend dialect by default. The optional
`unmarked_data_dialect` permits native data SQL after recognized setup/metadata
handling. This is a configured choice, not dialect guessing or route selection.
Comments never establish identity or authorization.

For Snowflake-backed listeners, the exact frontend connection probe `SELECT 1`
returns an integer from the compatibility handler, with a fresh catalog required.
This satisfies JDBC BI checks that compare the value with an integer. Explicit
native remarks bypass that handler and preserve Snowflake's numeric result type;
queries containing table reads or other expressions use ordinary execution.

The exact unaliased `SELECT pg_backend_pid()` and
`SELECT pg_catalog.pg_backend_pid()` probes return the gateway session's integer
PID from pgwire startup on both profiles, without routing, source execution or
result caching. Explicit native SQL and queries containing application reads or
other expressions retain ordinary execution. Superset's SQL-based cancellation
through `pg_cancel_backend`/`pg_stat_activity` remains outside this surface; pgwire
cancellation still uses the session PID and secret key.

Original SQL and its immutable AST reach context and routing. Read-only checks apply
before execution. PostgreSQL rejects an explicit native dialect for another engine.
Snowflake forwards both native and frontend-generated data queries without a dialect
gate or translation. Snowflake reports SQL errors; syntactic acceptance does not
guarantee equivalent behavior across dialects. Metadata compatibility handling remains
separate. Both backends use the shared result cache when `cache.enabled` is true.
The engine owns eligibility, capture, validation and replay; adapters have no cache
opt-in. PostgreSQL native metadata/text and converted Snowflake values remain exact.

The YAML host accepts engine-specific Snowflake targets and a Snowflake discovery
binding with a [shared catalog](catalog.md). It needs no PostgreSQL placeholder.
Library hosts can use `Backends::without_postgres`, register Snowflake bindings and
call `serve_backends`; existing PostgreSQL entry points remain available. Backend
adapters own HTTP clients, session SQL, cleanup, value conversion and errors.

A Snowflake binding fixes account, principal, role, database, schema and warehouse.
The adapter submits these coordinates on every public SQL API request with an
RSA key-pair JWT over verified HTTPS. HTTP overrides are restricted to explicit
numeric loopback emulator endpoints. Private keys stay in the host; no native
shared library, driver build or driver checksum is required.

## SQL API execution and values

Each statement is independently autocommitted. Frontend read-only `BEGIN`,
`COMMIT` and `ROLLBACK` blocks retain route affinity and normal I/T/E failure
states for BI compatibility, but do not create a shared Snowflake transaction or
snapshot. Rollback cannot undo a previous statement. Source read-only validation
still applies; the adapter never submits frontend transaction commands to Snowflake.

The asynchronous HTTP client polls accepted statements, then downloads result
partitions sequentially and sends rows through the common frontend encoder.
It retains one bounded decompressed JSON response at a time rather than collecting
the whole multi-partition result. `max_response_bytes` bounds a response (including
metadata); exceeding it fails explicitly. JSON decoding and frontend encoding add
bounded copies, so this is not a process RSS limit. Partition size is chosen by
Snowflake; larger partitions require a larger configured bound.

Admission is bounded per binding. On timeout, cancellation or disconnect, a known
statement handle gets a best-effort cancel followed by status checks. If submission
was interrupted before its handle arrived, capacity remains held for a full
server timeout plus a short grace period starting at interruption. Immediate remote cancellation
is not guaranteed. Abandoned response downloads cannot publish cache completion.

Results preserve decimal precision through 38, NULLs, booleans, floats, UTF-8,
binary, dates, times and supported timestamps. The adapter requests explicit
output formats and UTC on every statement. Nonzero sub-microsecond remainders
fail rather than rounding; supported dates/timestamps are years 1–9999. Zoned
timestamps encode the instant in UTC; use a text cast to preserve the original
offset. Unknown types fail explicitly. Real-source timestamp behavior remains an
acceptance gate.

## Local environment and verification

[The Docker setup](../dev/snowflake/README.md) builds a pinned Go/DuckDB Snowflake
emulator with two seeded schemas and persistent storage. Its documented fixture
corrections expose actual definitions through Snowflake-shaped metadata. It has no
access control and executes synchronously; focused HTTP tests separately exercise
JWT validation, polling, partitions and response bounds. Emulator results do not
establish real Snowflake grants, authentication, warehouse isolation or semantics.

```sh
python3 scripts/test.py --full
```

This runs the default/Python functional and documentation matrices, then combined
Snowflake/client coverage sequentially. Requesting the Snowflake feature automatically
creates a temporary signing key and an isolated Docker network fixture. External clients use
pinned images without requiring running BI applications. Missing requested
prerequisites fail explicitly. Persistent developer data is never used by tests.

Cache scope includes frontend profile, input dialect/mode, the unchanged-SQL
execution revision, actual source identity/role/warehouse, session settings and
serialization format. Explicit transactions and metadata requests bypass the cache.
Lineage, masking, generated-query translation, extended protocol and federation remain
separate work.
