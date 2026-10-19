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
adapters own native drivers, session SQL, cleanup, value conversion and errors.

A Snowflake binding fixes account, principal, role, database, schema and warehouse.
Fresh source connections isolate identities and transaction state; explicit frontend
transactions retain a source session and enforce route affinity. Blocking driver
operations run outside Tokio worker threads. Admission remains held until underlying
native work stops, even when a frontend disconnects or times out. Native parents
outlive statements, Arrow readers and cancellation handles; stale handles cannot
cancel a later operation.

## Native connector and values

The supported Linux x86-64/WSL artifact is the pinned Snowflake ADBC 1.11.0 native
library with the bounded-prefetch patch. `scripts/snowflake/build_native_driver.py`
verifies source/toolchain/patch checksums and records the resulting library digest.
The host verifies that digest before loading trusted executable code. An unmodified
replacement must pass the same gates before becoming supported.

The Rust driver manager remains a rejected diagnostic candidate; the runtime uses
the C ABI directly. Accepted ownership, cancellation, transaction, prefetch and fetch
checks run with ordinary Snowflake functional tests. Optional comparisons live in
[diagnostics](../tests/diagnostics/snowflake/README.md).

Results preserve decimal precision through 38, NULLs, booleans, signed integers,
floats, UTF-8, binary, dates, times and supported timestamps. Unrecognized Arrow
representations fail explicitly. Nonzero sub-microsecond remainders fail rather
than rounding; supported dates/timestamps are years 1–9999. The driver normalizes
zoned timestamps to UTC; preserving the original offset requires an explicit text
cast. Real-source LTZ/TZ behavior still needs validation. Arrow batch checks occur
after allocation and do not hard-cap native process RSS.

## Local environment and verification

[The Docker setup](../dev/snowflake/README.md) provides persistent Fakesnow with two
seeded schemas. Corrected abort/session/key behavior is shared with disposable test
fixtures. Fault-injected Arrow chunks are enabled only for tests. Real-source grants,
warehouse isolation and Snowflake semantics cannot be established by this emulator.

```sh
python3 scripts/test.py --full
```

This runs the default/Python functional and documentation matrices, then combined
Snowflake/client coverage sequentially. Requesting the Snowflake feature automatically
prepares its native artifact and isolated network fixture. External clients use
pinned images without requiring running BI applications. Missing requested
prerequisites fail explicitly. Persistent developer data is never used by tests.

Cache scope includes frontend profile, input dialect/mode, the unchanged-SQL
execution revision, actual source identity/role/warehouse, session settings and
serialization format. Explicit transactions and metadata requests bypass the cache.
Lineage, masking, generated-query translation, extended protocol and federation remain
separate work.
