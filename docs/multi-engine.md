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

Frontend metadata is recognized using the gateway dialect before query context.
Native remarks do not override recognized metadata or exact frontend probes such
as `SELECT pg_backend_pid()`. The context hook receives only ordinary raw SQL and
returns its incoming dialect plus application values. Missing dialects default to
the gateway. Metabase explicit native remarks use the common target dialect or an
explicit provider param; MBQL and unknown remarks use the gateway. Redash defaults
to the gateway and supports an explicit dialect override for valid remarks.

Parsing and read-only validation use that incoming dialect. Current PostgreSQL
and Snowflake adapters forward validated effective data SQL after optional application
modification. A syntactically valid query may therefore fail on a different backend dialect. Query context, modification, route
authorization, and result processing run on raw cache hits as well as misses.
The cache scope includes the incoming dialect and execution revision.

## Planned SQL translation

After target/login/compute selection, compare `query_context`'s resolved incoming
dialect with the target adapter's dialect. Equal dialects retain SQL; different
dialects require translation within the supported subset, regardless of client
or native/generated labels. Unsupported translations must fail explicitly.
The [application query modifier](query-modification.md) runs after initial parsing,
before lineage and selection, and emits SQL in the resolved incoming dialect.

For a Redshift gateway and PostgreSQL target, Metabase MBQL and unmarked SQL use
Redshift input and will require Redshift-to-PostgreSQL translation. Explicit
Metabase native SQL uses PostgreSQL input and requires no dialect translation.
Redash follows the gateway unless `params.dialect` specifies its incoming SQL.
Superset/custom hooks declare the same input dialect without a built-in remark
format. Metadata responses never pass through data-query translation.

Reparse translated SQL in the target grammar, repeat bounds/read-only validation,
and recompute effective lineage and query identity before cache eligibility/lookup.
Preserve original SQL separately; cache scope must include the execution/translation
revision. Different target schemas additionally require target-scoped catalogs.
Translation remains future work; no translation configuration or runtime is active.

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

All bindings share one occupied-capacity budget with PostgreSQL. Binding limits
remain additional local bounds. Registry registration attaches an unused adapter to
that shared budget before discovery or execution; an adapter already attached to a
different budget is rejected. On timeout, cancellation or disconnect, a known
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
local JWT signature/claim validation, SQL API request identity, polling, partition
sequencing and response bounds. Emulator results do not establish Snowflake-enforced
authentication, grants, role/warehouse isolation, asynchronous execution or semantics.

The account-free warehouse acceptance command is:

```sh
python3 scripts/test.py -- --all-targets --features snowflake
```

It runs the adapter tests and end-to-end gateway checks against a temporary emulator
with a generated signing key. These checks cover request construction, partitioned
values, frontend transaction affinity and recovery, deadlines, cancellation paths,
and resource cleanup. They establish YASP's handling of the tested SQL API contract;
they cannot verify controls enforced by Snowflake itself. Real account authentication,
grants and role/warehouse isolation remain a deployment gate.

```sh
python3 scripts/test.py --full
```

This runs the default/Python functional and documentation matrices, then combined
Snowflake/client coverage sequentially. Requesting the Snowflake feature automatically
creates a temporary signing key and an isolated Docker network fixture. External clients use
pinned images without requiring running BI applications. Missing requested
prerequisites fail explicitly. Persistent developer data is never used by tests.

Cache scope includes frontend profile, incoming dialect, the unchanged-SQL
execution revision, actual source identity/role/warehouse, session settings and
serialization format. Explicit transactions and metadata requests bypass the cache.
SQL translation, extended protocol, and federation remain separate work.
