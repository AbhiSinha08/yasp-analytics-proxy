# Integration tests

Run the PostgreSQL scenarios in a Linux environment with Rust 1.89, Python 3,
PostgreSQL 18 (`initdb`, `pg_ctl`, `postgres`, `psql`/libpq), and
TLS-capable `redis-server`/`redis-cli`, and OpenSSL installed:

```sh
python3 scripts/test.py
python3 scripts/test.py -- --doc
```

The runner starts a temporary PostgreSQL cluster and Redis server on free loopback
ports, with plaintext development and TLS endpoints. It generates a disposable CA,
server/client certificates, and invalid/expired certificate cases; all keys stay
in its temporary directory. It creates two non-superuser source logins: a database owner with `CREATEDB`
and a restricted reader. It supplies generated passwords and a temporary test
configuration; no development credentials or services are needed. PostgreSQL 18 is
required by the captured psql metadata queries. Set `YASP_TEST_PG_BIN` to the
PostgreSQL 18 binary directory if the runner cannot find it automatically.

Decision-model integration tests must not start a live Laya server or download its
weights. Mock the decision API and return representative responses instead.
`decision_model` covers REST validation, shared PostgreSQL sampling, strict/advisory
failures, and Redis decision reuse. `snowflake_cache` exercises the same classifier
through both frontend profiles and the Snowflake emulator.

Cargo arguments follow `--`, including individual scenarios:

```sh
python3 scripts/test.py -- --test routing
python3 scripts/test.py -- --test cache context_and_metadata_bypass
python3 scripts/test.py -- --test metabase --test redash --test superset
python3 scripts/test.py -- --test bi_context --features python
python3 scripts/test.py -- --all-targets --features python
```

Tool-specific schema scenarios live in `metabase.rs`, `redash.rs` and `superset.rs`.
`bi_context.rs` verifies the shared example context and role mapping across BI
remark formats. The schema tests share host setup in `support/catalog_host.rs`.

The Python feature needs matching CPython headers and a shared `libpython`. Set
`PYO3_PYTHON` for the build and `LD_LIBRARY_PATH` when that library is outside the
system loader path. For an interpreter managed by pyenv, resolve its paths with:

```sh
PYTHON="$(pyenv which python3.12)"
PYTHON_LIB="$("$PYTHON" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')"
PYO3_PYTHON="$PYTHON" \
LD_LIBRARY_PATH="$PYTHON_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
python3 scripts/test.py -- --all-targets --features python
```

Each live PostgreSQL harness creates its own databases from `template0`; the SQL
fixture and source grants stay inside them. Gateway listeners use ephemeral ports.
Rust guards bound gateway shutdown waits and join completed threads. They reap client
processes, remove temporary files and Redis keys, and drop databases during normal
return or panic. The host
protocol harness uses Python context managers for its processes and database.
Cleanup failures fail the test. The runner stops its entire Cargo process group,
including remaining gateway children, and removes its PostgreSQL/Redis data on
success, test failure, or interruption. `fixtures_lifecycle` deliberately unwinds a
scenario and verifies resource removal.

| Scenario                    | Observable contract                                                                                                                                                                                                                                            |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `gateway`                   | Compiled host startup, authentication, simple-query forwarding, native values/metadata, read-only enforcement, protocol limits, pools, timeouts, cleanup, and config validation.                                                                               |
| `routing`                   | Metadata classification/replay, selected credentials and source ACLs, errors and same-session recovery, separate pools, cancellation, and discarded cancelled leases.                                                                                          |
| `tls`                       | Verified frontend/source/Redis connections, outbound client certificates, SCRAM channel binding, encrypted cancellation, disconnect/bounds recovery, relative certificate paths, compute/discovery, cache replay, and dataset refresh.                         |
| `cache`                     | Both frontend profiles with PostgreSQL, normalized hits, expiry, selected-login scope, current selector denial, context order, metadata bypass, malformed entries, row/byte bounds, lossless values, cancelled fills, eligibility failures, and Redis outages. |
| `context_gateway`           | Fixed-backend query-context failure rejection, cancellation, disconnect, and shutdown.                                                                                                                                                                         |
| `query_modification`        | Effective SQL/AST propagation, downstream lineage, tenant cache isolation, dataset expiry, transactions, invalid rewrites, cancellation, disconnect, shutdown, and the compiled table-rename example.                                                          |
| `query_modification_python` | Python AST/context contract without lineage, copied inputs, dataset expiry, failures/deadlines, and modification-only YAML host startup/shutdown in both frontend profiles.                                                                                    |
| `context`, `python_hooks`   | Database-free hook contracts, bounds, worker capacity, failure rejection, and shutdown.                                                                                                                                                                        |
| `datasets`                  | Redis dataset startup/replacement, JSON fidelity, lookup limits, refresh coordination, failure recovery, and lease/revision fencing.                                                                                                                           |
| `datasets_gateway`          | Compiled-host startup loading, Rust dataset reads through context/selection/eligibility, current policy before cache hits, and invocation expiry.                                                                                                              |
| `datasets_python`           | Python loader callbacks, bounded lookup helpers, cancellation/expiry, and independent loader/query workers. Requires the Python feature.                                                                                                                       |

Routing and cache scenarios live under their matching subdirectories. Shared fixture
ownership is under `support/`; PostgreSQL drivers remain in the real engine adapters.
The Python feature adds routed selection, context, and cache callback scenarios.
Native transaction scenarios cover frontend status and recovery, retained physical
connections, route/discovery/compute conflicts, transactional setup, cache bypass
and session scope, cancellation, disconnects, and idle/partial-message deadlines.
The captured psycopg2/SQLAlchemy driver sequence is replayed by `gateway`.
Captured protocol replay does not establish full BI-client compatibility.
`python3 scripts/test.py --full` runs default/Python functional and documentation
checks, then combined Snowflake and external-client checks, sequentially. Python
runs use `PYO3_PYTHON` when set, otherwise the runner's interpreter, and expose its
matching shared-library directory to subprocesses.

The `snowflake` feature automatically generates a temporary signing key and
starts an owned Docker SQL API emulator. Its normal tests cover the REST connector,
gateway values/transactions/cancellation/recovery, both profiles' metadata, the
Snowflake-only YAML host, shared-cache hits through both profiles, lossless replay,
profile/dialect/role/warehouse scope, malformed entries, transaction bypass,
row/byte bounds, expiry, and persistent data/definitions across container recreation.
Tests never use the developer emulator volume. Explicitly requested prerequisites
fail the run when unavailable; these functional tests are not ignored.
Test image tags, containers and volumes belong to one run and are removed on exit;
rebuilding the developer image cannot replace an active test fixture's image.

The test-only `client-tests` feature enables pinned Metabase JDBC and SQLAlchemy
client containers. No running BI application is required. Capture manifests and
workloads live under `fixtures/clients`; client-specific SQL regressions remain in
the corresponding BI test module. Captured metadata/native-read acceptance does
not establish arbitrary query-builder or real Snowflake compatibility.

The `connection_budget` test covers PostgreSQL/Snowflake contention in both
directions, idle reclamation, cached reads and catalog refresh under saturation.
Focused adapter tests cover multiple bindings and aliases, conflicting registrations,
atomic admission, cancelled waiters and retained cleanup after an interrupted request.

The `catalog_gateway` test covers required/matching catalog startup, native reflection,
metadata with an unavailable source, unsupported requests, transaction failure and
expiry. The `catalog` test covers periodic DDL refresh, pinned generation stability, source
spelling/types, column-grant isolation, bounded-load failures and expiry.
Focused HTTP tests validate signed JWTs, polling, partitions and malformed/oversized
responses independently of emulator limitations. The signing key under
`fixtures/snowflake-test-key.p8` is public test material only. Inventory utilities
remain under `scripts/`; fixture setup is owned by the normal runner.
See [the persistent development setup](../dev/snowflake/README.md).

Future phase coverage belongs in [the plan](../docs/PLAN.md).

## Existing test services

To use pre-provisioned PostgreSQL and Redis instead:

```sh
python3 scripts/test.py --configured
YASP_TEST_CONFIG=/path/to/tests.yml cargo test --locked --all-targets
```

All live harnesses honor `YASP_TEST_CONFIG`, defaulting to `tests/config.yml`.
Process environment values take precedence over the optional root `.env`:

- `YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD`, `YASP_GATEWAY_DATABASE` supply
  the frontend identity and database label.
- Password variables named by the configured logins supply source credentials.
  The example uses `YASP_TEST_TARGET_PASSWORD` and `YASP_TEST_SECOND_PASSWORD`.
- The default source login needs `CREATEDB` on its configured maintenance database.
  Its `secondary` login must be distinct and non-superuser. Tests grant it access
  only to disposable fixture data; they do not create or alter existing roles.
- `YASP_TEST_REDIS_URL` defaults to `redis://127.0.0.1:6379/`. Each cache scenario
  uses a unique namespace and removes only its own keys.

Missing services, credentials, privileges, or fixture setup errors fail the tests.

The `tls` suite additionally requires `YASP_TEST_TLS_DIR` containing `ca.crt`,
`other-ca.crt`, `server.crt`/`.key`, `client.crt`/`.key`, `wrong.crt`/`.key`
(hostname mismatch), and `expired.crt`/`.key`. Its source must accept TLS and
require a client certificate signed by `ca.crt`. Set `YASP_TEST_REDIS_TLS_URL`
to a `rediss://localhost:PORT/` endpoint using the same CA and requiring client
certificates. The ordinary Redis URL is used for administrative inspection and must allow
ACL user creation/deletion and killing that test user's connections. Tests use
these privileges only for their own temporary users and namespaces.
Prefer `python3 scripts/test.py -- --test tls` to provision these fixtures.

## AST lineage

`lineage.rs` runs the offline PostgreSQL/Snowflake/Redshift corpus in
`fixtures/lineage.json`, with manually reviewed source expectations and provenance
links for adapted dbt, TICKIT, Snowflake, and Google Analytics query shapes.
`lineage_gateway.rs` covers current-catalog analysis before selection/result hooks, Redis association
and TTL, mixed model metadata, transactions, and schema refresh on cache hits.
`lineage_python.rs` checks the raw AST and independent plain JSON lineage payloads.
`lineage_snowflake.rs` covers the configured SQL API host and both frontend profiles.
`fixtures/lineage-future.json` documents native BigQuery/Databricks cases for future
adapters; these are not placeholder passing tests or provider acceptance claims.

```sh
python3 scripts/test.py -- --test lineage --test lineage_gateway
python3 scripts/test.py -- --test lineage_python --features python
```

## Result processing

`result_processing` covers Rust/Python batches, raw cache replay, current application
context and AST/model outcomes, NULL/type fidelity, filtering/completion counts,
byte bounds, Python retention/saturation, and cancellation/disconnect recovery.
Snowflake feature runs add configured-host checks through the local emulator.

```sh
python3 scripts/test.py -- --test result_processing --features python
python3 scripts/test.py -- --test result_processing --features snowflake,python
```

`metabase_rbac` starts the three local configurations against disposable PostgreSQL
and Redis with a mocked decision endpoint. It checks source-role restrictions,
Metabase remark identities, email masks on raw cache hits, aliases, NULLs, and
unavailable/uncertain model failures. Run it with:

```sh
python3 scripts/test.py -- --test metabase_rbac --features python
```
