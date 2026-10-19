# Integration tests

Run the PostgreSQL scenarios in WSL/Linux with Rust 1.89, Python 3,
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
required by the captured psql metadata queries. Set `YASP_TEST_PG_BIN` if its binaries
are outside `/usr/lib/postgresql/18/bin` or `pg_config --bindir`.

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
system loader path. For the local pyenv installation:

```sh
PYO3_PYTHON="$HOME/.pyenv/versions/3.12.9/bin/python3" \
LD_LIBRARY_PATH="$HOME/.pyenv/versions/3.12.9/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
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

| Scenario                  | Observable contract                                                                                                                                                                                                                    |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `gateway`                 | Compiled host startup, authentication, simple-query forwarding, native values/metadata, read-only enforcement, protocol limits, pools, timeouts, cleanup, and config validation.                                                       |
| `routing`                 | Metadata classification/replay, selected credentials and source ACLs, errors and same-session recovery, separate pools, cancellation, and discarded cancelled leases.                                                                  |
| `tls`                     | Verified frontend/source/Redis connections, outbound client certificates, SCRAM channel binding, encrypted cancellation, disconnect/bounds recovery, relative certificate paths, compute/discovery, cache replay, and dataset refresh. |
| `cache`                   | Both frontend profiles with PostgreSQL, normalized hits, expiry, selected-login scope, current selector denial, context order, metadata bypass, malformed entries, row/byte bounds, lossless values, cancelled fills, eligibility failures, and Redis outages.                 |
| `context_gateway`         | Fixed-backend context fallback, cancellation, disconnect, and shutdown.                                                                                                                                                                |
| `context`, `python_hooks` | Database-free hook contracts, bounds, worker capacity, failure defaults, and shutdown.                                                                                                                                                 |
| `datasets`                | Redis dataset startup/replacement, JSON fidelity, lookup limits, refresh coordination, failure recovery, and lease/revision fencing.                                                                                                   |
| `datasets_gateway`        | Compiled-host startup loading, Rust dataset reads through context/selection/eligibility, current policy before cache hits, and invocation expiry.                                                                                      |
| `datasets_python`         | Python loader callbacks, bounded lookup helpers, cancellation/expiry, and independent loader/query workers. Requires the Python feature.                                                                                               |

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

The `snowflake` feature automatically prepares the verified native driver and
starts an owned Fakesnow container. Its normal tests cover the native connector,
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

The `catalog` test covers periodic DDL refresh, pinned generation stability, source
spelling/types, column-grant isolation, bounded-load failures and expiry.
Optional [connector diagnostics](diagnostics/snowflake/README.md) compare rejected
approaches separately from functional acceptance. Driver construction and inventory
utilities remain under `scripts/`; fixture setup is owned by the normal runner.
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
