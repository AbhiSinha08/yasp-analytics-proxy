# Integration tests

Run the PostgreSQL scenarios in WSL/Linux with Rust 1.89, Python 3,
PostgreSQL 18 (`initdb`, `pg_ctl`, `postgres`, `psql`/libpq), and
`redis-server`/`redis-cli` installed:

```sh
python3 scripts/test.py
python3 scripts/test.py -- --doc
```

The runner starts a temporary PostgreSQL cluster and Redis server on free loopback
ports. It creates two non-superuser source logins: a database owner with `CREATEDB`
and a restricted reader. It supplies generated passwords and a temporary test
configuration; no development credentials or services are needed. PostgreSQL 18 is
required by the captured psql metadata queries. Set `YASP_TEST_PG_BIN` if its binaries
are outside `/usr/lib/postgresql/18/bin` or `pg_config --bindir`.

Cargo arguments follow `--`, including individual scenarios:

```sh
python3 scripts/test.py -- --test routing
python3 scripts/test.py -- --test cache context_and_metadata_bypass
python3 scripts/test.py -- --all-targets --features python
```

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

| Scenario | Observable contract |
| --- | --- |
| `gateway` | Compiled host startup, authentication, simple-query forwarding, native values/metadata, read-only enforcement, protocol limits, pools, timeouts, cleanup, and config validation. |
| `routing` | Metadata classification/replay, selected credentials and source ACLs, errors and same-session recovery, separate pools, cancellation, and discarded cancelled leases. |
| `cache` | Normalized hits, expiry, selected-login scope, current selector denial, context order, metadata bypass, malformed entries, row/byte bounds, lossless values, cancelled fills, eligibility failures, and Redis outages. |
| `context_gateway` | Fixed-backend context fallback, cancellation, disconnect, and shutdown. |
| `context`, `python_hooks` | Database-free hook contracts, bounds, worker capacity, failure defaults, and shutdown. |

Routing and cache scenarios live under their matching subdirectories. Shared fixture
ownership is under `support/`; PostgreSQL drivers remain in the real engine adapters.
The Python feature adds routed selection, context, and cache callback scenarios.
Captured psql/DBeaver protocol replay does not establish full BI-client compatibility.
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
