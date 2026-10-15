# Development setup

## Rust toolchain

Use Rust 1.89.0 with rustfmt and Clippy, as pinned in rust-toolchain.toml. The package
minimum is Rust 1.89; serde-saphyr and pgwire 0.41 require it.
A system Cargo/Rust installation does not automatically honor rustup toolchain files.
Check `rustc --version`; with rustup, install the pinned toolchain:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
rustup toolchain install 1.89.0 --profile minimal --component rustfmt --component clippy
cargo check --locked --all-targets
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo run --locked
```

Commit Cargo.lock for the repository host application. Library consumers resolve
their own dependency graph. Recheck the minimum compiler and features on upgrades.
The integration check creates a fresh PostgreSQL database for each run and drops
that database during teardown. It requires a PostgreSQL login with `CREATEDB`
permission, Python 3, `psql`/libpq, and a free `127.0.0.1:6432`. See
[test guidance](../tests/README.md) for setup and fixture behavior.

## Dependency choices through Phase 2

Cargo.lock pins installed versions. Rows marked deferred describe future choices and
are not present in the current dependency graph.

| Dependency                                                 | Status and purpose                                                                                                   | Constraints and alternatives                                                                                                                                                                                                                                                                                     |
| ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tokio 1.53                                                 | Added: async runtime, networking, queues, timeouts, shutdown.                                                        | Enable the used feature families instead of `full`; Python work stays off async workers.                                                                                                                                                                                                                         |
| Serde 1 + serde-saphyr 1.3                                 | Added: typed YAML configuration deserialization.                                                                     | serde_yaml is archived/unmaintained. serde-saphyr provides duplicate-key rejection and parsing budgets; keep budgets enabled and reject unknown configuration fields. Serialization/includes/interpolation are disabled. TOML + the toml crate is a reasonable alternative only if the YAML requirement changes. |
| thiserror 2                                                | Added: typed library errors.                                                                                         | Keep recoverable error categories; add no generic error framework to the library.                                                                                                                                                                                                                                |
| tracing 0.1 + tracing-subscriber 0.3                       | Added: structured events and host log filtering/output.                                                              | The library emits events; the host alone installs the subscriber. Redact query data and secrets.                                                                                                                                                                                                                 |
| PyO3 0.29                                                  | Added behind the `python` feature: embed CPython for trusted callbacks.                                              | No extension-module, abi3, auto-initialize, or unused conversion features. Initialize explicitly after configuring imports. Rust-only applications do not link Python.                                                                                                                                           |
| serde_json 1                                               | Added: shared JSON values for bounded context, hook params, and Python callback input/output.                          | The callback boundary is not a lossless database value codec.                                                                                                                                                                                                                                                    |
| pgwire 0.41                                                | Added with `server-api-ring` and `client-api-ring`: frontend protocol handling and native PostgreSQL backend client. | Streams text rows and PostgreSQL metadata without converting values through a driver value model. The guarded frontend transport checks the configured frame bound before payload allocation.                                                                                                                    |
| dotenvy 0.15                                               | Added: host-only `.env` parsing.                                                                                     | Iterator API avoids process environment mutation; parse before runtime startup and redact parser diagnostics.                                                                                                                                                                                                    |
| futures 0.3 + async-trait 0.1 + tokio-util 0.7             | Added: asynchronous protocol handlers and framed transport.                                                          | Uses pgwire streaming APIs; no separate query framework.                                                                                                                                                                                                                                                         |
| deadpool 0.13 (`managed`, `rt_tokio_1`)                    | Added: bounded PostgreSQL backend pool.                                                                              | Uses the managed pool API; `deadpool-postgres` and `tokio-postgres` are not installed.                                                                                                                                                                                                                           |
| rustls + tokio-rustls; compatible PostgreSQL TLS connector | Deferred to transport implementation.                                                                                | Frontend TLS does not secure backend connections. Validate CA roots and hostnames on both database and Redis transports. Select compatible versions and one crypto provider with pgwire; avoid duplicate providers.                                                                                              |
| sqlparser 0.63 (`visitor`)                                 | Added: bounded PostgreSQL-dialect syntax inspection.                                                                 | The parser recursion budget is 64; this is not a limit of 64 SQL nesting levels. Parsing is syntax inspection, not semantic authorization.                                                                                                                                                                       |
| redis 1.7                                                  | Added for the opt-in result cache.                                                                                    | Async Tokio support and a multiplexed connection are sufficient; no separate pool/cluster/cache framework. Off-host use awaits verified TLS support.                                                                                                                                                              |
| Axum 0.8                                                   | Deferred to Phase 2 management APIs.                                                                                 | Fits Tokio. Add authentication/authorization and request limits before exposing configuration or execution. No UI tooling is needed now.                                                                                                                                                                         |
| Warehouse/model SDKs                                       | Deferred to the chosen Phase 2 adapters.                                                                             | Choose an actual backend/model first. Reuse an existing HTTP client when sufficient; keep vendor SDKs in the consuming application's model adapter or the database adapter.                                                                                                                                      |

NUMERIC conversion needs special attention: rust_decimal's finite precision is not
enough for every PostgreSQL numeric. Preserve native values or use a proven lossless
codec; do not enable a decimal integration and infer full PostgreSQL type support.

## Phase 1 prerequisites

| Component                          | Requirement                                                                                                                   |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Rust                               | Supported compiler, rustfmt, and Clippy.                                                                                      |
| Python, when enabled               | CPython 3.12 with a shared libpython and a compatible standard library; verify the selected installation's development files. |
| PostgreSQL                         | Dedicated test databases and non-owner, non-superuser, least-privileged logins without BYPASSRLS.                             |
| Redis, when caching is enabled      | Dedicated namespace/ACL, memory cap, eviction policy, and bounded client timeouts.                                            |
| Native build tools                 | C compiler and linker; additional tools only when selected crate features require them.                                       |
| BI client                          | Validate its version, connection settings, and supported query behavior before claiming compatibility.                        |

## BI client compatibility

Record the intended BI client's version, network path to the proxy, authentication
mode, and representative startup, discovery, and query traffic. Use one BI connection
with shared proxy credentials. The consuming application supplies any trusted context
provider, policy data, and role selector. Define the metadata contract for the selected
client; do not assume SQL comments establish end-user identity or prevent a client
from forging routing attributes.

Validate discovery, query forms, background work, and missing-context behavior under
the deployment's authorization model. Review BI-side caching and shared metadata
access before enabling those features.

## Python embedding

There are two independent dependency systems: Cargo builds the Rust engine and PyO3;
pip installs packages imported by application hooks. PyO3 links a CPython library; it
does not bundle Python, create a virtual environment, or install Python packages.
The supplied hooks/requirements.txt is intentionally empty apart from comments.

Create one environment per consuming application/deployment using the same CPython
minor version and architecture that the Rust binary embeds:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r hooks/requirements.txt
.venv/bin/python -m pip check
PYO3_PYTHON="$PWD/.venv/bin/python" cargo build --locked --features python
```

The application owns its requirements file: pin direct and transitive dependencies
with hashes for deployment, or use a committed uv.lock and `uv sync --frozen` in
applications already using uv. Model SDKs, numpy, or inference engines belong there
only when a hook uses them. PyYAML is unnecessary: Rust parses YAML and passes policy
data as Python objects. Maturin is for producing Python extension packages and is
not required for embedding.

`PYO3_PYTHON` selects build/link settings, not the interpreter's runtime import path.
Activating a venv or setting VIRTUAL_ENV alone does not configure an embedded
interpreter. The deployed binary needs the matching shared library, base standard
library, hook modules, and that environment's site-packages. A venv alone is not a
portable Python distribution.

For the local import probe, pass the venv site-packages explicitly. On Linux, expose
libpython through the loader if it is not installed in a standard search path:

```sh
YASP_PYTHON_SITE=$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')
YASP_PYTHON_LIB=$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')
PYO3_PYTHON="$PWD/.venv/bin/python" \
PYTHONPATH="$YASP_PYTHON_SITE:$PWD/hooks/examples" \
PYTHONNOUSERSITE=1 \
LD_LIBRARY_PATH="$YASP_PYTHON_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
cargo run --locked --features python --example python_environment -- passthrough
```

Replace `passthrough` with an installed package's import name to check that dependency
inside the Rust process. The probe only initializes Python and imports a module; it
does not validate worker execution. A missing module exits with an error.
On platforms with separate purelib and platlib paths, provide both. Do not set
PYTHONHOME to the venv: it usually lacks the base standard library.

The probe can still see the base interpreter's site-packages. The callback runtime
initializes CPython with isolated PyConfig and site imports disabled. It ignores
PYTHONPATH and adds only `hooks.python.search_paths` to the base standard-library
paths. No working-directory, user-package, base-site-package, or automatic .pth
imports are added. Supply both purelib and platlib when they differ; dependencies
requiring .pth startup actions need application packaging that makes their imports
available explicitly. Missing imports and invalid callback registration fail before
accepting connections. Missing pkg-config metadata alone is not evidence that
Python needs reinstalling. Consuming applications must let the runtime initialize
CPython; an existing interpreter or second runtime is rejected. Clone the runtime
to share it among callbacks, and never finalize it while callbacks exist.

All hooks in one embedded interpreter share sys.modules, package versions, and the
Python 3.12 GIL. There is no per-query venv or package installation. Bounded dedicated
workers keep blocking Python work off Tokio, but do not provide parallel Python CPU
execution or terminate stuck callbacks. A timed-out worker retains its permit until
it finishes. Conflicting package versions, untrusted code, or hard kill deadlines
require process workers with separate environments. Replace processes to activate
new packages; do not pip-install or reload native modules in a serving interpreter.

### Schema discovery and desktop clients

Metadata has a source independent of the application's query selector:

```yaml
schema_discovery:
  target: primary
  backend_login: secondary
```

The target provides the source database and credentials. Omit the target only when
there is one; omit the login to select its first YAML declaration. Use the source
database name as `YASP_GATEWAY_DATABASE` for browsing and restart after configuration
changes. Catalog enumeration exposes only that source database; refresh the client's
database tree to remove cached unrelated names such as `postgres`.

In DBeaver, use autocommit and these PostgreSQL driver properties:
`preferQueryMode=simple`, `prepareThreshold=0`, `binaryTransfer=false`, and local
`sslmode=disable`. Connect to the gateway database explicitly. `SET application_name`
and discovery `SET search_path` are supported. Use schema-qualified names for ordinary
reads: the discovery search path does not change their routing or execution context.
Clients can reaffirm fixed UTF-8, text formatting, and read-only defaults; other
values receive an unsupported-feature error. Prepared queries, transactions, and
full Metabase sync remain separate compatibility milestones.

### Python selector configuration

The host accepts an external file reference in `hooks.select_backend` as well as
compiled Rust names. Paths are relative to the YAML file, independently of the
process working directory. For ignored `config/local.yml`:

```yaml
hooks:
  select_backend:
    type: python
    path: ../hooks/examples/select_backend.py
    callback: select_backend
    params:
      target: primary
      backend_login: secondary
  python:
    search_paths: []
    queue_capacity: 32
    queue_max_bytes: 4194304
    max_payload_bytes: 2097152
    execution_timeout_ms: 2000
```

Choose a target/login that exists in your own config. The example selects that
pair from params. Params default to an empty mapping; omitted runtime limits retain
the values above. To import application helpers, explicitly add their directory to `search_paths`. Add your environment's
site-packages there when packages are needed; no Python builtin hook is supplied.

Build and run with the same CPython installation used for linking:

```sh
LD_LIBRARY_PATH=$(python3.12 -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))') \
cargo run --locked --features python -- --config config/local.yml
```

Use your normal gateway credentials with psql and run `SELECT current_user` to
confirm the selected source login. A Rust-only build rejects Python references
with a missing-feature diagnostic. Python files load at startup and remain external
to the binary; restart after changing scripts or dependencies.

The runtime uses one worker and bounded immediate admission. Timeouts stop waiting,
not executing Python; cancelled/timed-out jobs retain worker capacity until they
return. See [the callback contract](../hooks/README.md#external-python-callbacks)
for data types, resource bounds, errors, and the reusable library API.

### Context hook configuration

The optional `hooks.context` callback derives application context once after query
validation and before backend selection and cache eligibility, including for cache
hits. Its result is available to later hooks as nested JSON data. Authenticated
frontend identity remains separate. Provider errors, invalid results, timeouts, or
Python saturation yield empty context; cancellation stops the query. No BI identity
extractor is enabled by default.

The repository binary can load Python context callbacks when built with the `python`
feature. A consuming Rust host registers its own compiled callback name and binds it
with `hooks::bind_context`. The reference form is:

```yaml
gateway:
  context_timeout_ms: 2000
  max_context_bytes: 65536
hooks:
  context:
    type: python
    path: ../hooks/examples/context.py
    callback: derive_context
    params: {}
```

Timeout defaults to 2000 ms, encoded context defaults to 65,536 bytes, and maximum
nested values are limited to 64 levels. Python receives `function(request, params)`
with JSON data and returns a dictionary or `None`. The example may call
`yasp_extractors`; the named Metabase, Superset, and Redash functions currently
raise `NotImplementedError`. An application should add extraction only after
validating the actual metadata provided by its BI client.

Context does not partition cache entries or alter their validation scope. Source-role
permissions remain the database access boundary; application RBAC and result
processing are separate work.

## Runtime configuration

The host loads ignored `config/local.yml` by default. Use `--config PATH` to select
another YAML file. Use the supported runtime fields described below and copy the
secret template:

```sh
cp .env.example .env
```

`config/example.yml` is loadable by the current runtime. Provision its databases,
logins, and environment secrets before running it. Planned settings remain in marked
commented blocks. Caching is disabled by default; set `cache.enabled: true` to opt in.
Runtime YAML supports `version: 1`, optional `gateway`, `backend`, and `cache` blocks,
PostgreSQL targets and logins, optional `hooks.context`, `hooks.select_backend`, and
`cache.eligibility` references, context bounds in `gateway`, and shared
`hooks.python` runtime settings.
Unknown fields and sections are rejected.
The loader also rejects duplicate keys, invalid types, and YAML files larger than
64 KiB.

For an explicit built-in selector and default target/login, use:

```yaml
version: 1
hooks:
  select_backend:
    type: rust
    callback: builtin.select_backend.passthrough
    params:
      target: primary
      backend_login: reader
targets:
  primary:
    engine: postgresql
    host: "127.0.0.1"
    port: 5432
    database: yasp
    tls_mode: disable
    logins:
      reader:
        username: yasp_reader
        password_env: YASP_PRIMARY_READER_PASSWORD
```

Both languages use `type`, `callback`, and an optional JSON-compatible `params`
mapping. Python also requires `path`; Rust callback names refer to code compiled
and registered by the host. The host parses Rust params into its callback's typed
state before binding with `hooks::bind_select_backend`. Custom params, including
RBAC data, remain application-owned; the engine does not interpret their keys.

The built-in passthrough accepts exactly `target` and `backend_login` strings. Empty
params infer the sole configured pair; multiple pairs require explicit params.
An omitted selector or the Rust shorthand
`select_backend: builtin.select_backend.passthrough` uses empty params. Selection errors never
trigger a fallback. The strict loader rejects a separate `routing` block; put the
passthrough choice in its params.

Custom Rust selector code is compiled into the host application. Rebuild after
changing that code and restart the process after changing its configuration.

```sh
cargo run --locked -- --config config/local.yml
cargo build --release --locked
./target/release/yasp --config config/local.yml
```

The optional `gateway` block supports these settings; omitted fields use the defaults
shown:

```yaml
gateway:
  protocol: postgresql
  listen: "127.0.0.1:6432"
  max_sessions: 32
  max_message_bytes: 8388608
  max_sql_bytes: 1048576
  query_timeout_ms: 60000
  read_timeout_ms: 600000
  write_timeout_ms: 60000
  startup_timeout_ms: 120000
  shutdown_timeout_ms: 10000
  tls:
    mode: local_development
```

`protocol` must be `postgresql`, and `listen` must be a loopback socket address with
a nonzero port. `tls.mode` must be `local_development`, which uses plaintext.
Real TLS is deferred.
`max_message_bytes` bounds incoming frontend frames; the backend frame limit remains
fixed at 8 MiB. `max_sql_bytes` and the frontend write timeout are configurable here.
Gateway credentials and the frontend database label are not YAML settings.
Session, SQL, and timeout limits must be positive; incoming frame limits must be
between 8 and 2,147,483,647 bytes. Query and write timeouts cannot exceed
2,147,483,647 milliseconds, the PostgreSQL timer limit.

Timeouts under `gateway` describe proxy policy:

| Setting               | What it bounds                                                                                                                                   |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `query_timeout_ms`    | Database query wait budget and PostgreSQL statement timeout. Frontend writes, pool acquisition, and cleanup have separate deadlines.             |
| `read_timeout_ms`     | Waiting for the next complete message from an authenticated client, including idle and partial requests. It does not run during query execution. |
| `write_timeout_ms`    | Each response write to a client that is not consuming data quickly enough.                                                                       |
| `startup_timeout_ms`  | The complete frontend startup and authentication exchange.                                                                                       |
| `shutdown_timeout_ms` | Draining existing frontend sessions after the listener stops accepting clients.                                                                  |

The PostgreSQL adapter enforces the gateway query budget when sending a query and
waiting for database replies. Statement and idle-transaction timers are scoped to
that query's read-only transaction; the idle timer uses the larger query/write
budget. Pool connection, acquisition, and cleanup limits are separate operational
bounds, rather than target database credentials.

Each target must use PostgreSQL and has a host, port, database, TLS mode, and one or
more named logins. Each login has a username and `password_env` key naming the
environment variable that holds its password. The router selects only configured
target/login pairs. The built-in passthrough uses `hooks.select_backend.params`
to choose a target/login when multiple pairs make the choice ambiguous. Custom Rust
selectors are registered by the host application; their name is the reference's
`callback` or the Rust string shorthand. An omitted selector defaults to
`builtin.select_backend.passthrough`.
The repository host registers the built-in passthrough selector and loads Python
file references when built with `python`. It rejects compiled selector names it
does not register; a consuming host maps its own names to its compiled callbacks.

Prepared-statement and portal limits are not runtime settings in this milestone.

### Multi-route pool tuning

For multiple configured target/login pairs, each pair gets a lazy pool. Capacity
defaults are 8 connections per pool and 16 total across all pools. Configuration is
rejected when the sum of configured per-pool capacities exceeds the total. Set both
limits in the optional `backend` block when the default total cannot cover the pairs:

```yaml
backend:
  # Example for no more than two configured target/login pairs.
  max_connections_per_pool: 4
  max_connections_total: 8
```

Selectors choose among these configured pairs using their application params or
host-owned state. The host binds compiled callbacks or registers external Python
files/functions before opening the listener. Callback decisions are validated before
connection acquisition.

Gateway credentials are supplied only through `YASP_GATEWAY_USERNAME`,
`YASP_GATEWAY_PASSWORD`, and `YASP_GATEWAY_DATABASE` in the process environment or
optional `.env`. The gateway database value is the frontend connection label;
`targets.<target>.database` selects the actual PostgreSQL database. The target login
password comes from its named `password_env` variable. Process environment values
take precedence over `.env`; the file is optional when all required variables
are already supplied. Target connection settings come from YAML.

The frontend listener must use a loopback address. It defaults to `127.0.0.1:6432`
and can be changed in the optional gateway block. Backend targets use their configured
host and currently require `tls_mode: disable`. The same target/login structure and
optional gateway settings are used by `tests/config.yml`.

The default limits are: 32 frontend sessions, 8 MiB per frontend or backend frame
including its header, 1 MiB SQL text, 4,096 significant SQL tokens, and a parser
recursion limit of 64; eight connections per backend pool and 16 total; 10 seconds each for backend
connect/acquire/cleanup; and 60 seconds each for backend query waits and frontend
writes. The gateway settings for session count, incoming frontend frame size, SQL
size, listener, and query/read/write/startup/shutdown timeouts can be overridden in
the optional YAML block. Authenticated frontend message reads default to 60 seconds,
startup to 120 seconds, and shutdown to 10 seconds. The backend frame limit and pool
connect/acquire/cleanup limits remain fixed. Runtime sets
UTF8 client encoding, `DateStyle = ISO, MDY`, UTC timezone, `IntervalStyle = postgres`,
and the default `bytea_output = hex`. Each query runs in a read-only transaction.

For tests, `tests/config.yml` uses the same version/target/login structure and sets
the target database to the `postgres` maintenance database. The harness creates a
fresh test database from `template0`, runs the proxy against it, and drops only that
database during teardown. The test target password uses the named variable
`YASP_TEST_TARGET_PASSWORD`.

### Redis result cache

Caching is disabled by default. Enable `cache.enabled` and provide the Redis URL
through `YASP_REDIS_URL` (or the environment variable named by `url_env`). A hit does
not contact PostgreSQL, so source grants and row-level security are not rechecked
until expiry. The default TTL is one hour; hits do not extend it.

```yaml
cache:
  enabled: false
  url_env: YASP_REDIS_URL
  namespace: yasp:dev:v1
  ttl_seconds: 3600
  max_rows_per_query: 10000
  max_buffered_bytes_total: 33554432
  eligibility: # Optional Rust or Python callback to bypass selected queries.
    type: rust
    callback: builtin.cache_eligibility.exclude_sql_containing
    params:
      excluded_sql_fragments: [pg_stat_activity, clock_timestamp]
```

The row limit bounds rows captured and replayed; larger results bypass caching and
continue streaming. The 32 MiB default is an aggregate in-process buffer budget. The
internal serialized-entry guard is one third of that budget, reserving room for Redis,
memory, and response copies. Redis storage and its eviction policy are separate.
Frontend and selected-backend scope are checked on hits. Redis failures, timeouts,
malformed entries, and scope mismatches are misses.
The Redis operation deadline is fixed at 100 ms. The current connection supports
loopback TCP only; verified remote TLS is deferred.

The built-in Rust hook checks effective SQL for case-insensitive literal substrings.
It includes comments and string literals, so it can bypass extra queries, and it does
not guarantee a parsed table-name match. Python uses the same `excluded_sql_fragments`
params through `request["sql"].lower()`. Use these fragments when monitoring results
must stay fresh. See [cache hook examples](../hooks/README.md#cache-eligibility).

For a local check, export `YASP_REDIS_URL`, enable caching and the example policy,
then run both queries twice: `SELECT 1` should hit the cache; `clock_timestamp()`
should stay fresh. Inspect only your namespace:

```sh
redis-cli -u "$YASP_REDIS_URL" PING
redis-cli -u "$YASP_REDIS_URL" --scan --pattern 'your-unique-namespace:*'
for run in 1 2; do
  PGPASSWORD="$YASP_GATEWAY_PASSWORD" psql "host=127.0.0.1 port=6432 dbname=$YASP_GATEWAY_DATABASE user=$YASP_GATEWAY_USERNAME" -c 'SELECT 1' -c 'SELECT clock_timestamp()'
done
```

Use a dedicated Redis deployment with a memory cap and eviction policy. Do not print
cached payloads; they can contain query results.

The library exposes `yasp::hooks::SelectionRequest`, `BackendSelection`,
`SelectionError`, and `bind_select_backend` for host-supplied Rust selectors.
`SelectionRequest` carries borrowed authenticated-user, client-ID, query-ID, and
`ParsedQuery` values; it contains no backend credentials. Rust selectors can inspect
the borrowed statement tree through `request.query.statement()`. A returned pair is
validated against configuration before the router acquires a connection. The gateway
supports simple-query requests and PostgreSQL cancellation for active simple queries.
Client transactions, prepared statements and extended-query cancellation, `SET`, binary
results, and Unicode escaped identifiers (`U&"..."`) are unsupported. A frontend
disconnect aborts that connection's active backend work.

See [the project plan](PLAN.md) for phase boundaries and planned components.

## References

- [pgwire APIs and feature flags](https://docs.rs/pgwire/0.41.1/pgwire/)
- [serde-saphyr configuration parsing](https://docs.rs/serde-saphyr/1.3.0/serde_saphyr/)
- [serde_yaml maintenance status](https://github.com/dtolnay/serde-yaml)
- [sqlparser capabilities](https://github.com/apache/datafusion-sqlparser-rs)
- [pg_query alternative](https://docs.rs/pg_query/latest/pg_query/)
- [Axum](https://docs.rs/axum/latest/axum/)
- [PostgreSQL numeric range](https://www.postgresql.org/docs/current/datatype-numeric.html)
- [rust_decimal representation](https://docs.rs/rust_decimal/latest/rust_decimal/)
- [Redis async support](https://docs.rs/redis/latest/redis/)
- [PyO3 build settings and embedding](https://pyo3.rs/v0.29.3/building-and-distribution.html)
- [CPython initialization and paths](https://docs.python.org/3.12/c-api/init_config.html)
- [pip repeatable installs](https://pip.pypa.io/en/stable/topics/repeatable-installs/)
