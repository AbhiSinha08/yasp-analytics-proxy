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
| pgwire 0.41                                                | Added with `server-api-ring` and `client-api-ring`: frontend protocol handling and native PostgreSQL backend client. | Streams text rows and PostgreSQL metadata without converting values through a driver value model. The guarded frontend transport checks the configured frame bound before payload allocation.                                                                                                                    |
| dotenvy 0.15                                               | Added: host-only `.env` parsing.                                                                                     | Iterator API avoids process environment mutation; parse before runtime startup and redact parser diagnostics.                                                                                                                                                                                                    |
| futures 0.3 + async-trait 0.1 + tokio-util 0.7             | Added: asynchronous protocol handlers and framed transport.                                                          | Uses pgwire streaming APIs; no separate query framework.                                                                                                                                                                                                                                                         |
| deadpool 0.13 (`managed`, `rt_tokio_1`)                    | Added: bounded PostgreSQL backend pool.                                                                              | Uses the managed pool API; `deadpool-postgres` and `tokio-postgres` are not installed.                                                                                                                                                                                                                           |
| rustls + tokio-rustls; compatible PostgreSQL TLS connector | Deferred to transport implementation.                                                                                | Frontend TLS does not secure backend connections. Validate CA roots and hostnames on both database and Redis transports. Select compatible versions and one crypto provider with pgwire; avoid duplicate providers.                                                                                              |
| sqlparser 0.63 (`visitor`)                                 | Added: bounded PostgreSQL-dialect syntax inspection.                                                                 | The parser recursion budget is 64; this is not a limit of 64 SQL nesting levels. Parsing is syntax inspection, not semantic authorization.                                                                                                                                                                       |
| redis 1.7                                                  | Deferred until cache eligibility and serialization exist.                                                            | Async Tokio support and a multiplexed connection are sufficient initially; no separate pool/cluster/cache framework. Enable verified TLS when deployed off-host.                                                                                                                                                 |
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
| Redis, when caching is implemented | Dedicated namespace/ACL, memory cap, eviction policy, and bounded client timeouts.                                            |
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

The probe can still see the base interpreter's site-packages. The production host
must configure explicit trusted paths before importing hooks, excluding the working
directory, user packages, and unrelated base-environment packages. Use CPython PyConfig
when isolated initialization is needed; the probe's environment setup is a local
development convenience. If approved dependencies need .pth processing, initialize
that controlled site directory deliberately. Validate standard-library/native
extension imports, required package versions, and callback registration before
accepting connections. Missing pkg-config metadata alone is not evidence that
Python needs reinstalling.

All hooks in one embedded interpreter share sys.modules, package versions, and the
Python 3.12 GIL. There is no per-query venv or package installation. Bounded dedicated
workers keep blocking Python work off Tokio, but do not provide parallel Python CPU
execution or terminate stuck callbacks. A timed-out worker retains its permit until
it finishes. Conflicting package versions, untrusted code, or hard kill deadlines
require process workers with separate environments. Replace processes to activate
new packages; do not pip-install or reload native modules in a serving interpreter.

## Runtime configuration

The host loads ignored `config/local.yml` by default. Use `--config PATH` to select
another YAML file. Create it from the minimal `config/example.yml` and copy the
secret template:

```sh
cp .env.example .env
```

`config/example.yml` is a minimal, supported single-target/single-login example.
Runtime YAML supports `version: 1`, an optional `gateway` block,
PostgreSQL targets and their logins, an optional `routing.default_backend`, optional
`hooks.select_backend`, and an optional `backend` block. Unknown fields and sections
are rejected.
The loader also rejects duplicate keys, invalid types, and YAML files larger than
64 KiB.

For an explicit built-in selector and default target/login, use:

```yaml
version: 1
hooks:
  select_backend: builtin.select_backend.passthrough
routing:
  default_backend:
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

With one configured target/login pair, the built-in selector infers that pair and
`routing.default_backend` can be omitted. Custom Rust selector code is compiled into
the host application. Rebuild after changing that code and restart the process after
changing its configuration.

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
target/login pairs. `routing.default_backend` names a target and login for the built-in
passthrough selector when multiple pairs make the choice ambiguous. Custom Rust
selectors are registered by the host application; their configured name is
`hooks.select_backend`, which defaults to `builtin.select_backend.passthrough`.
The repository host currently registers the built-in passthrough selector. It rejects
selector names it does not register; a consuming host maps its own names to its
compiled callbacks.

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

`routing.default_backend` identifies a configured target/login pair when the built-in
passthrough selector needs to resolve multiple choices. A custom selector can choose
among configured pairs using host-owned state. The name in `hooks.select_backend`
defaults to `builtin.select_backend.passthrough`; the host maps names to compiled
callbacks at startup. The `config/example.yml` stays intentionally minimal.

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

The library exposes `yasp::hooks::SelectionRequest`, `BackendSelection`,
`SelectionError`, and `bind_select_backend` for host-supplied Rust selectors.
`SelectionRequest` carries borrowed authenticated-user, client-ID, query-ID, and
`ParsedQuery` values; it contains no backend credentials. Rust selectors can inspect
the borrowed statement tree through `request.query.statement()`. A returned pair is
validated against configuration before the router acquires a connection. The gateway
supports simple-query requests; client transactions, prepared statements, PostgreSQL
cancel requests, `SET`, binary results, and Unicode escaped identifiers (`U&"..."`)
are unsupported. A frontend disconnect aborts that connection's active backend work.

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
