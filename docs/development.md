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

| Dependency | Status and purpose | Constraints and alternatives |
| --- | --- | --- |
| Tokio 1.53 | Added: async runtime, networking, queues, timeouts, shutdown. | Enable the used feature families instead of `full`; Python work stays off async workers. |
| Serde 1 + serde-saphyr 1.3 | Added: typed YAML configuration deserialization. | serde_yaml is archived/unmaintained. serde-saphyr provides duplicate-key rejection and parsing budgets; keep budgets enabled and reject unknown configuration fields. Serialization/includes/interpolation are disabled. TOML + the toml crate is a reasonable alternative only if the YAML requirement changes. |
| thiserror 2 | Added: typed library errors. | Keep recoverable error categories; add no generic error framework to the library. |
| tracing 0.1 + tracing-subscriber 0.3 | Added: structured events and host log filtering/output. | The library emits events; the host alone installs the subscriber. Redact query data and secrets. |
| PyO3 0.29 | Added behind the `python` feature: embed CPython for trusted callbacks. | No extension-module, abi3, auto-initialize, or unused conversion features. Initialize explicitly after configuring imports. Rust-only applications do not link Python. |
| pgwire 0.41 | Added with `server-api-ring` and `client-api-ring`: frontend protocol handling and native PostgreSQL backend client. | Streams text rows and PostgreSQL metadata without converting values through a driver value model. The guarded frontend transport checks the 8 MiB frame bound before payload allocation. |
| dotenvy 0.15 | Added: host-only `.env` parsing. | Iterator API avoids process environment mutation; parse before runtime startup and redact parser diagnostics. |
| futures 0.3 + async-trait 0.1 + tokio-util 0.7 | Added: asynchronous protocol handlers and framed transport. | Uses pgwire streaming APIs; no separate query framework. |
| deadpool 0.13 (`managed`, `rt_tokio_1`) | Added: bounded PostgreSQL backend pool. | Uses the managed pool API; `deadpool-postgres` and `tokio-postgres` are not installed. |
| rustls + tokio-rustls; compatible PostgreSQL TLS connector | Deferred to transport implementation. | Frontend TLS does not secure backend connections. Validate CA roots and hostnames on both database and Redis transports. Select compatible versions and one crypto provider with pgwire; avoid duplicate providers. |
| sqlparser 0.63 (`visitor`) | Added: bounded PostgreSQL-dialect syntax inspection. | The parser recursion budget is 64; this is not a limit of 64 SQL nesting levels. Parsing is syntax inspection, not semantic authorization. |
| redis 1.7 | Deferred until cache eligibility and serialization exist. | Async Tokio support and a multiplexed connection are sufficient initially; no separate pool/cluster/cache framework. Enable verified TLS when deployed off-host. |
| Axum 0.8 | Deferred to Phase 2 management APIs. | Fits Tokio. Add authentication/authorization and request limits before exposing configuration or execution. No UI tooling is needed now. |
| Warehouse/model SDKs | Deferred to the chosen Phase 2 adapters. | Choose an actual backend/model first. Reuse an existing HTTP client when sufficient; keep vendor SDKs in the consuming application's model adapter or the database adapter. |

NUMERIC conversion needs special attention: rust_decimal's finite precision is not
enough for every PostgreSQL numeric. Preserve native values or use a proven lossless
codec; do not enable a decimal integration and infer full PostgreSQL type support.

## Phase 1 prerequisites

| Component | Requirement |
| --- | --- |
| Rust | Supported compiler, rustfmt, and Clippy. |
| Python, when enabled | CPython 3.12 with a shared libpython and a compatible standard library; verify the selected installation's development files. |
| PostgreSQL | Dedicated test databases and non-owner, non-superuser, least-privileged logins without BYPASSRLS. |
| Redis, when caching is implemented | Dedicated namespace/ACL, memory cap, eviction policy, and bounded client timeouts. |
| Native build tools | C compiler and linker; additional tools only when selected crate features require them. |
| Metabase | Supplied Docker-hosted instance, with its version and reachable endpoint recorded. |

The supplied container is the Metabase compatibility environment. Host Java is not
required for its official Docker image. Docker is needed on the host running
Metabase, not necessarily in the YASP workspace. No duplicate instance is needed.

## Metabase connectivity

Record the Metabase version, endpoint, networking details, and representative query
metadata when configuring the integration. Metabase's
web endpoint and the database/proxy endpoint are different connections; container
localhost normally means the container itself. Select a reachable proxy address,
with TLS and authentication for non-local access.

Use one BI database connection with shared proxy credentials. Supply the integration's
context provider, RBAC data, and role selector. Application-defined metadata such as
`group` is not a built-in Metabase metadata guarantee. Native SQL authors must
not be able to select a stronger role by forging a comment. If trustworthy routing
context is unavailable, the prototype cannot claim viewer isolation.

Disable BI result caching and write-oriented features initially. Test saved questions,
query-builder/native queries, dashboard sharing, subscriptions, downloads, and
background scans under the actual BI permission model. Shared discovery/field scans
must use a role whose exposed metadata and sampled values are safe to share. The
Metabase application database is separate from the analytics databases YASP routes.

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

The host loads the ignored `config/local.yml` by default. Create it with only
`version: 1` and one target/login from the full planned reference in
`config/example.yml`. Copy the environment template for local secrets:

```sh
cp .env.example .env
```

Use `--config PATH` to select another YAML file. Runtime YAML contains `version: 1`
and one PostgreSQL target with one login (`engine: postgresql`), under the existing
`targets.<target>.logins.<login>` structure. Configure the target host, port, actual
PostgreSQL database name, and `tls_mode: disable`; give its login a username and a
`password_env` key naming the environment variable that holds its password. The
runtime accepts exactly one target and one login and rejects unsupported engines or
TLS modes. Additional configured targets or logins are not silently ignored or
selected; per-query routing is not implemented.
The loader rejects unknown fields, duplicate keys, invalid types, and YAML files
larger than 64 KiB.

Gateway credentials are supplied only through `YASP_GATEWAY_USERNAME`,
`YASP_GATEWAY_PASSWORD`, and `YASP_GATEWAY_DATABASE` in the process environment or
optional `.env`. The gateway database value is the frontend connection label;
`targets.<target>.database` selects the actual PostgreSQL database. The target login
password comes from its named `password_env` variable. Process environment values
take precedence over `.env`; the file is optional when all required variables
are already supplied. Target connection settings come from YAML.

The listener and backend must use loopback addresses. The listener defaults to
`127.0.0.1:6432`; TLS is not available in this milestone. The same existing target
and login structure is used by `tests/config.yml`.

The current limits are: 32 frontend sessions, 8 MiB per frontend or backend frame
including its header, 1 MiB SQL text, 4,096 significant SQL tokens, and a parser
recursion limit of 64; eight backend pool connections; 10 seconds each for backend
connect/acquire/cleanup; and 60 seconds each for backend query waits and frontend
writes. Startup is limited to 120 seconds and shutdown to 10 seconds. Runtime sets
UTF8 client encoding, `DateStyle = ISO, MDY`, UTC timezone, `IntervalStyle = postgres`,
and the default `bytea_output = hex`. Each query runs in a read-only transaction.

For tests, `tests/config.yml` uses the same version/target/login structure and sets
the target database to the `postgres` maintenance database. The harness creates a
fresh test database from `template0`, runs the proxy against it, and drops only that
database during teardown. The test target password uses the named variable
`YASP_TEST_TARGET_PASSWORD`.

`BackendConfig::new(host, port, username, password, database)` configures the
PostgreSQL backend, and `PostgresBackend::new(config)` constructs its pool.
`gateway::serve(listener, config, Arc<PostgresBackend>, shutdown)` serves through
a host-owned loopback listener. `ParsedQuery` retains the original SQL text and an
immutable PostgreSQL statement tree. `statement()` provides borrowed read-only
statement access for future Rust hooks. Python hooks are not implemented. The gateway
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
- [Metabase result caching](https://www.metabase.com/docs/latest/configuring-metabase/caching)
