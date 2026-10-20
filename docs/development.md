# Development

Use the [documentation index](README.md) for current architecture, configuration,
and extension contracts. The [plan](PLAN.md) defines phase scope; implementation
proceeds only through requested slices.

The [development containers](../dev/README.md) cover the BI tools and Snowflake
emulator, including host-service prerequisites, ports, and persistent state.

## Toolchain and checks

Use Rust 1.89.0 with rustfmt and Clippy as pinned in
[rust-toolchain.toml](../rust-toolchain.toml). Run service-backed checks in a
Linux environment. Verify the compiler selected by the shell; a system Cargo
installation does not automatically honor rustup toolchain files.

```sh
rustup toolchain install 1.89.0 --profile minimal --component rustfmt --component clippy
rustc --version
cargo check --locked --all-targets
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
python3 scripts/test.py
# Includes optional Snowflake/client fixtures and Python/docs:
python3 scripts/test.py --full
```

The runner provisions disposable PostgreSQL 18 and Redis instances and executes
`cargo test --locked --all-targets`. It needs an unprivileged Linux user,
PostgreSQL 18 server tools plus psql/libpq, a TLS-capable Redis server/CLI,
OpenSSL, and Python 3.
`YASP_TEST_PG_BIN` can select the PostgreSQL binary directory. It cleans up its own
services and files; it does not replace an existing developer service. Full setup,
configured-service mode, and focused runs are in [tests/README.md](../tests/README.md).
Run feature combinations sequentially when sharing a Cargo target directory:
host integration tests launch the same output binary path.

```sh
cargo test --locked --lib config
python3 scripts/test.py -- --all-targets --features python
python3 scripts/test.py -- --doc
```

The Python feature needs the matching interpreter/shared-library environment below.
Run checks appropriate to the changed observable contract; broaden testing when
failures or cross-component behavior require it. Preserve unrelated checkout edits
and never kill unknown services to free a port.

## Rust structure and testing norms

Keep one library plus a thin binary host. Prefer existing code, the standard
library, and installed crates. Apply these Rust conventions:

- Group modules and helpers by responsibility. Split large orchestration functions
  into named stages; avoid catch-all utility modules or directories for tiny helpers.
- Use the least visibility needed, usually `pub(super)` or `pub(crate)`. Preserve
  public paths through explicit exports in short component facades.
- Borrow request data and return owned, validated decisions. Use `Arc` for shared
  process state rather than copying policy or credentials per request.
- Use typed library errors; adapters map them into their native protocol. Use
  `expect` only for established engine invariants, never unchecked client data.
- Comments explain purpose, stage order, bounds, and cancellation/drop constraints.
  Treat rustfmt and Clippy as the formatting and lint authorities.

Core owns bounds, adapters, execution, validation, cache mechanics, and callback
invocation. Hosts own secrets, registration names/params, context interpretation,
and organization policy. Keep driver/dialect/native-value/session/error behavior
inside the appropriate adapter. Do not add speculative registries, runtime Rust
compilation, generic frameworks, or dependencies for future phases.

Preserve data fidelity, cancellation, timeout and cleanup behavior, validation order,
and redacted errors during refactors. Existing tests should pass unchanged when
observable behavior is unchanged. Prefer integration scenarios and focused
regressions for access isolation, protocol/value compatibility, and recovery.
Avoid tests that mirror trivial helpers or module layout.

For large functionality changes, update the relevant component documentation and
Mermaid flow diagrams together. Keep the [index](README.md),
[architecture](architecture.md), [configuration](configuration.md), and
[extensions](extensions.md) consistent with actual behavior. Mark future stages
explicitly and keep product direction in the plan.

## Installed dependency responsibilities

[Cargo.toml](../Cargo.toml) selects features; [Cargo.lock](../Cargo.lock) pins the host
application's resolved versions. Library consumers resolve their own graph.

| Direct dependency family                     | Current purpose                                                                                                         |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Tokio 1.53                                   | Runtime, networking, queues, deadlines, and signals; selected features rather than `full`.                              |
| pgwire 0.41                                  | PostgreSQL frontend and backend protocol with `server-api-ring` and `client-api-ring`; native text values and metadata. |
| deadpool 0.13                                | Managed bounded PostgreSQL connection pools with Tokio runtime support.                                                 |
| sqlparser 0.63 with `visitor`                | Bounded PostgreSQL AST inspection; syntax checks do not replace database authorization.                                 |
| Serde 1, serde-saphyr 1.3, serde_json 1      | Strict typed YAML configuration and bounded JSON hook/context values. JSON is not the database result codec.            |
| thiserror 2                                  | Typed library error categories.                                                                                         |
| tracing 0.1, tracing-subscriber 0.3          | Library events and host-installed stdout filtering.                                                                     |
| futures 0.3, async-trait 0.1, tokio-util 0.7 | Streaming protocol handlers and framed asynchronous I/O.                                                                |
| dotenvy 0.15                                 | Host-only `.env` iterator parsing without environment mutation.                                                         |
| Redis 1.7                                    | Asynchronous multiplexed cache/dataset connections with `tokio-rustls-comp`.                                            |
| Rustls 0.23 and rustls-native-certs 0.8      | TLS 1.2/1.3, certificate/hostname verification, PEM parsing, and system trust roots; Ring matches pgwire.               |
| sha2 0.11, bytes 1                           | Query/cache scope hashes and bounded native protocol buffers.                                                           |
| PyO3 0.29, optional                          | Trusted embedded Python; Rust-only builds do not link CPython.                                                          |
| reqwest 0.12, jsonwebtoken 9, rsa 0.9, pkcs8 0.10, base64 0.22, uuid 1 and chrono 0.4, optional | Snowflake SQL API HTTPS, RSA key-pair JWTs, encrypted PEM loading, request IDs and exact temporal conversion; enabled by `snowflake`. |

Additional backend/model SDKs and management frameworks belong to
their active milestones. Choose compatible versions/features and prove native type,
transport, and resource behavior before claiming support. Preserve PostgreSQL numeric
text instead of introducing a fixed-precision conversion that loses values.

## Python embedding

Cargo builds PyO3; the application installs Python packages. Use one matching CPython
minor version/architecture with its shared library, standard library, and application
packages. The supplied [requirements.txt](../hooks/requirements.txt) has no third-party
requirements. Pin application dependencies before deployment; no package installation
or module reload happens while serving.

Choose the interpreter explicitly when several installations exist. For pyenv,
`YASP_PYTHON="$(pyenv which python3.12)"` selects its installed CPython 3.12; the
default below also follows the shell's `python3.12` selection.

```sh
YASP_PYTHON="${YASP_PYTHON:-python3.12}"
"$YASP_PYTHON" -m venv .venv
.venv/bin/python -m pip install --require-hashes -r hooks/requirements.txt
.venv/bin/python -m pip check
PYO3_PYTHON="$PWD/.venv/bin/python" cargo build --locked --features python
```

`PYO3_PYTHON` selects build/link settings. A venv activation alone does not configure
embedded imports, and a venv does not contain a portable base interpreter. On Linux,
expose a nonstandard shared-library directory to the runtime loader:

```sh
YASP_PYTHON_LIB=$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')
export PYO3_PYTHON="$PWD/.venv/bin/python"
export LD_LIBRARY_PATH="$YASP_PYTHON_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
python3 scripts/test.py -- --all-targets --features python
```

The callback runtime initializes CPython in isolated mode with site imports disabled.
It ignores PYTHONPATH and adds only explicit `hooks.python.search_paths` to base
standard-library paths. Include required application helper and purelib/platlib
site-package directories there; paths in YAML resolve relative to that YAML file.
Do not set PYTHONHOME to a venv. Packages relying on automatic `.pth` startup actions
need explicit deployment packaging. Missing imports and registrations fail startup.

The optional import probe uses ordinary import paths and only tests initialization
and imports, not callback execution:

```sh
YASP_PYTHON_SITE=$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')
PYTHONPATH="$YASP_PYTHON_SITE:$PWD/hooks/examples" PYTHONNOUSERSITE=1 \
cargo run --locked --features python --example python_environment -- passthrough
```

Let `PythonRuntime` initialize the interpreter once; an existing interpreter or second
runtime is rejected. Clone it for more callbacks and never finalize it while they
exist. All callbacks share package versions, `sys.modules`, and interpreter state.
Timed-out executing code retains capacity; process replacement is needed for stuck
trusted code, conflicting environments, or enforced termination. See
[extension limits](extensions.md#external-python) and [configuration](configuration.md).
