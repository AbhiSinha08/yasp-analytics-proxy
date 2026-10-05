# Development setup

## Current repository

The repository is a single Cargo package with no external dependencies. Its binary
prints a scaffold status message. The configuration and callback examples document
planned interfaces; runtime services are not contacted.

```sh
cargo check --all-targets
cargo run
```

The scaffold uses Rust 1.85.1 and edition 2024. The package minimum is Rust 1.85.
Phase 1 requires a compiler supported by its selected dependencies: the published
pgwire 0.41.0 manifest requires Rust 1.89. Keep the toolchain pin, package minimum,
and application lockfile consistent with the selected versions.

Formatting and lint checks require rustfmt and Clippy:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Tests cover component behavior and bugs, rather than duplicating implementation
logic. The scaffold has no runtime behavior to exercise with integration tests.

## Phase 1 prerequisites

| Component | Requirement |
| --- | --- |
| Rust | rustup-managed supported compiler, rustfmt, and Clippy. |
| Python | Python 3.12 with headers and a shared library for PyO3 embedding. |
| PostgreSQL | Running instance with dedicated test databases and least-privileged logins. |
| Redis | Reachable instance with a dedicated namespace, memory cap, and eviction policy. |
| Native build tools | C compiler, Make, and pkg-config; additional tools only if selected crate features require them. |
| Metabase | Supplied Docker-hosted instance, with a recorded version and reachable endpoint. |

The supplied container is the Metabase compatibility environment. Host Java is not
required for the official Docker image; its runtime is inside the container. Docker
is needed on the machine hosting Metabase, not necessarily in the YASP workspace.
Local Compose environments are optional when isolated service tests need them.

## Metabase connectivity

The Metabase host, published web port, and container networking details are pending.
Record them when supplied. Do not invent an endpoint or start a duplicate instance.
The web endpoint and the database/proxy endpoint are different connections.

Metabase must reach the YASP listener from inside its container. Container localhost
normally refers to the container itself. Choose a reachable proxy address and bind
interface for the actual deployment, with TLS and authentication for non-local access.
The configuration example's localhost address is a local-development default.

Use one Metabase database connection configuration with shared proxy credentials.
Provide representative BI metadata and the local RBAC policy/custom selection script
so successive queries exercise different source-role connections. Disable
write-oriented features for the read-only first phase.
The Metabase application database stores its own metadata and is separate from the
analytics databases routed through YASP.

## Python embedding

Set PYO3_PYTHON to the executable of the selected Python 3.12 environment. Verify
Python.h, the shared libpython library, dynamic linking, and standard-library imports
with the embedding integration. Missing pkg-config metadata alone does not establish
that Python needs reinstalling.

The no-op hook example uses only the standard library. Use a virtual environment
when external hook dependencies are needed. Python callbacks are trusted code;
embedded execution does not isolate them from the application.

## Configuration and secrets

- config/example.yml documents the Phase 1 configuration, including the frontend
  protocol and backend engine. It is not loaded by the current scaffold.
- config/local.yml is ignored by version control and can hold local non-secret
  settings when the loader is available.
- Fields ending in _env name secret environment variables. The BI service uses one
  SCRAM verifier; each backend login profile references its source-role password.
- The YAML contains RBAC mapping data and names a custom selection callback. The
  callback chooses a configured target/login from extracted metadata. Source roles
  own the database privileges; no per-role Metabase connection is needed.
- Configure a restricted role for discovery/setup operations without user metadata.
  Other missing or unmapped role-selection metadata denies the query.
- Service tests use dedicated databases and a unique Redis namespace. Provisioning
  and teardown scripts accompany the tests that need them.
- Non-local deployment requires frontend certificates and verified backend TLS.

## Library integration and custom code

The Rust library supplies core engine capabilities. The repository binary is a host;
other applications can depend on the library and supply their own engine setup.
During implementation, extension contracts accept an application-defined context
provider, role selector, and optional query/result/lifecycle hooks.

Keep custom Rust implementations in the consuming application or a separate
organization crate and compile them with the application. Register Python files as
application resources at engine startup. Configuration supplies policy data and
credentials; organization code interprets policy through the registered contracts.
No engine construction API is implemented in the scaffold.

## Later-phase tools

Frontend build tools belong to the management UI phase. Warehouse SDKs belong to
their backend adapters. Cloud SDKs and model-provider accounts belong to their own
integration phases. Maturin is not needed to embed Python in a Rust binary.

CI runs formatting, Clippy, meaningful Rust/Python checks, and isolated service
integration tests as those components become executable.

## References

- [pgwire toolchain requirement](https://docs.rs/crate/pgwire/0.41.0/source/Cargo.toml)
- [PyO3 embedding requirements](https://github.com/PyO3/pyo3)
- [Metabase Docker deployment](https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker)
