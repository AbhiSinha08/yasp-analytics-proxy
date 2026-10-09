# YASP

**Yet Another SQL Proxy**

YASP is a reusable SQL proxy engine for analytics across multiple database engines,
with authenticated routing, database-backed access controls, result masking,
trusted Python hooks, and query-result caching.

The first phase supports PostgreSQL clients and backends, with Metabase as the
first BI compatibility target. A custom callback selects source-role connections
from query metadata and a local RBAC policy behind one BI connection configuration.
Phase 2 adds database adapters, decision-model dynamic masking, and management
features around the shared routing, policy, hook, and cache components.

Applications use the Rust library with their own context providers and hooks.
Organization-specific customization stays outside core engine modules and is
registered when constructing the engine.

The repository currently contains a Rust scaffold with foundation dependencies
and optional PyO3 embedding support. The binary prints a status message; runtime
components are not implemented. Rust 1.89.0 is pinned for reproducible builds.

```sh
cargo check --locked --all-targets
cargo run --locked
```

- [Phases and components](docs/PLAN.md)
- [Development setup](docs/development.md)
- [Configuration Design](config/example.yml)
- [Python callback design](hooks/README.md)
- [Test strategy](tests/README.md)
