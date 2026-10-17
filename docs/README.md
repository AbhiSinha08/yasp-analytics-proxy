# YASP documentation

YASP is a reusable SQL proxy library and a local host application. The current
runtime supports a bounded PostgreSQL simple-query subset, configured source-role
routing, application context hooks, shared JSON datasets, metadata discovery, and an
optional Redis cache, with optional verified TLS for PostgreSQL and Redis.

| Read                                                               | For                                                                                      |
| ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| [Architecture](architecture.md)                                    | Current components, ownership, query flow, sessions, cancellation, and caching.          |
| [Configuration](configuration.md)                                  | Runtime settings, secrets, defaults, pools, discovery, and desktop-client setup.         |
| [TLS setup](tls.md)                                                | Certificates, encrypted client/source/Redis connections, and library application setup.  |
| [Extensions](extensions.md)                                        | Current Rust/Python contracts and boundaries for future engines, hooks, RBAC, and UI.    |
| [Development](development.md)                                      | Toolchain, dependencies, Rust structure, and verification commands.                      |
| [Hook examples](../hooks/README.md)                                | Binding callbacks and choosing runnable examples.                                        |
| [Tests](../tests/README.md)                                        | Isolated test services, fixtures, requirements, and Cargo filters.                       |
| [Plan](PLAN.md)                                                    | Product direction, phase boundaries, and planned capabilities.                           |
| [Source access and compute routing](source-access-routing-plan.md) | Partial selector decisions, compute types, overflow, shared budgets, and future queuing. |

Architecture and configuration describe implemented behavior. The plan and the
explicitly marked future sections in the extension guide describe the intended
end state. Prepared statements, client transactions, binary results, general query
rewrites, result hooks, engine RBAC interpretation, and management UI remain
future capabilities.
