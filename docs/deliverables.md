# Deliverables

## Phase 0 — Project foundation

- [x] **Rust package and module boundaries** — Provides the host binary, reusable library, and component modules.
- [x] **Toolchain and foundation dependencies** — Pins Rust 1.89 and the dependencies used by the current gateway and PostgreSQL backend.
- [x] **Configuration and hook examples** — Documents intended settings and application callback contracts; runtime hook loading remains deferred.
- [x] **Python environment probe** — Checks embedded Python linking and imports without starting the proxy.
- [x] **Development and test guidance** — Documents runtime configuration, isolated test databases, and fixture ownership.

## Phase 1 — PostgreSQL analytics proxy (in progress)

- [x] **Simple-query PostgreSQL gateway** — Accepts loopback clients, authenticates with SCRAM, streams backend results, and reports PostgreSQL metadata.
  - [x] **Supported read-only SQL subset** — Handles SELECT, read-only WITH, subqueries, joins, aggregates, unions, catalog queries, and SHOW variable/ALL requests.
  - [x] **Parsed-query inspection** — Preserves original SQL and a PostgreSQL AST for read-only inspection and future hook use; hooks are not implemented.
  - [x] **Bounded protocol and SQL input** — Enforces the 8 MiB frontend/backend frame bound before payload allocation, and bounds SQL to 1 MiB and 4,096 significant tokens, with parser recursion budget 64.
- [x] **Single PostgreSQL backend and pool** — Uses one configured backend with bounded pool capacity and connect/acquire/query/cleanup timeouts.
  - [x] **Read-only execution and lease cleanup** — Runs each query in a read-only transaction, verifies rollback and DISCARD ALL cleanup, and discards interrupted or unhealthy connections and connections with changed protected text parameters.
  - [x] **Runtime YAML connection configuration** — Loads one PostgreSQL target/login from `config/local.yml` or `--config PATH`; gateway credentials and the named target password come from the process environment or optional `.env`.
  - [x] **Isolated database test fixture** — Creates a fresh database from `template0`, loads synthetic test data, and drops the created database during teardown; requires a test login with `CREATEDB`.
- [x] **Milestone validation** — Rust formatting, all-feature Clippy, and the live PostgreSQL suite pass with the canonical target/login configuration and an isolated test database.
- [ ] **Client transactions, cancellation, prepared statements, SET, and binary formats** — Deferred beyond the simple-query milestone.
- [ ] **Metabase integration and trusted routing metadata** — Requires compatibility validation against the supplied Metabase instance.
- [ ] **Source RBAC and per-query routing** — Requires application policy callbacks and multiple configured role/target connections.
- [ ] **Result processing, hooks, and Redis cache** — Requires bounded extension invocation and authorized result/cache contracts.
- [ ] **TLS, full multi-target/policy YAML configuration, operations, and end-to-end acceptance**.

## Phase 2 — Additional engines, dynamic masking, and management

- [ ] **Warehouse adapter** — Adds one selected engine with its own execution, metadata, type, and session behavior.
- [ ] **Gateway and dialect support** — Defines supported client/backend combinations and handles only an explicit SQL subset.
- [ ] **Shared execution contract** — Dispatches to multiple adapters while keeping routing and policy independent of drivers.
- [ ] **Decision model and dynamic masking** — Classifies bounded result data and applies schema-preserving masks before results are sent.
- [ ] **Management API** — Provides authenticated configuration inspection, health, and scoped cache controls.
- [ ] **Admin UI and SQL Studio** — Manages mappings and runs queries through the same authorized execution path.
- [ ] **Configuration generations** — Activates validated settings consistently and retires affected pools or processes safely.

## Phase 3 — Advanced analytics and integrations

- [ ] **Compute lifecycle** — Coordinates provider-specific provisioning, suspension, readiness, and recovery.
- [ ] **Governance integration** — Adds verified end-user context and external policy checks with privacy controls.
- [ ] **Cache evolution** — Adds metadata caching and reliable invalidation across data versions and targets.
- [ ] **Semantic caching** — Reuses compatible result supersets while preserving SQL ordering, filtering, NULL, limit, and aggregate semantics.
- [ ] **Federation** — Plans and runs cross-engine queries with explicit type, consistency, and failure behavior.
- [ ] **Natural-language queries** — Adds model-assisted query creation through the existing authorized execution path.
