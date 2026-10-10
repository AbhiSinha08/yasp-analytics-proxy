# Deliverables

## Phase 0 — Project foundation

- [x] **Rust package and module boundaries** — Provides the host binary, reusable library, and component modules.
- [x] **Toolchain and foundation dependencies** — Pins Rust 1.89 and the dependencies used by the current gateway and PostgreSQL backend.
- [x] **Configuration and hook setup** — Explains how to configure database connections and choose the code that selects one for each query.
- [x] **Python environment probe** — Checks embedded Python linking and imports without starting the proxy.
- [x] **Development and test guidance** — Documents runtime configuration, isolated test databases, and fixture ownership.

## Phase 1 — PostgreSQL analytics proxy (in progress)

- [x] **Simple-query PostgreSQL gateway** — Accepts loopback clients, authenticates with SCRAM, streams backend results, and reports PostgreSQL metadata.
  - [x] **Supported read-only SQL subset** — Handles SELECT, read-only WITH, subqueries, joins, aggregates, unions, catalog queries, and SHOW variable/ALL requests.
  - [x] **Parsed-query inspection** — Keeps the original SQL and its parsed form available to routing code.
  - [x] **Bounded protocol and SQL input** — Enforces the configurable frontend frame bound before payload allocation (8 MiB by default), the fixed 8 MiB backend frame bound, and SQL/token/parser recursion limits.
- [x] **PostgreSQL connections and pools** — Connects to configured databases and logins, with limits on open connections.
  - [x] **Read-only execution and lease cleanup** — Runs each query in a read-only transaction, verifies rollback and DISCARD ALL cleanup, and discards interrupted or unhealthy connections and connections with changed protected text parameters.
  - [x] **Runtime YAML connection configuration** — Loads database connections and logins from `config/local.yml` or the file named with `--config`; passwords come from environment variables.
  - [x] **Configurable gateway limits and listener** — Supports the optional PostgreSQL-only gateway block for a loopback listener and session/frame/SQL/write limits; omitted values use defaults. `local_development` is plaintext; real TLS is deferred.
  - [x] **Gateway timeout policy** — Configures query, client-message read, write, startup, and shutdown deadlines. PostgreSQL enforces transaction-local query timers; pool lifecycle limits remain separate.
  - [x] **Isolated database test fixture** — Creates a fresh database from `template0`, loads synthetic test data, and drops the created database during teardown; requires a test login with `CREATEDB`.
- [x] **Shared stdout logging** — The reusable logger reads `LOG_LEVEL` (`INFO` by default; `DEBUG`, `INFO`, `WARN`, and `ERROR` are accepted). Query receive and backend dispatch logs include query ID, client ID, backend target, database user, and SQL at DEBUG; client connection changes are logged at INFO and failures at ERROR.
- [x] **Choose a database login for each query** — Lets the application select which configured database connection handles a query.
  - [x] **Built-in and application selectors** — Includes a default selector and lets an application register its own compiled selector.
  - [x] **Check selected connections** — Allows only database/login pairs listed in the configuration. The built-in selector needs a default when there is more than one choice.
  - [x] **Connection pools for each choice** — Opens connections when needed, with a default limit of 8 per choice and 16 total. Startup reports limits that cannot fit the configured choices.
  - [x] **Test query routing** — Tests routing to two databases, separate connection pools, pool limits, selector errors, and recovery.
  - [x] **Test source-login access** — Checks that `local2` can read granted test data but is denied access to another database and a restricted table, then verifies the client can query again. Test usernames are in YAML; passwords are in environment variables or `.env`.
- [ ] **Python backend selector** — Add a selector written in Python. Its execution limits and runtime behavior need separate design.
- [ ] **PostgreSQL cancellation** — Let clients cancel a query that is still running.
- [ ] **BI client context and compatibility** — Check what identity information the BI client provides and test its connection and queries.
- [ ] **Source access rules (RBAC)** — Let the application apply its own access rules when choosing a database login, and validate them with the intended BI client.
- [ ] **Result processing, hooks, and Redis cache** — Requires bounded extension invocation and authorized result/cache contracts.
- [ ] **TLS, operations, and end-to-end testing** — Add encrypted connections and verify the full setup works.
- [ ] **Remaining BI client compatibility** — Do these last in Phase 1; move an item earlier if client testing requires it.
  - [ ] **Prepared queries and basic portals** — Let clients prepare a query once and run it later.
  - [ ] **Limited `SET` support** — Support selected session settings requested by BI clients.
  - [ ] **Client transactions and resumable results** — Keep related queries in one transaction and let clients pause and continue receiving results.
  - [ ] **Binary data formats** — Support PostgreSQL's compact binary representation for query values and results when clients request it.

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
