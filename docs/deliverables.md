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
  - [x] **Connection pools for each choice** — Opens connections when needed, with a default limit of 8 per choice and 16 total. Shared physical budgets bound actual open connections across choices.
  - [x] **Test query routing** — Tests routing to two databases, separate connection pools, pool limits, selector errors, and recovery.
  - [x] **Test source-login access** — Checks that `local2` can read granted test data but is denied access to another database and a restricted table, then verifies the client can query again. Test usernames are in YAML; passwords are in environment variables or `.env`.
- [x] **Python backend selector** — Execute application-supplied Python callbacks through the optional reusable runtime.
  - [x] **External callback platform** — Registers file/function references with application params, isolated imports, persistent modules, and fresh invocation data.
  - [x] **Bounded dedicated worker** — Bounds queue count, input bytes, payloads, nesting, and waiting time; retains executing work after timeout/cancellation and stops admission on shutdown.
  - [x] **Selector adapter and configuration** — Supports Python references alongside compiled Rust names, validates selected pairs, and preserves cancellation and session recovery.
  - [x] **External passthrough example** — Supplies a Python example under hooks/examples and configures ignored local.yml for manual use.
  - [x] **Runtime and routing verification** — Exercises arbitrary callbacks without a database, routed source logins, error recovery, and manual current_user selection.
- [x] **PostgreSQL cancellation** — Handle client cancel requests and Forward backend cancellation safely.
- [x] **Redis result cache** — Adds an opt-in shared cache for complete read-only PostgreSQL results.
  - [x] **Cache settings and bounds** — Keeps caching disabled by default and limits cached rows and aggregate in-process buffers.
  - [x] **SQL-text cache eligibility** — Lets Rust or Python callbacks bypass freshness-sensitive queries using case-insensitive literal SQL fragments; normalized query hashes and configured target/login names identify cache entries.
- [x] **Shared application datasets** — Loads bounded JSON datasets for consuming-host callbacks.
  - [x] **Loader registration and startup lifecycle** — Bind Rust/Python loaders, share concurrent loads, and fail startup when initial loading fails.
  - [x] **Separate Redis registry and bounded lookup API** — Read current shared values with bounded `get`, `get_many`, and full-dataset operations.
  - [x] **Scheduled refresh and recovery** — Use lease/revision protection and retain prior values after refresh errors.
  - [x] **Examples and documentation** — Provide JSON, Python, and Rust loader examples plus dataset-aware context and selection examples.
- [ ] **BI client context and compatibility** — Supply application context and verify the intended clients' metadata, connections, and queries.
  - [x] **Context hook platform** — Derives bounded application context once per validated query with async Rust or external Python callbacks.
  - [x] **Context propagation** — Makes the same context available to backend selection and cache eligibility without changing authenticated frontend identity.
  - [x] **Placeholder extractors** — Provides named Metabase, Superset, and Redash helper functions that report they are not implemented.
  - [ ] **Real BI metadata extractors** — Implement and validate extraction against observed client metadata; no extractor is enabled by default.
- [x] **Database schema discovery** — Route metadata to its configured source and verify client browsing.
  - [x] **Discovery route and inference** — Resolves a configured target/login, preserves the first declared login, and bypasses callbacks and Redis.
  - [x] **PostgreSQL metadata rules** — Recognizes bounded catalog reads, qualifies native names, and limits database listings to the current source.
  - [x] **Client setup and source fingerprint** — Handles bounded application name/discovery search path, fixed session defaults, and the discovery source's server version.
  - [x] **Routing and protocol checks** — Tests metadata/data separation, native results, settings, callback bypass, and pool cleanup with real PostgreSQL clients.
  - [x] **Live desktop-client browsing** — Verify VS Code Database Client and DBeaver UI refresh with autocommit and JDBC simple-query mode.
- [ ] **Source access rules (RBAC)** — Let the application apply its own access rules when choosing a database login, and validate them with the intended BI client.
  - [x] **Partial selector decisions** — Let the single selector omit unambiguous target/login fields and request an optional target-scoped compute type.
  - [ ] **BI policy validation** — Verify user isolation, missing context, source denial, and cached and uncached query behavior with the intended real BI client.
- [x] **Optional compute-type routing and overflow** — Uses [target-scoped compute routing](source-access-routing-plan.md) and bounded shared allocation.
  - [x] **Target-scoped compute configuration** — Define compute types, ordered instances, and supported login bindings without changing ordinary targets.
  - [x] **Shared connection budgets** — Bound actual open connections across pools and reclaim idle sessions when another binding needs capacity.
  - [x] **Type-local overflow and recovery** — Allocate within the selected type and verify shared capacity, cancellation, cleanup, discovery, and cache scope.
- [ ] **TLS, operations, and end-to-end testing** — Add encrypted connections and verify the full setup works.

## Phase 2 — Additional engines, dynamic masking, and management

- [ ] **Warehouse adapter** — Adds one selected engine with its own execution, metadata, type, and session behavior.
- [ ] **Gateway and dialect support** — Defines supported client/backend combinations and handles only an explicit SQL subset.
- [ ] **Decision model and dynamic masking** — Classifies bounded result data and applies schema-preserving masks before results are sent.
- [ ] **Query Modification Hook** - Modify query after context extraction and before cache eligibility and query ID generation
- [ ] **Result processing hooks** — Requires result-hook integrations and authorized result/cache contracts.
- [ ] **Backend Database Schema Caching**
- [ ] **AST Based Dynamic Masking**
- [ ] **Redis query queue** — Add bounded shared admission for burst workloads, with deadlines, cancellation, worker ownership, and recovery. Current acquisition waits asynchronously for `backend.acquire_timeout_ms` (60 seconds by default), then returns `53300`.
- [ ] **Admin UI and SQL Studio** — Manages mappings and runs queries through the same authorized execution path.
- [ ] **Configuration generations** — Activates validated settings consistently and retires affected pools or processes safely.
- [ ] **Git Backed config Management in UI**

## Phase 3 — Advanced analytics and integrations

- [ ] **Benchmarking Framework**
- [ ] **Additional load-distribution algorithms** — Add round-robin and least-busy allocation within a selected compute type, preserving access, capacity, and cancellation bounds.
- [ ] **Compute lifecycle** — Coordinates provider-specific provisioning, suspension, readiness, and recovery.
- [ ] **Remaining BI client compatibility** — Prepared queries, Portals, limited SET support, resumable results, Binary Data formats
- [ ] **Governance integration in UI** — Adds verified end-user context and external policy checks with privacy controls.
- [ ] **Management API** — Provides authenticated configuration inspection, health, and scoped cache controls, Dataset refreshes, edits.
- [ ] **Semantic caching** — Reuses compatible result supersets while preserving SQL ordering, filtering, NULL, limit, and aggregate semantics.
- [ ] **Federation** — Plans and runs cross-engine queries with explicit type, consistency, and failure behavior.
- [ ] **Natural-language queries** — Adds model-assisted query creation through the existing authorized execution path.
