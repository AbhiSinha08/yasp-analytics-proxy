# YASP — Yet Another SQL Proxy

## Product goal

YASP is a SQL proxy for analytics workloads across multiple database engines.
It sits between BI clients and backend databases, providing authenticated routing,
access-control integration, query hooks, result masking, and caching.

The reusable Rust library owns connections, query execution, and bounded data
processing. A consuming application supplies its organization's context derivation,
role-selection policy, and hooks before constructing the engine. Rust extensions
compile into that application; registered Python callbacks provide script-based
customization and participate in query execution when used.

PostgreSQL is the first client protocol and backend engine. The product architecture
supports adding other database adapters without rewriting shared components.
Client protocol support and backend engine support are separate capabilities:
adding a warehouse backend does not automatically add its native client protocol
or translate all PostgreSQL SQL into that warehouse's dialect.

## Components and boundaries

Start with one Rust package containing a binary and a library. Each component has
one clear responsibility. Keep database-specific behavior inside adapters, and
introduce only the contracts needed by the current phase.

| Component | Responsibility | Extension boundary |
| --- | --- | --- |
| Client gateway | Authenticate connections, manage client sessions, decode requests, and encode responses. | Client protocol adapters own wire messages, authentication exchanges, and response encoding. PostgreSQL is first. |
| Query processing | Preserve SQL, expose raw query metadata to the registered context provider, inspect statements, and validate rewrites. | Dialect-specific parsing and validation stay separate from routing and policy. |
| Backend execution | Acquire a connection, execute a request, stream results, cancel work, and clean up sessions. | Database adapters own drivers, pools, bind conversion, native types, session behavior, and error mapping. |
| Routing | Invoke the selection callback using query metadata and configured RBAC policy, then acquire the selected target/role connection. | Depends on target identity and supported capabilities, not driver-specific objects. |
| Policy | Supply the configured role-selection policy, validate callback decisions, and apply configured result masks. | Database access controls remain authoritative; masking preserves the result schema. |
| Extensions | Invoke application-supplied context derivation and policy/query/result/lifecycle hooks. | Contracts belong to the library; organization implementations live outside core engine code. |
| Result cache | Decide eligibility and store complete authorized results for a limited time. | Keys include backend/dialect identity, security context, parameters, and relevant session state. |
| Configuration and operations | Validate settings, resolve secrets, enforce limits, and report health and performance. | Shared configuration names the client protocol and each target's engine explicitly. |

The gateway passes execution requests to the backend boundary rather than opening
backend-driver connections itself. An execution request carries SQL and its dialect,
parameters, BI service identity, query metadata, selected backend role, target, and
session context. Results expose
column metadata, streamed bounded batches, completion status, and errors.

These contracts must preserve numeric precision, NULLs, timestamps, and native
metadata. The PostgreSQL adapter can retain PostgreSQL type identifiers and opaque
values where needed; there is no requirement to convert every value to JSON or to
invent a universal type system. Adapters declare supported behavior, and unsupported
combinations fail explicitly. Cancellation, transaction state, and cleanup belong
to the execution boundary, not scattered backend-specific checks in shared code.

The first implementation can use a concrete PostgreSQL adapter. Add a common trait
or dispatch mechanism when another adapter needs it. No dynamic plugin framework,
unused driver implementations, or runtime Rust compilation is required.

### Reusable library and organization extensions

The core engine and organization-specific customization are separate code. Any
organization or developer can depend on the Rust library and assemble a proxy
application with its own extensions rather than editing or forking engine modules.
The repository binary is a host entry point; library use does not require it.

**Core library:** protocol adapters, query inspection, backend adapters, pool and
session lifecycle, cache mechanics, resource limits, hook execution, and validation
of extension decisions. It defines narrow extension contracts and owns the engine
lifecycle. It has no hard-coded organization roles, tag formats, group mappings,
classification model, or result transformation rules.

**Application customization:** context derivation from SQL comments or other
available request/session metadata; source-role selection using local RBAC data;
query/result transformations; decision-model integration; and lifecycle callbacks.
These implementations, policy data, and script files live outside core modules.
The library's hook runtime invokes them; it does not contain their business rules.

The consuming application registers a context provider and the hooks it needs,
then passes them and validated configuration to engine construction. Rust providers
are compiled with that application's library dependency. Python modules are supplied
as application resources and registered before serving queries. Engine invocation
uses those registrations; it does not search for organization code inside src/.

```text
Organization application
  -> supply configuration and policy data
  -> register context derivation and Rust/Python hooks
  -> construct core engine
  -> authenticate request and expose available metadata
  -> derive organization context
  -> run query rewrite and role-selection hooks
  -> execute/cache through the core engine
  -> apply registered result processing and return response
```

Keep authenticated service identity and engine-owned session/security fields
separate from derived organization context. A context provider may enrich request
metadata but cannot replace authenticated identity or bypass configured connection
bounds. Shared cache logic consumes the selected role and declared decision scope;
it does not interpret organization-specific groups itself.

Define only extension contracts required by the active phase. Phase 1 needs context
derivation and backend selection, with optional rewrite/result/lifecycle hooks.
Phase 2 adds the decision-model extension. Support application-defined Rust providers
without runtime compilation or a dynamic plugin framework. Examples remain outside
core modules; consuming projects keep their custom Rust code in their own crates.

## Phase 0 — Project foundation

**Outcome:** a buildable scaffold, a clear design, and setup instructions.

- One dependency-free Cargo package with flat modules for configuration, gateway,
  query processing, backend execution, policy, caching, and hooks.
- Example configuration and external callback scaffolds documenting intended contracts.
- Component-level test scenarios and database fixture requirements.
- The binary prints a scaffold status. It does not serve queries or contact services.

Keep modules flat until their implementations justify directories. Split workspace
crates only when a component needs independent reuse or release.

```text
yasp/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── README.md
├── AGENTS.md
├── src/
│   ├── main.rs
│   ├── lib.rs
│   ├── config.rs
│   ├── gateway.rs
│   ├── query.rs
│   ├── backend.rs
│   ├── policy.rs
│   ├── cache.rs
│   └── hooks.rs
├── config/
│   └── example.yml
├── hooks/
│   ├── README.md
│   └── examples/
│       ├── role_selector.py
│       └── passthrough.py
├── tests/
│   ├── README.md
│   └── fixtures/README.md
└── docs/
    ├── PLAN.md
    └── development.md
```

## Phase 1 — PostgreSQL analytics proxy

**Outcome:** a read-only PostgreSQL proxy with one BI connection configuration.
For each query, a custom callback uses extracted metadata and a configured RBAC
policy to choose the backend target and role-specific credentials. PostgreSQL roles
enforce source access; configured result callbacks can additionally mask outputs.

One BI connection configuration may use multiple transport sessions through the BI
client's pool. Neither a separate BI connection nor separate proxy credentials are
required for each source role.

```text
BI query through shared proxy credentials
  -> extract metadata and apply any SQL rewrite
  -> select_backend callback with metadata and configured RBAC policy
  -> validate target/role decision and transaction compatibility
  -> role- and policy-scoped cache lookup
  -> acquire a matching backend connection on a miss, or create one within limits
  -> execute under the selected source role
  -> configured result processing and cache storage
  -> return result through the same BI connection
```

Deliver connectivity first, then governance/hooks/routing, then caching, followed
by complete component and client validation.

### Gateway and query processing

- Use Tokio and pgwire for PostgreSQL frontend connections. Support simple queries,
  prepared/parameterized queries, metadata descriptions, and requested result formats.
- Preserve transaction status, backend errors and SQLSTATE codes, column metadata,
  and cancellation behavior. Document the tested client/version/feature matrix.
- Use sqlparser's PostgreSQL dialect for supported inspection. Parsing describes
  syntax; it is not semantic validation or a security boundary.
- Preserve execution SQL unless an explicit hook rewrites it. Expose available
  query/comment/session metadata to the application-supplied context provider;
  pass its derived context to role selection together with configured RBAC data.
- Phase 1 assumes the connected BI service supplies the metadata used for role
  selection. Service authentication authenticates that integration, not each viewer;
  query comments alone do not prove an end-user identity. Verified end-user context
  is a later integration, not a requirement for the configuration/script prototype.
- Forward authorized catalog queries to PostgreSQL for Metabase schema discovery
  and field scanning. Schema caching is outside this phase.
- Support read-only session setup. Reject writes, COPY, replication, and
  temporary-table workflows with explicit errors.

### Backend execution and routing

- Use tokio-postgres and deadpool-postgres inside the PostgreSQL adapter.
- Invoke the required select_backend callback for each query using its metadata,
  configured RBAC policy, and current session/transaction context. Its decision
  names a configured target and backend login profile, never arbitrary credentials.
- Validate the selected pair against the BI service's configured access bounds.
  Acquire an idle matching connection, or open a new one with that profile's
  credentials if the bounded pool has capacity. Pool exhaustion has a bounded wait.
- Partition pools by target and backend login. Authenticate directly using the
  selected source role's credentials; source roles enforce their existing RBAC.
- Autocommit queries release connections after execution and verified cleanup.
  Consecutive queries on the same frontend session may select different roles.
- Explicit transactions pin the connection and role chosen for the transaction.
  Reject a callback decision that changes either until commit/rollback. Never move
  an in-progress transaction between role pools.
- Keep frontend prepared statements independent of individual pooled connections;
  prepare them on the selected backend as needed. Metadata and execution must use
  a compatible selected role, preserving parameter and result type semantics.
- Replay supported frontend session settings on acquired connections. Reject
  unsupported session-dependent features rather than silently losing their state.
- Distinguish physical creation/destruction from checkout/return events. Pool return
  does not suspend or destroy compute. Roll back/reset sessions before reuse;
  discard connections whose cleanup cannot be verified.

### Source RBAC and result processing

- Source database roles define privileges, row-level security (RLS), and restricted
  views. YASP selects the appropriate role-specific login rather than implementing
  a separate source permission system or requiring per-role BI connections.
- The local YAML supplies metadata-to-role policy data; an application-supplied
  Rust callback or trusted Python script interprets it and selects a target/login.
  Rust validates the permitted pair; credentials remain in the connection layer.
- Missing required metadata, unmapped policy entries, invalid selections, and
  callback errors deny the query. Schema discovery and connection setup use an
  explicit configured restricted selection for operations without viewer metadata.
- The same BI connection can therefore receive different source-authorized results
  for queries carrying different role-selection metadata.
- Optional result masks or result callbacks apply additional presentation rules.
  They preserve column schema, defaulting to SQL NULL or compatible text values.
  They are not required for source-role RBAC to function.
- Output names alone do not establish source lineage. Protection against arbitrary
  derived expressions or aggregate inference relies on source access controls;
  proxy result masking does not claim general SQL disclosure prevention.
- Required selection, policy, and result-hook failures fail the affected query
  closed. Unknown SQL must not silently bypass a mandatory check.

### Hooks

- Keep BI service identity, application-derived context, selected backend role,
  session ID, target, dialect, transaction state, and configuration/policy/hook generations in
  Rust-owned query context. Selected-role fields are populated after selection.
- Python receives a bounded context projection, SQL strings, typed bind metadata,
  and bounded typed result batches. Keep driver objects and the Rust AST internal.
- Optional before_query callbacks propose SQL rewrites; revalidate them and preserve
  parameter positions/types. The required select_backend callback then receives
  effective query context and RBAC policy and returns target/login plus cache scope.
- Run selection before cache lookup, including on hits. It must not open connections
  itself. The Rust backend component handles reuse or creation using the decision.
- Result callbacks preserve column schema and row ordering. Rust callbacks are
  compiled into the binary.
- Register application-provided Rust hooks and trusted Python modules at startup.
  Keep custom behavior outside engine modules. Use bounded workers outside Tokio's
  async worker threads. Python 3.12 CPU work shares the global interpreter lock (GIL).
- Embedded Python is not a sandbox. A timeout cannot reliably terminate executing
  code; count a worker as occupied until it finishes. Enforced isolation/termination
  and parallel Python CPU execution require a separate process-worker design.

### Redis result cache

- Approve deterministic reads explicitly; cache only in autocommit sessions with
  known relevant settings. Read-only SQL is not automatically deterministic.
- Keys include exact effective SQL, typed bound values, backend engine/dialect,
  target/database, BI service identity, selected source role, callback cache scope,
  relevant settings, and configuration/policy/hook generations. The cache scope
  identifies additional policy inputs that affect returned data or result processing.
  If that scope cannot be established, bypass caching. A shared frontend credential
  never makes results from different selected roles interchangeable.
- Cache final authorized results with column metadata and completion information.
  Hits still require current authorization and do not repeat result transformations.
- Default TTL: 30 seconds. Maximum entry: 8 MiB. This explicitly permits TTL-bounded
  staleness; it does not guarantee immediate invalidation after backend updates.
- Bypass transactions, catalog discovery, unknown settings, unapproved queries,
  volatile results, and nondeterministic hooks unless changing inputs are keyed.
- Stream using bounded buffers. Stop cache accumulation at the size cap while
  continuing streaming. Never cache partial, failed, or cancelled results.
- Redis outages and malformed entries are misses. Use versioned serialization,
  a dedicated namespace, and an explicit Redis memory policy.

### Configuration and operations

- Validate versioned YAML at startup. Configuration names the frontend protocol,
  target engines, BI service credentials, permitted target/login pairs, RBAC policy,
  the selection callback, optional result hooks, TLS, and resource limits.
- Resolve secrets through environment references. Local development binds localhost;
  non-local use requires encrypted frontend connections and verified backend TLS.
- Emit redacted logs and counters for latency, routing, pool waits, hooks, and cache
  behavior. Do not log credentials, bound values, or result values by default.
- Support graceful shutdown and resource cleanup.
- Use the supplied Docker-hosted Metabase instance. Record its version and reachable
  endpoint when available; configure a proxy address reachable from its container.

### Acceptance

Metabase connects, discovers authorized metadata, runs parameterized questions and
dashboards, and cancels work. psql exercises simple queries. Component scenarios
cover role changes on the same frontend session, source access restrictions,
connection reuse/creation, transaction pinning, prepared statements across role
pools, type fidelity, callback failures, role-scoped cache separation, outages,
oversized results, and cleanup. Compare throughput, latency, memory, and hook overhead with direct PostgreSQL before making performance
claims. Production readiness requires separate operational and security validation.

## Phase 2 — Additional engines, dynamic masking, and management

**Outcome:** additional backend support through localized adapters, dynamic masking
through a System 1 decision model, and authenticated management tools.

| Component | Capability |
| --- | --- |
| Backend adapters | Add a selected warehouse engine, such as Redshift or Snowflake, with its own execution, metadata, type, and session rules. |
| Gateway and dialect handling | Publish which frontend/backend combinations work. Add explicit translation for a supported SQL subset where needed; reject unsupported combinations. |
| Shared execution contract | Introduce adapter dispatch when the second engine is implemented. Keep routing, policies, hooks, and cache logic independent of its driver. |
| Decision model and masking | Classify result data and apply dynamic, policy-driven masks before forwarding results. |
| Management service | Add authenticated APIs for configuration inspection, health, and cache clearing; use Axum if an embedded HTTP service fits. |
| Admin UI and SQL Studio | Manage routing/access mappings and inspect query results through the same authorized execution path. |
| Configuration lifecycle | Validate replacement settings and hook versions, then activate a consistent generation without exposing mixed policies. |

Choose the second engine when this phase starts. Do not install its SDK or implement
its adapter in Phase 1. Adding another native client protocol is a separate gateway
capability, not a prerequisite for every new backend.

### System 1 decision-model integration and dynamic masking

- Integrate a configurable lightweight decision model through result-processing
  hooks. Use column metadata and bounded result samples or batches to classify
  columns, returning data tags and scores to the masking policy.
- An application-supplied model adapter provides classification without coupling the
  core engine to a provider SDK. The policy combines it with context and the
  selected source role to decide which values or columns to mask dynamically. Apply masks before
  forwarding affected data to the BI client, preserving column types and schema.
- Classification needed for a batch must complete before that batch is emitted.
  A sample-based column decision applies to the full corresponding result column;
  do not return an unmasked prefix while waiting for that decision.
- Define bounded inference time, input size, and buffering. For mandatory masking,
  inference failure or uncertain classification masks the affected output or denies
  the query according to explicit policy; it cannot silently return raw values.
- Sampling and probabilistic classification are not guarantees of detecting every
  sensitive value. Source-role RBAC remains independently enforced by the database.
- Cache only final masked results together with required classification tags and
  model/policy versions. Include model version and decision context in cache keys;
  incompatible versions miss. Never reuse a less-restricted output for a context
  requiring stronger masking.
- Choose the model/provider and confidence policy when this component is designed.
  No specific model SDK or implementation is required in Phase 1.

Acceptance includes adapter contract scenarios against real backends, faithful value
conversion, clear unsupported-feature errors, identity isolation, and cache separation.
Management actions require authorization and must not bypass execution policies.
Dynamic-masking scenarios cover restricted/permitted columns, type fidelity,
classification before emission, uncertain decisions, inference failures, bounded
processing, and cache isolation across roles and model/policy versions.

## Phase 3 — Advanced analytics and integrations

**Outcome:** independently scoped capabilities built on validated execution boundaries.

| Component | Capability and design requirement |
| --- | --- |
| Compute lifecycle | Provider-specific provisioning/suspension with coordination, readiness checks, and failure recovery. |
| Governance integration | Verified end-user context and external governance rules, with privacy controls for classification inputs. |
| Cache evolution | Metadata caching, explicit cross-target equivalence, and reliable data-version invalidation. |
| Semantic caching | Reuse compatible supersets through a local execution engine with correct ordering, NULLs, limits, filters, and aggregates. |
| Federation | Plan and execute across engines with explicit type, consistency, and failure semantics. |
| Natural-language queries | Model-provider integrations using the same authorized query execution path. |

Each capability needs its own bounded scope and acceptance criteria before work
begins. SQL parsing alone does not establish translation or semantic equivalence.

## Test and documentation principles

Tests exercise observable component/function contracts and real regressions. Cover
client compatibility, data integrity, access isolation, cache behavior, and failure
recovery at the appropriate boundary. Do not reproduce small internal logic in tests
or assert implementation structure. Refactors preserve tests when behavior is unchanged;
expectations change when the public behavior intentionally changes.

Documentation describes the final design and actual current capabilities in plain
language. Keep planned phases distinct from implemented behavior. Historical changes
and abandoned proposals belong in version control, not maintained design documents.

## References

- [pgwire protocol support](https://docs.rs/pgwire/latest/pgwire/)
- [PostgreSQL protocol flow](https://www.postgresql.org/docs/current/protocol-flow.html)
- [SQL parser capabilities](https://github.com/apache/datafusion-sqlparser-rs)
- [PyO3 parallelism](https://pyo3.rs/main/parallelism)
- [PyO3 embedding requirements](https://github.com/PyO3/pyo3)
- [Metabase database connections](https://www.metabase.com/docs/latest/databases/connections/postgresql)
- [Metabase discovery and scanning](https://www.metabase.com/docs/latest/databases/sync-scan)
- [Metabase Docker deployment](https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker)
