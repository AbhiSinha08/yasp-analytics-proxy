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

The design is feasible for a documented SQL, type, and client compatibility subset.
Full PostgreSQL transparency, arbitrary cross-engine SQL translation, and guaranteed
sensitive-data detection by a probabilistic model are outside that promise. Resolve
the BI metadata contract and prove protocol/type handling before building caching
or a management UI.

## Components and boundaries

Start with one Rust package containing a binary and a library. Each component has
one clear responsibility. Keep database-specific behavior inside adapters, and
introduce only the contracts needed by the current phase.

| Component | Responsibility | Extension boundary |
| --- | --- | --- |
| Client gateway | Authenticate connections, manage client sessions, decode requests, and encode responses. | Client protocol adapters own wire messages, authentication exchanges, and response encoding. PostgreSQL is first. |
| Query processing | Preserve SQL, expose raw query metadata to the registered context provider, inspect statements, and validate rewrites. | Dialect-specific parsing and validation stay separate from routing and policy. |
| Backend execution | Acquire a connection, execute a request, stream results, cancel work, and clean up sessions. | Database adapters own drivers, pools, bind conversion, native types, session behavior, and error mapping. |
| Routing | Invoke the registered selector with available request context, validate its configured target/login pair, and acquire that connection. | Application-specific context and policy remain outside the engine. |
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
  -> look up the final-result cache after authorization
  -> on hit: return the already processed result
  -> on miss: execute, apply result hooks, then mandatory masks and validation
  -> cache/return only the final authorized response
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

## Current implementation

The current PostgreSQL milestone serves loopback clients over the simple-query
protocol and forwards supported read-only queries through bounded pools for
configured PostgreSQL target/login pairs. Supported statements include `SELECT`, read-only
`WITH`, subqueries, joins, aggregates, unions, catalog queries, and `SHOW` variable
or `ALL` requests. Query inspection parses SQL once into a PostgreSQL-dialect AST;
`ParsedQuery` retains the original SQL and an immutable statement tree for read-only
inspection. Results preserve PostgreSQL column metadata and stream text rows without
driver value conversion.

SQL parsing defaults to a 1 MiB byte limit, with 4,096 significant tokens and a parser
recursion limit of 64; Unicode escaped identifiers (`U&"..."`) are unsupported.
Incoming frontend frames default to 8 MiB; backend frames have a fixed 8 MiB limit,
both checked before payload allocation. Queries run in
read-only backend transactions. `set_config` is allowed only when its setting-name
argument is the constant `application_name`; its value may be an expression. Calls
targeting serialization or resource settings are rejected. SQL inspection does not
analyze custom function bodies, so functions that change engine session settings are
unsupported. Rollback and `DISCARD ALL` clean up a lease. PostgreSQL reported text
`ParameterStatus` changes outside the allowed application name, failed cleanup,
interrupted queries, or unhealthy leases cause the connection to be discarded. The
PostgreSQL server checks disconnected clients every second. Backend connect, acquire,
query, frontend-write, and shutdown operations have separate bounds. The frontend
listener is loopback-only. Configured PostgreSQL targets use `tls_mode: disable`;
TLS is not available in this milestone.

The host loads ignored `config/local.yml` by default; `--config PATH` selects another
file. Runtime YAML uses `version: 1`, configured PostgreSQL targets/logins, optional
`routing.default_backend` and `hooks.select_backend` entries, an optional `backend`
capacity block, and an optional `gateway` block for the loopback listener,
session/frame/SQL bounds, query/read/write/startup/shutdown timeouts, and
local-development transport mode.
Omitted gateway settings use defaults. Query policy is passed to the PostgreSQL
adapter for database waits and a transaction-local statement timeout. Frontend read
limits apply between authenticated client messages, independently of query execution.
`protocol` accepts only PostgreSQL; listener addresses must be loopback;
`tls.mode: local_development` is plaintext. Frontend message size is configurable,
while the backend frame cap remains fixed at 8 MiB. Prepared-statement and portal
settings are not accepted by the runtime loader. Gateway username, password, and frontend database label come only
from `YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD`, and `YASP_GATEWAY_DATABASE`
in the process environment or optional `.env`; target login password is resolved
using YAML `password_env`. The frontend database label is separate from the actual
database in `targets.<target>.database`. Unsupported engines fail startup. Library
consumers can construct `BackendConfig` and `GatewayConfig` directly. Use
`gateway::serve` for one backend or `gateway::serve_routed` with `PostgresBackends`
and a registered selector for routed requests.

`tests/config.yml` uses the same version/target/login layout, with the target database
set to `postgres` for test database administration and the target secret named
`YASP_TEST_TARGET_PASSWORD`. The harness creates a fresh database from `template0`,
loads the SQL fixture, runs the proxy against that database, and drops only the
database it created. The source login needs `CREATEDB`; a non-superuser is sufficient.
The runtime creates no application database or persistent data fixture.
Client transactions, PostgreSQL cancel requests, prepared statements, `SET`, binary
results, organization-specific RBAC interpretation, query/result hooks, caching, TLS,
and BI-client integration are not implemented. The routing milestone below implements
selection contracts, configured target/login validation, and bounded PostgreSQL pools.
See [development setup](development.md) and [test guidance](../tests/README.md).

## Phase 0 — Project foundation

**Outcome:** a buildable scaffold, a clear design, and setup instructions.

- One Cargo package with foundation dependencies and flat modules for configuration,
  gateway, query processing, backend execution, policy, caching, and hooks.
- Example configuration and external callback scaffolds documenting intended contracts.
- Component-level test scenarios and database fixture requirements.
- The foundation provides module boundaries; Phase 1 adds runtime capabilities.
- An optional Python embedding example checks linking and imports without starting
  the proxy. Python callback execution is not implemented.

Keep modules flat until their implementations justify directories. Split workspace
crates only when a component needs independent reuse or release.

```text
yasp/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── README.md
├── AGENTS.md
├── examples/python_environment.rs
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
│   ├── requirements.txt
│   ├── builtin/
│   │   ├── mod.rs
│   │   └── select_backend/
│   │       ├── mod.rs
│   │       └── passthrough.rs
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

**Goal:** a read-only PostgreSQL proxy with one BI connection configuration.
The current milestone selects configured target/login pairs with a typed Rust
callback. Trusted BI context integration and application-owned RBAC policy remain
planned work. PostgreSQL logins enforce source access; result callbacks are planned.

One BI connection configuration may use multiple transport sessions through the BI
client's pool. Neither a separate BI connection nor separate proxy credentials are
required for each source role.

```text
BI query through shared proxy credentials
  -> inspect the parsed query and build a borrowed selection request
  -> registered selector returns a configured target/login pair
  -> validate the pair and acquire from its pool, opening connections on demand
  -> execute the read-only simple query under the selected source login
  -> return result through the same BI connection
```

Deliver a local protocol/type compatibility spike first, then backend selection and
bounded routing. Validate the intended BI client with caching disabled before adding
the result cache.
An incomplete connectivity spike is not an access-controlled deployment.

### BI context and client validation (planned)

Define how the consuming application obtains trusted routing context from the BI
client and any application-owned context provider. Validate the actual client version,
authentication mode, setup/discovery requests, query forms, background work, and
missing-context behavior before claiming compatibility. Keep authenticated frontend
identity distinct from derived application context; client-supplied SQL comments do
not establish identity by themselves. Review BI-side caching and shared metadata
access against the deployment's own authorization model.

### Gateway and query processing

- Use Tokio and pgwire for PostgreSQL frontend connections. Support simple queries,
  prepared/parameterized queries, metadata descriptions, and requested result formats.
- Start with one SQL statement per simple-query message; reject multiple statements
  explicitly. Extended-query Parse already requires one statement. Test the intended
  BI client's JDBC setup traffic against this restriction before claiming compatibility.
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
- Forward authorized catalog queries to PostgreSQL for BI schema discovery and field
  scanning. Schema caching is outside this phase.
- Support read-only session setup. Reject writes, COPY, replication, and
  temporary-table workflows with explicit errors.
- Enforce read-only backend transactions as well as least-privileged source logins.
  Reject writable CTEs, SELECT INTO, write-capable EXPLAIN ANALYZE, role/session
  authorization changes, and requests to disable read-only mode. Inspect the full
  statement rather than its first keyword; reject unsupported syntax. Restrict
  executable functions and role membership at the database: SELECT can call
  functions with side effects, and read-only transactions are not a code sandbox.

### Protocol and value compatibility gate

pgwire handles the frontend protocol; tokio-postgres is a database client, not a
transparent wire relay. Before committing to the adapter, demonstrate Parse, Bind,
Describe, Execute, Close, Flush, Sync, portal suspension/resumption, error recovery,
and cancellation with psql and the intended BI client's JDBC driver. Serialize execution
per frontend session. Preserve the protocol's failed-transaction state and discard
extended messages after an error until Sync as required by PostgreSQL.

Use streaming driver APIs rather than collecting a complete result. Binary/text
bind and result conversion needs an explicit supported-type matrix, including NULLs,
arbitrary-precision NUMERIC, timestamps and infinity, bytea, arrays, JSON, and domains.
PostgreSQL NUMERIC cannot in general fit in rust_decimal. Opaque bytes can pass
through only when source and requested formats and types agree; they do not provide
automatic binary-to-text conversion or Python values. Reject unsupported conversions
and mandatory masks on unsupported types. Keep lossless codecs inside the adapter.

Bound frontend frames and backend rows before large allocations, and bound queued
bytes and decoded copies, not only batch row counts. tokio-postgres 0.7's codec
waits for a complete backend message and exposes no application frame-size limit;
hook batch limits alone cannot bound that allocation. The adapter spike must prove
a bounded transport/codec integration or select a lower-level protocol client with
that control before claiming hard memory bounds. Do not silently weaken the limit.

### Backend execution and routing

The current routing milestone supports configured PostgreSQL target/login pairs and
an application-registered selector. `SelectionRequest` borrows the authenticated
frontend user, client and query IDs, and `ParsedQuery`; it contains no credentials.
`BackendSelection` names a target and backend login, while `SelectionError` distinguishes
denial from selector failure. The built-in passthrough selector uses the sole configured
pair automatically and requires `routing.default_backend` when multiple pairs make the
choice ambiguous. Denials and invalid configured-pair selections map to PostgreSQL
SQLSTATE `42501`; selector failures map to `XX000`. Per-pool capacity defaults to 8 connections and
total capacity to 16; startup rejects limits whose per-pool capacities cannot fit
within the total.

The library exposes `yasp::hooks::bind_select_backend` to bind a typed Rust callback
to application state. Rust extensions compile into the consuming host. The repository's
`hooks/builtin/select_backend/passthrough.rs` contains its built-in selector. Reusable
repository hooks live under `hooks/builtin` and are registered by the host. A consuming
host maps configured names to its own compiled callbacks and loads policy data at startup.
Selectors choose only among configured pairs; the backend owns credentials and the
router validates each result. Selection runs synchronously on the query path, so
callbacks must be fast and nonblocking; the engine has no dedicated callback worker
or hard selector timeout.

- Invoke the configured selector for each executable query using its request context.
  Its decision
  names a configured target and backend login profile, never arbitrary credentials.
- Validate the selected pair against configured targets and logins.
  Acquire an idle matching connection, or open a new one with that profile's
  credentials if the bounded pool has capacity. Pool exhaustion has a bounded wait.
- Partition pools by target and backend login. Authenticate directly using the
  selected source role's credentials; source roles enforce their existing RBAC.
- Autocommit queries release connections after execution and verified cleanup.
  Consecutive queries on the same frontend session may select different roles.
- Explicit transactions pin the connection and role chosen for the transaction.
  Reject a callback decision that changes either until commit/rollback. Never move
  an in-progress transaction between role pools.
- A metadata-free BEGIN records a pending read-only transaction. Its first operation
  needing a backend, including preparation/description, selects and pins the route;
  replay the transaction options before that operation. COMMIT/ROLLBACK on an empty
  transaction require no backend. Transaction control and cancellation act on the
  current session's pinned work without a fresh routing decision, so missing metadata
  cannot prevent rollback. Bound idle transaction and suspended-portal lifetime.
- Keep frontend prepared statements independent of individual pooled connections;
  prepare them on the selected backend as needed. Metadata and execution must use
  a compatible selected role, preserving parameter and result type semantics.
- Parse/Describe can need backend metadata before Bind provides values. Select from
  metadata available at that stage; deny preparation if routing requires unavailable
  inputs. Retain the SQL and context with the frontend statement. Reauthorize each
  execution, reprepare on its selected connection, and check parameter types and
  the previously advertised result description before returning rows. A changed
  route must not silently change that description. Bind and suspended portals retain
  their execution route; never switch roles partway through a portal.
- Replay supported frontend session settings on acquired connections. Reject
  unsupported session-dependent features rather than silently losing their state.
- Distinguish physical creation/destruction from checkout/return events. Pool return
  does not suspend or destroy compute. Roll back/reset sessions before reuse;
  discard connections whose cleanup cannot be verified.
- Deadpool's Fast recycling is not cleanup; Verified only checks liveness. Drain or
  cancel outstanding work, roll back, reset session state, and verify before reuse.
  Clean recycling can help reset settings after rollback. If using DISCARD ALL,
  also invalidate the driver's prepared-statement cache. On timeout or disconnect,
  keep the lease until cleanup completes or discard it. Remove cancellation mappings
  before reuse so a stale cancellation cannot affect the next borrower.
- Cancellation dispatch and connection release must be synchronized: removing a
  mapping does not recall a cancellation already sent. If delivery/completion is
  uncertain, discard the backend connection instead of making it reusable.
- Bound total connections as well as each target/login pool, including retiring
  generations. Reapply supported settings and backend statement timeouts on checkout;
  a timeout on a Rust future alone does not stop PostgreSQL execution.

### Source RBAC and result processing

The capabilities in this section are planned. The current engine validates configured
target/login selections; it does not derive BI viewer context or interpret organization
RBAC policy.

- Source database roles define privileges, row-level security (RLS), and restricted
  views. YASP selects the appropriate role-specific login rather than implementing
  a separate source permission system or requiring per-role BI connections.
- Use non-owner, non-superuser logins without BYPASSRLS or membership permitting
  escalation. Audit view/function execution privileges and RLS behavior with those
  actual logins; a successful test as the table owner proves nothing about isolation.
- Application policy data and RBAC interpretation remain owned by the consuming
  application; the engine validates the selected configured target/login pair.
- Once application policy is integrated, missing required metadata, unmapped policy
  entries, invalid selections, and callback errors must deny the query. Any distinct
  discovery selection must be supplied by the host's policy.
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

- Future context integration may keep BI service identity, application-derived context, selected backend role,
  session ID, target, dialect, transaction state, and configuration/policy/hook generations in
  Rust-owned query context. Selected-role fields are populated after selection.
- Python may receive a bounded context projection, SQL strings, typed bind metadata,
  and bounded typed result batches in a later milestone. Keep driver objects internal;
  Rust selectors can inspect the borrowed parsed query through `query.statement()`.
- Optional before_query callbacks may propose SQL rewrites; revalidate them and
  preserve parameter positions/types. Application code owns policy interpretation.
- Run selection before cache lookup, including on hits. It must not open connections
  itself. The Rust backend component handles reuse or creation using the decision.
- Result callbacks preserve column schema and row ordering. Rust callbacks are
  compiled into the binary.
- Apply optional transformations before mandatory classification/masking; no later
  extension may restore raw data. Validate types, row counts, and byte limits after
  each result callback. Hook logging must not expose raw query/result values.
- Register application-provided Rust hooks at startup. Python callback execution is
  a later milestone; trusted Python modules are not loaded by the current router.
  Keep custom behavior outside engine modules. Use bounded workers outside Tokio's
  async worker threads. Python 3.12 CPU work shares the global interpreter lock (GIL).
- Embedded Python is not a sandbox. A timeout cannot reliably terminate executing
  code; count a worker as occupied until it finishes. Enforced isolation/termination
  and parallel Python CPU execution require a separate process-worker design.
- The `python` Cargo feature adds PyO3, not Python packages. Deploy one pinned Python
  environment per host process, import registered modules before accepting queries,
  and fail startup if a required import fails. Build-time interpreter selection and
  runtime package search paths are separate; see [Python setup](development.md#python-embedding).
  All embedded hooks share package versions and interpreter state. Conflicting
  dependencies or hard execution limits require separate processes. New package
  versions activate through process replacement, not in-place pip installs/reloads.

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
  Hits still require current proxy policy authorization and do not repeat result
  transformations; source authorization freshness is limited as described below.
- Default TTL: 30 seconds. Maximum entry: 8 MiB. This explicitly permits TTL-bounded
  staleness; it does not guarantee immediate invalidation after backend updates.
  Hits do not extend expiry. In-flight queries and their snapshots can also delay
  visibility of source changes; TTL alone is not an immediate-revocation mechanism.
- Cache hits do not recheck PostgreSQL grants, RLS changes, or disabled backend
  logins. Source authorization is authoritative when executing at the database;
  replaying a cached result requires an explicit acceptance of bounded authorization
  staleness too. Keep caching disabled when immediate revocation is required, unless
  a coordinated invalidation/generation mechanism covers those source changes.
- Bypass transactions, catalog discovery, unknown settings, unapproved queries,
  volatile results, and nondeterministic hooks unless changing inputs are keyed.
- Stream using bounded buffers. Stop cache accumulation at the size cap while
  continuing streaming. Never cache partial, failed, or cancelled results.
- Redis outages and malformed entries are misses. Use versioned serialization,
  a dedicated namespace, and an explicit Redis memory policy.
- Bound Redis connect/command time and reads before deserialization; enforce entry
  size on hits as well as writes. Use ACLs and TLS off-host because cached results
  remain sensitive. Include requested result formats in keys when storing wire
  bytes. Do not use untrusted Python pickle for cache entries.

### Configuration and operations

- Future full-deployment configuration may include TLS and additional operational
  settings. The current loader accepts connection pairs, selector registration,
  pool bounds, and supported gateway limits; application policy data remains owned
  by the consuming host.
- Resolve secrets through environment references. Local development binds localhost;
  non-local use requires encrypted frontend connections and verified backend TLS.
- Emit redacted logs and counters for latency, routing, pool waits, hooks, and cache
  behavior. Do not log credentials, bound values, or result values by default.
- Support graceful shutdown and resource cleanup.
- Limit SQL/parameter/frame sizes, prepared statements and portals per session,
  queued hook bytes, active queries, total cache buffers, and slow-client write time.
  Account for Rust/Python copies and retain worker permits after callback timeouts.
- Validate against the intended BI client. Record its version and reachable endpoint
  when available, and configure a proxy address reachable from its runtime environment.

### Acceptance

The intended BI client connects, discovers authorized metadata, runs supported
parameterized questions, and cancels work. psql exercises simple queries. Component scenarios
cover role changes on the same frontend session, source access restrictions,
connection reuse/creation, transaction pinning, prepared statements across role
pools, type fidelity, callback failures, role-scoped cache separation, outages,
oversized results, and cleanup. Compare throughput, latency, memory, and hook overhead with direct PostgreSQL before making performance
claims. Production readiness requires separate operational and security validation.

### Remaining Phase 1 order

Next implement the Python selector option, then PostgreSQL cancellation, context and
client compatibility, result processing and caching, and TLS/operations. Add basic
prepared statements and portals, limited `SET` and transactions, suspended portals,
and binary results after those slices. Treat this order as a working sequence; promote
an item when a concrete BI compatibility requirement needs it.

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

### Execution milestones

1. **Mask PostgreSQL results first.** Prove a deterministic policy with fixture
   classifications, then integrate the selected model with measured accuracy,
   latency, and failure behavior. Management UI work is not a prerequisite.
2. **Add one warehouse.** Pick it from actual workload requirements. Write the
   frontend/backend SQL, metadata, type, authentication, cancellation, and session
   matrix before selecting its driver. Redshift's PostgreSQL-compatible connection
   does not imply identical catalog or type behavior; Snowflake needs its own
   execution/authentication path. PostgreSQL JDBC catalog discovery against a
   warehouse needs metadata adaptation as well as query translation. Restrict the
   initial surface rather than promise general PostgreSQL emulation.
3. **Expose management, then UI.** Use the same authorized execution path for SQL
   Studio. Redact secrets and results in inspection APIs; authorize configuration
   changes and scoped cache invalidation. A namespace/generation change must prevent
   in-flight old requests from repopulating a newly cleared cache.
4. **Activate configuration generations.** Each query retains one immutable set of
   policy, hooks, model, and settings; transactions and portals retain compatible
   state until drained or explicitly cancelled. New requests use the replacement
   generation. Credential rotations retire affected pools. Python package/module
   changes use a replacement process. Immediate revocation requires explicit
   cancellation and cache invalidation rather than waiting for old work to drain.

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
- Choose a bounded sampling mode explicitly. A prefix sample fixes a column decision
  for that result and can miss sensitive values later. For per-batch classification,
  mask each batch before emission; later decisions cannot retract earlier data.
  If policy requires a verdict on the entire result, enforce a whole-result size cap
  and deny oversized results, or conservatively mask from the start. Unlimited
  streaming and retrospective whole-result classification cannot both be promised.
- Identify outputs by ordinal plus metadata, not just aliases: duplicate names,
  expressions, and joins do not establish lineage. Classifiers return bounded,
  validated tags/scores, never executable SQL or code. Unknown types/tags and invalid
  scores follow the mandatory-mask failure policy. Prefer NULL when a compatible
  replacement value cannot be encoded.
- Define bounded inference time, input size, and buffering. For mandatory masking,
  inference failure or uncertain classification masks the affected output or denies
  the query according to explicit policy; it cannot silently return raw values.
- Sampling and probabilistic classification are not guarantees of detecting every
  sensitive value. Source-role RBAC remains independently enforced by the database.
- Classifiers necessarily see pre-mask samples. Decide local inference versus an
  approved external endpoint, data minimization, retention, and logging policy before
  sending those samples. Enforce request limits and disable raw sample logging.
- Cache only final masked results together with required classification tags and
  model/policy versions. Include model version and decision context in cache keys;
  incompatible versions miss. Never reuse a less-restricted output for a context
  requiring stronger masking.
- Choose the model/provider and confidence policy when this component is designed.
  No specific model SDK or implementation is required in Phase 1.

If an HTTP API suffices, use the consuming application's existing HTTP client before
adding a provider SDK. Local inference libraries and native/GPU dependencies belong
to that application's Python environment or Rust adapter. A model that needs a hard
kill deadline belongs in a process worker or external service, not embedded Python.

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

- [Dependency choices and embedding setup](development.md)
- [pgwire protocol support](https://docs.rs/pgwire/latest/pgwire/)
- [tokio-postgres streaming API](https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Client.html#method.query_raw)
- [tokio-postgres backend codec](https://docs.rs/tokio-postgres/latest/src/tokio_postgres/codec.rs.html)
- [Deadpool recycling behavior](https://docs.rs/deadpool-postgres/latest/deadpool_postgres/enum.RecyclingMethod.html)
- [PostgreSQL protocol flow](https://www.postgresql.org/docs/current/protocol-flow.html)
- [PostgreSQL read-only transactions](https://www.postgresql.org/docs/current/sql-set-transaction.html)
- [PostgreSQL row security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
- [SQL parser capabilities](https://github.com/apache/datafusion-sqlparser-rs)
- [PyO3 parallelism](https://pyo3.rs/main/parallelism)
- [PyO3 embedding requirements](https://github.com/PyO3/pyo3)
