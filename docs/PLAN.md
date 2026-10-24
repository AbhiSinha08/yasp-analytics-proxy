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
the BI metadata contract and prove protocol/type handling before enabling caching or
building a management UI.

## Components and boundaries

Start with one Rust package containing a binary and a library. Each component has
one clear responsibility. Keep database-specific behavior inside adapters, and
introduce only the contracts needed by the current phase.

| Component                    | Responsibility                                                                                                                         | Extension boundary                                                                                                                                                |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Client gateway               | Authenticate connections, manage client sessions, decode requests, and encode responses.                                               | Client protocol adapters own wire messages, authentication exchanges, and response encoding. PostgreSQL is first.                                                 |
| Query processing             | Preserve SQL, expose raw query metadata to the registered context provider, inspect statements, and validate rewrites.                 | Dialect-specific parsing and validation stay separate from routing and policy.                                                                                    |
| Backend execution            | Acquire a connection, execute a request, stream results, cancel work, and clean up sessions.                                           | Database adapters own drivers, pools, bind conversion, native types, session behavior, and error mapping.                                                         |
| Routing                      | Invoke the registered selector with available request context, validate its configured target/login pair, and acquire that connection. | Application-specific context and policy remain outside the engine.                                                                                                |
| Policy                       | Supply the configured role-selection policy, validate callback decisions, and apply configured result masks.                           | Database access controls remain authoritative; masking preserves the result schema.                                                                               |
| Extensions                   | Invoke application-supplied context derivation and policy/query/result/lifecycle hooks.                                                | Contracts belong to the library; organization implementations live outside core engine code.                                                                      |
| Result cache                 | Store source results for a limited time, then apply current engine processing.                                                         | The Redis key uses the namespace, configured target/login names, and normalized SQL hash; the entry scope validates backend, identity, and fixed session context. |
| Schema catalog               | Store source definitions in bounded immutable generations and refresh them periodically.                                               | Source identity, exact names and native types are shared by frontend metadata and AST lineage; schema facts do not authorize access or apply masks.               |
| Configuration and operations | Validate settings, resolve secrets, enforce limits, and report health and performance.                                                 | Shared configuration names the client protocol and each target's engine explicitly.                                                                               |

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
Result pipeline; query rewrites, engine RBAC, and mandatory masks remain planned
Organization application
  -> supply configuration and policy data
  -> register context derivation and Rust/Python hooks
  -> construct core engine
  -> authenticate request and expose available metadata
  -> derive organization context
  -> run role selection (query rewrite remains planned)
  -> look up the source-result cache
  -> on hit: load the cached source result
  -> on miss: execute the source query and cache its result
  -> classify bounded raw samples and run optional result hooks and validation
  -> apply future engine RBAC and mandatory masks
  -> return the processed result
```

Keep authenticated service identity and engine-owned session/security fields
separate from derived organization context. A context provider may enrich request
metadata but cannot replace authenticated identity or bypass configured connection
bounds. The cache validates the selected backend endpoint and source user together
with authenticated frontend identity and fixed session context; it does not interpret
organization-specific groups itself. Future engine policy and masking run on loaded
source results before any data reaches the client.

Define only extension contracts required by the active phase. Phase 1 needs context
derivation and backend selection, with optional rewrite/result/lifecycle hooks.
Phase 2 adds the decision-model extension. Support application-defined Rust providers
without runtime compilation or a dynamic plugin framework. Examples remain outside
core modules; consuming projects keep their custom Rust code in their own crates.

## Current implementation

The runtime supports a bounded PostgreSQL simple-query subset over independently configured TLS or plaintext connections,
configured source-role routing, optional query context, shared application datasets,
separate metadata discovery, cancellation, and an opt-in Redis result cache. PostgreSQL enforces source
access. It preserves native text values and metadata with verified pool cleanup.
Prepared statements, query/lifecycle hooks, engine RBAC
interpretation, masking, and management UI remain planned. Bounded read-only,
read-committed client transactions pin one physical backend until commit/rollback
and bypass result caching.
Verified TLS covers PostgreSQL clients, source/cancellation connections, and both
Redis consumers; see [TLS setup](tls.md).

An experimental Redshift profile shares the simple-query transport. The optional
Snowflake backend forwards native and frontend-generated data SQL unchanged through the normal YAML host, with
bounded schema snapshots serving captured PostgreSQL/Redshift metadata requests.
The persistent local emulator and unified client tests are documented in
[multi-engine compatibility](multi-engine.md). Generated data-query translation,
real Snowflake access isolation and complete BI exploration remain separate gates.

Use the [documentation index](README.md) for verified current
[architecture and flows](architecture.md), [configuration](configuration.md),
[extension contracts](extensions.md), [development](development.md), and
[test setup](../tests/README.md). The sections below define phase goals and intended
boundaries rather than claiming every listed capability is implemented.

## Phase 0 — Project foundation

**Outcome:** a buildable scaffold, a clear design, and setup instructions.

- One Cargo package with foundation dependencies and flat modules for configuration,
  gateway, query processing, backend execution, policy, caching, and hooks.
- Example configuration and external callback scaffolds documenting intended contracts.
- Component-level test scenarios and database fixture requirements.
- The foundation provides module boundaries; Phase 1 adds runtime capabilities.
- An optional Python embedding example checks linking and imports without starting
  the proxy. The Phase 1 hook runtime executes external Python callbacks.

Keep modules flat until their implementations justify directories. Split workspace
crates only when a component needs independent reuse or release.

Current source ownership is documented in the
[component map](architecture.md#component-ownership). Add module directories only
when a component's implementation needs separate responsibilities.

## Phase 1 — PostgreSQL analytics proxy

**Goal:** a read-only PostgreSQL proxy with one BI connection configuration.
The current milestone derives optional application context and selects configured
target/login pairs with typed Rust or external Python callbacks. Verification of
trusted BI metadata and application-owned RBAC policy remain planned work.
PostgreSQL logins enforce source access; optional bounded result callbacks apply presentation rules.

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
bounded routing. Validate the intended BI client with caching disabled before enabling
the result cache.
An incomplete connectivity spike is not an access-controlled deployment.

### BI context and client validation

The library exposes an optional context provider configured at `hooks.context`.
For ordinary queries it receives authenticated frontend identity, frontend database, client/query IDs,
and the immutable parsed query. The provider derives bounded plain JSON context;
the engine invokes it once after SQL validation and before backend selection and
cache eligibility, including on cache hits. That context is passed to downstream
hooks and cannot replace authenticated frontend identity. Provider errors, invalid
output, timeouts, and runtime saturation produce empty context; cancellation still
stops the query. This contract does not establish that any BI client's metadata is
trusted or present. Verify actual client version, authentication mode, discovery,
query forms, background work, and missing-context behavior before claiming
compatibility. Client-supplied SQL comments do not prove end-user identity.
The `metabase_pg` and Redash extractors parse the observed leading PostgreSQL
remarks for the tested client versions and expose normalized diagnostic fields.
`metabase_rs` parses the partner remark defined by Metabase's Redshift driver;
manual validation awaits a YASP Redshift gateway. Superset extraction is not
builtin because its SQL mutator is deployment configured.

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
- Preserve execution SQL unless an explicit hook rewrites it. The optional context
  provider receives the validated parsed query and derives application context for
  later hooks; it does not authenticate BI metadata or interpret RBAC.
- Phase 1 assumes the connected BI service supplies the metadata used for role
  selection. Service authentication authenticates that integration, not each viewer;
  query comments alone do not prove an end-user identity. Verified end-user context
  is a later integration, not a requirement for the configuration/script prototype.
- Serve captured schema-discovery queries from a required catalog loaded through
  the configured metadata identity and explicit schema scope. Refresh immutable
  generations periodically; reject unsupported metadata without live fallback.
  Field scanning uses ordinary application routing.
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

The [source access and compute routing guide](source-access-routing-plan.md)
describes partial selector decisions, target-scoped compute types, overflow, and
shared connection budgets.

The current routing milestone supports configured PostgreSQL target/login pairs and
an application-registered selector. `SelectionRequest` borrows the authenticated
frontend user, client and query IDs, and `ParsedQuery`; it contains no credentials.
`BackendDecision` names optional target, backend login, and compute type. The engine
resolves it to a concrete `BackendSelection` pair before allocation. `SelectionError` distinguishes
denial from selector failure. The built-in passthrough selector uses the sole configured
pair automatically with empty params; ambiguous target/login choices require explicit names in
`hooks.select_backend.params`. Compute-enabled targets use an explicit default type
or their sole type when the decision omits it. Denials and invalid configured-pair
selections map to PostgreSQL SQLSTATE `42501`; selector failures map to `XX000`. Per-pool capacity defaults to 8 connections and
total capacity to 16; the total bounds actual open pooled connections rather than reserving every pool
maximum. Idle sessions can be reclaimed for another binding. Saturation waits
asynchronously under a 10-second acquisition deadline, then returns `53300`.

The library exposes `yasp::hooks::bind_select_backend` to bind a typed Rust callback
to application state. Rust extensions compile into the consuming host. The repository's
`hooks/builtin/select_backend/passthrough.rs` contains its built-in selector. Reusable
repository hooks live under `hooks/builtin` and are registered by the host. A consuming
host maps configured names to its own compiled callbacks and loads policy data at startup.
Rust and Python references use `type`, `callback`, and application-owned `params`;
Python also requires a YAML-relative `path`. Rust string references are shorthand
with empty params. Hosts parse Rust params into typed callback state before binding.
The loadable configuration example preserves unimplemented settings in marked
comments: future RBAC mappings belong to custom selector params, while principals
and engine-applied result masks remain independent enforcement settings.
Selectors choose only among configured pairs; the backend owns credentials and the
router validates each result. Rust selection runs synchronously on the query path,
so compiled callbacks must be fast and nonblocking and have no enforceable timeout.
Python selection uses the dedicated bounded runtime described below.

- Invoke the configured selector for each executable query using its request context.
  Its decision
  names optional configured target, backend login profile, and compute type, never arbitrary credentials.
- Validate the selected pair against configured targets and logins.
  Acquire an idle matching connection, or open a new one with that profile's
  credentials if the bounded pool has capacity. Pool exhaustion has a bounded wait.
- Partition pools by target, backend login, and physical compute instance. Authenticate directly using the
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

Engine RBAC interpretation and mandatory masking in this section are planned. Optional bounded
result processing is implemented. The current engine validates configured target/login selections and can pass optional derived
context to callbacks; it does not authenticate BI viewer identity or interpret
organization RBAC policy.

- Source database roles define privileges, row-level security (RLS), and restricted
  views. YASP selects the appropriate role-specific login rather than implementing
  a separate source permission system or requiring per-role BI connections.
- Use non-owner, non-superuser logins without BYPASSRLS or membership permitting
  escalation. Audit view/function execution privileges and RLS behavior with those
  actual logins; a successful test as the table owner proves nothing about isolation.
- Application policy data and RBAC interpretation remain owned by the consuming
  application; the engine validates the selected configured target/login pair.
- Application policy defines default permissions for missing or unmapped context.
  Invalid selections and required policy failures deny the query. Any distinct
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

- The optional context hook receives a borrowed `ContextRequest` with authenticated
  user, frontend database, client/query IDs, and `ParsedQuery`. `bind_context(state, f)`
  binds an async Rust closure and returns owned JSON object data or `ContextError`.
  Context configuration uses `type`, `callback`, and `params`; Python references also
  name a file `path` and call `function(request, params)`, returning a dictionary or
  `None`. `hooks::builtin::extractors` exposes `metabase_pg`, `metabase_rs`, and
  `redash` parsers, plus the unimplemented Superset placeholder. Python
  `yasp_extractors` exposes the same names and raises `NotImplementedError` for
  Superset.
- Derive context once for ordinary queries after SQL validation and before backend selection and cache
  eligibility, also on cache hits. Downstream selection sees context as nested plain
  immutable data; Python receives a copy. Context cannot replace frontend identity.
  The defaults are a 2-second context wait, 65,536 encoded bytes, and nesting 64.
  Runtime errors, invalid output, timeout, and saturation supply empty context and
  do not deny the query. Cancellation returns `57014` and stops the query. Startup
  configuration, import, and registration errors fail startup. Python retains its
  existing single-worker reservations after a timed-out invocation.
- Context does not partition cache keys, change cache scope, or add a cache bypass.
  Source-role permissions remain the active database authorization boundary.
  Engine policy remains future work; result processing runs on loaded raw results.
- Future session lifecycle integration may keep BI service identity, application-derived context, selected backend role,
  session ID, target, dialect, transaction state, and configuration/policy/hook generations in
  Rust-owned query context. Selected-role fields are populated after selection.
- Python backend selection receives a bounded dictionary of authenticated frontend
  user, client/query IDs, original SQL, parser JSON AST, output data contexts, and
  derived application context, with application-owned params. Return a configured
  target/login dictionary or None to deny. It receives no credentials or Rust
  driver objects. Typed binds require later contracts. Result hooks use native batches;
  driver objects stay internal. Rust selectors inspect the borrowed tree through `query.statement()`.
- Optional before_query callbacks may propose SQL rewrites; revalidate them and
  preserve parameter positions/types. Application code owns policy interpretation.
- Run selection before cache lookup, including on hits. It must not open connections
  itself. The Rust backend component handles reuse or creation using the decision.
- Result callbacks preserve column schema and retained-row ordering; they may filter rows. Rust callbacks are
  compiled into the binary.
- Classification describes raw values before result callbacks run. Future mandatory
  masks must follow optional transformations, with no later extension restoring raw
  data. Validate types, row counts, and byte limits after each result callback.
  Hook logging must not expose raw query/result values.
- Register application-provided Rust hooks and external Python file/function references
  at startup. The feature-gated `hooks::python` platform exposes reusable registration
  and asynchronous invocation of trusted synchronous callbacks; backend selection
  and context derivation use the runtime. Result integration uses native bounded batches; query/lifecycle integration remains planned.
  Python examples live under hooks/examples and are not embedded in the binary.
  One dedicated worker runs registration and invocation outside Tokio. Python 3.12
  CPU work shares the global interpreter lock (GIL).
- Python imports use isolated initialization, standard-library paths, and explicitly
  configured search paths. The default queue capacity is 32, queued-plus-active input
  budget is 4 MiB, individual payload limit is 2 MiB, and nesting limit is 64. JSON
  bytes are bounded; arbitrary Python heap use is not. The 2-second waiting deadline
  includes queue time and registration. Saturation, invalid output, exceptions, and
  timeout fail selection without fallback. Cancellation abandons the selection wait
  with `57014`; queued expired/abandoned jobs never execute. Shutdown stops admission
  and wakes waiters without joining running Python. See [hook contracts](../hooks/README.md).
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

- Caching is disabled by default. Shared execution applies it to every backend and
  both frontend profiles, without adapter opt-ins. A hit skips the source, so grants,
  row-level security, and disabled source logins are not rechecked until expiry.
  Configure `url_env` as `YASP_REDIS_URL`, a namespace, and a TTL (default one hour).
- Every supported read-only query is eligible by default. Optional Rust or Python
  `cache.eligibility` callbacks return a boolean to bypass queries; they cannot
  expand supported SQL or resource limits. The built-in Rust callback
  `builtin.cache_eligibility.exclude_sql_containing` bypasses effective SQL containing
  configured case-insensitive literal fragments. It also matches comments and string
  literals, so it may bypass extra queries; it does not parse SQL or guarantee a
  table-name match. Use it for freshness-sensitive monitoring such as
  `pg_stat_activity` or `clock_timestamp()`. Normalized effective-SQL SHA-256 query
  IDs combine with configured target/login names under the namespace to identify
  cache entries; SQL-text fragments remain the exclusion policy.
- Entries preserve exact frontend metadata and text values in an opaque versioned
  payload. Before replay, scope validates frontend profile/identity/database, input
  dialect/mode, actual backend endpoint/database/principal, role/warehouse/schema or
  compute instance, fixed source/session settings, query/write timers, frame bound,
  and codec/execution revision. Explicit transactions and metadata bypass caching.
  Partial, failed, cancelled, malformed, or mismatched entries are not replayed. Redis failures and fixed 100 ms operation timeouts are cache misses.
- `max_rows_per_query` defaults to 10,000 and bounds rows captured and replayed;
  larger results bypass caching and continue streaming. `max_buffered_bytes_total`
  defaults to 32 MiB and bounds aggregate in-process cache buffers, not Redis storage.
  The internal serialized-entry guard is one third of this budget. Each active query
  reserves up to three times its entry budget for Redis, buffered, and response-wire
  copies. Redis server memory and eviction are configured separately. Hits do not
  extend TTL.
- The library exposes `config::CacheSettings`, `cache::RedisCache`, and
  `GatewayConfig::with_cache`; eligibility uses `hooks::bind_cache_eligibility`.
  Python registrations share the process's existing `PythonRuntime`.
  Redis transport supports plaintext TCP and verified TLS.
- Future application RBAC and Phase 2 masking must also apply to cached results.
  Current hits validate frontend and selected-backend scope; source authorization is
  checked when PostgreSQL executes a query.

#### Redis deployment recommendation

Redis is sufficient for this shared TTL cache. Use a dedicated deployment with
[a memory cap and eviction policy](https://redis.io/docs/latest/develop/reference/eviction/).
[Valkey](https://valkey.io/) is an alternative to evaluate; an in-process cache is
simpler when only one proxy instance needs the data. Restrict cache access and do not
log payloads. The current transport supports plaintext TCP and verified TLS.

### Configuration and operations

- The current loader accepts TLS certificates and trust settings, connection pairs, selector registration,
  pool bounds, and supported gateway limits; application policy data remains owned
  by the consuming host.
- Resolve secrets through environment references. The default gateway binds
  localhost. Deployments independently select plaintext or verified TLS for
  frontend, source, and Redis connections; plaintext supports private VPC endpoints.
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
pools, type fidelity, callback failures, selected-source cache isolation, outages,
oversized results, and cleanup. Compare throughput, latency, memory, and hook overhead with direct PostgreSQL before making performance
claims. Production readiness requires separate operational and security validation.

### Remaining Phase 1 order

Next validate client context and compatibility. Bounded result processing is
implemented. Validate the result cache with the intended BI client before enabling
it in that deployment. Add basic
prepared statements and portals, broader `SET` and transaction features, suspended portals,
and binary results after those slices. Treat this order as a working sequence; promote
an item when a concrete BI compatibility requirement needs it.

## Phase 2 — Additional engines, dynamic masking, and management

**Outcome:** additional backend support through localized adapters, dynamic masking
through a System 1 decision model, and authenticated management tools.

| Component                    | Capability                                                                                                                                                                                                                  |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Backend adapters             | Add a selected warehouse engine, such as Redshift or Snowflake, with its own execution, metadata, type, and session rules.                                                                                                  |
| Gateway and dialect handling | Publish which frontend/backend combinations work. Add explicit translation for a supported SQL subset where needed; reject unsupported combinations.                                                                        |
| Shared execution contract    | Use concrete source/session dispatch and typed execution contracts. Preserve opaque PostgreSQL values; extend adapters after their connector gates pass.                                                                    |
| Decision model and masking   | Classify result data and apply dynamic, policy-driven masks before forwarding results.                                                                                                                                      |
| Management service           | Add authenticated APIs for configuration inspection, health, and cache clearing; use Axum if an embedded HTTP service fits.                                                                                                 |
| Admin UI and SQL Studio      | Manage routing/access mappings and inspect query results through the same authorized execution path.                                                                                                                        |
| Configuration lifecycle      | Validate replacement settings and hook versions, then activate a consistent generation without exposing mixed policies.                                                                                                     |
| Redis query queue            | Bound burst admission with deadlines, cancellation, worker ownership, and recovery. Current local acquisition waits asynchronously for `backend.acquire_timeout_ms` (60 seconds by default), then returns a capacity error. |

Snowflake runtime dependencies are optional behind the `snowflake` feature. The
normal YAML host supports Snowflake-only discovery and data bindings through both
frontend profiles. The adapter uses the public SQL API with key-pair authentication,
bounded JSON partitions and autocommit statements. Frontend transaction blocks
preserve route affinity and failure states without a shared source snapshot. The
pinned Docker emulator and focused HTTP tests cover the local compatibility contract.
Shared execution applies one bounded result-cache path to every backend; adapters
provide source identity and verified completion without cache opt-ins. Generated
SQL executes unchanged after read-only checks. Broader SQL translation and real-source authentication/role/warehouse acceptance remain
separate gates. Adding a native client protocol is a separate gateway capability,
not a prerequisite for each backend.

### Execution milestones

The shared [schema catalog](catalog.md) supplies qualified relation/column
coordinates and native types through periodically refreshed generations. Optional
AST lineage resolves output constituents and native paths from the currently
available catalog, releasing it before hooks and execution. Missing metadata remains
explicit uncertainty. Dictionary classification and dynamic masking remain separate
stages; metadata snapshots alone do not establish output lineage.

1. **Mask PostgreSQL results first.** Prove a deterministic policy with fixture
   classifications, then integrate the selected model with measured accuracy,
   latency, and failure behavior. Management UI work is not a prerequisite.
2. **Add one warehouse.** Validate the selected Snowflake candidate's
   frontend/backend SQL, metadata, type, authentication, cancellation, and session
   matrix before production acceptance. Redshift's PostgreSQL-compatible connection
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

Optional REST column classification is implemented across backends, using one
bounded prefix, typed probabilities/confidence, and cache-associated decisions.
See [result classification](decision-model.md) and [result processing](result-processing.md).
Engine-enforced masking remains planned; the following requirements describe its intended integration.

- Group output context sources under `data_contexts`. The active `decision_model`
  source supplies bounded classification outcomes; `ast` supplies static output
  constituents without dictionary matching. Expose both named contexts to result-processing hooks and future masking policy. Decide dictionary matching placement
  when that capability is implemented.
- The application supplies the classification endpoint and questions; the shared
  REST client requires no provider SDK. The future masking policy combines its
  outcomes with context and the
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
- Keep source results separate from processed output. On cache hits and misses, apply
  current engine RBAC, result hooks, and mandatory masks before returning data. These
  processing stages are planned; they are not implemented in the current runtime.
- The application configures its REST model and questions. Confidence thresholds
  and mandatory-mask failure policy belong to the future masking stage.

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

| Component                | Capability and design requirement                                                                                                                         |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Compute lifecycle        | Provider-specific provisioning/suspension with coordination, readiness checks, and failure recovery.                                                      |
| Load distribution        | Add round-robin and least-busy allocation within the authorized compute type, preserving shared limits and cancellation. Overflow is the current default. |
| Governance integration   | Verified end-user context and external governance rules, with privacy controls for classification inputs.                                                 |
| Cache evolution          | Metadata caching, explicit cross-target equivalence, and reliable data-version invalidation.                                                              |
| Semantic caching         | Reuse compatible supersets through a local execution engine with correct ordering, NULLs, limits, filters, and aggregates.                                |
| Federation               | Plan and execute across engines with explicit type, consistency, and failure semantics.                                                                   |
| Natural-language queries | Model-provider integrations using the same authorized query execution path.                                                                               |

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
