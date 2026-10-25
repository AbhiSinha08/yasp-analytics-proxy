# YASP — Yet Another SQL Proxy

## Product goal

YASP is a reusable SQL proxy for analytics across multiple database engines. BI
applications use one connection configuration; the consuming application supplies
context, source-role selection, and query/result behavior. The engine owns protocol
handling, execution, resource limits, and validation. Database roles enforce source
access. Engine-enforced dynamic masking and management tools are later capabilities.

Client protocol, source engine, and SQL translation are separate capabilities.
Adding a warehouse adapter does not provide its native client protocol or arbitrary
cross-dialect SQL. Support is a tested SQL, type, and client subset, with explicit
errors outside it. Full PostgreSQL transparency and guaranteed sensitive-data
detection by a probabilistic model are outside that promise.

This file owns product direction, phase boundaries, and acceptance requirements.
Use [architecture](architecture.md) for current flows, [configuration](configuration.md)
for settings/defaults, and the [documentation index](README.md) for detailed contracts.
[deliverables.md](deliverables.md) tracks work within these phases.

## Components and boundaries

Keep one Rust library plus a thin host application. Database adapters own native
protocols, types, pools, sessions, cancellation, and error conversion. Shared
execution owns stage order and validates application decisions. See the current
[component map](architecture.md#component-ownership) for source ownership.

### Reusable library and organization extensions

Organizations register compiled Rust callbacks or trusted external Python files
before serving. Context interpretation, role mappings, model questions, and
transformation rules belong to the application. Core owns their invocation and
bounds; callbacks cannot replace authenticated identity or supply arbitrary source
credentials. See [extension contracts](extensions.md).

Add contracts only for active capabilities. Prefer concrete dispatch and existing
dependencies; introduce reusable traits when another implementation needs them.
Avoid runtime Rust compilation, dynamic plugin discovery, speculative crates, and
a universal value model that loses source precision.

## Current implementation

The [architecture](architecture.md) and [multi-engine compatibility matrix](multi-engine.md)
describe implemented behavior. The runtime includes bounded read-only simple queries,
source-role routing, query context/modification, shared datasets/catalog, optional
lineage/classification/result callbacks, and opt-in Redis result caching. PostgreSQL
is the primary source; Snowflake and the Redshift frontend profile have explicit
compatibility limits. TLS is configured independently for each connection.

Prepared statements, binary results, lifecycle hooks, engine RBAC interpretation,
mandatory masking, automatic translation, and management remain planned. Phase goals
below include both delivered foundations and work still required for acceptance.

## Phase 0 — Project foundation

**Outcome:** a buildable library/host, clear component ownership, example settings,
and reproducible development/test setup.

Keep modules flat until their responsibilities justify directories. Split crates
only for independent reuse or release. The Python import probe establishes linking
and imports; callback behavior is verified separately. See [development](development.md)
and [tests](../tests/README.md) for commands and prerequisites.

## Phase 1 — PostgreSQL analytics proxy

**Goal:** a read-only proxy with one BI connection configuration and application-owned
source-role policy. A BI connection can use multiple transport sessions; different
queries may select different source logins without per-role BI connections.

### BI context and client validation

Use the current [query-context contract](extensions.md#query-context) and
[observed BI formats](extensions.md#observed-sql-context-from-local-bi-traffic).
Service authentication identifies the integration; SQL comments do not prove viewer
identity. The application defines trust, absent-context behavior, and role policy.
Verified end-user context remains a separate integration requirement.

Validate the intended BI client with result caching disabled before enabling it.
Record client/driver versions, startup and discovery traffic, query/cancel behavior,
and BI-side caching. Connectivity alone does not establish access isolation.

### Gateway and query processing

Current parsing and simple-query stage order are described in
[architecture](architecture.md#query-flow). Remaining protocol work includes
prepared/parameterized queries, descriptions and requested formats, broader session
settings, and suspended portals.

Preserve SQL unless a configured modifier replaces it. Parsing is syntax inspection,
not authorization. Inspect the complete effective statement; reject writes, writable
CTEs, SELECT INTO, write-capable EXPLAIN ANALYZE, role/session authorization changes,
COPY, replication, and unsupported session-dependent behavior. Database grants and
read-only transactions remain necessary: SELECT functions can have side effects.

### Protocol and value compatibility gate

Prove Parse, Bind, Describe, Execute, Close, Flush, Sync, portal suspension/resumption,
error recovery, and cancellation with psql and the intended JDBC driver before
claiming extended-protocol support. Serialize execution per frontend session;
preserve failed transaction state and discard extended messages until Sync after
an error as required by the protocol.

Define supported bind/result formats for NULLs, arbitrary-precision NUMERIC,
timestamps/infinity, bytea, arrays, JSON, domains, and native metadata. Opaque bytes
are reusable only when source/requested types and formats agree; they do not provide
binary-to-text conversion or Python values. Keep lossless codecs in adapters and
reject unsupported conversions. NUMERIC cannot generally fit in a fixed decimal type.

Bound frames and rows before allocation, plus queued bytes and decoded copies.
Prove those bounds for every new driver/codec; batch row limits alone are insufficient.
The current bounded transport is documented in [architecture](architecture.md#values-and-cache-ownership).

### Backend execution and routing

Current defaults, overflow and ownership live in [configuration](configuration.md#routing-and-pools)
and [routing](source-access-routing.md). Preserve these requirements when adding
prepared statements or richer transactions:

- Route every executable query through configured-bound validation, including cache
  hits. Source credentials remain adapter-owned.
- Pin explicit transactions to one physical connection and role. BEGIN may remain
  pending until an operation needs a source; control/cancellation needs no fresh
  selection. Bound idle transaction and suspended-portal lifetime.
- Keep frontend prepared statements independent of pooled connections. Parse/Describe
  may need routing before Bind values exist; deny preparation if required metadata
  is unavailable. Retain SQL/context, reauthorize executions, reprepare on the
  selected connection, and verify advertised parameter/result descriptions.
- Bind and suspended portals retain their execution route. Never change roles or
  result descriptions halfway through a portal.
- Replay supported settings on checkout. Drain/cancel work, roll back, reset, and
  verify idle state before reuse. Pool liveness checks alone do not prove cleanup;
  invalidate driver prepared caches when resetting them with DISCARD ALL.
- Retain the lease through cleanup or discard it. Synchronize cancellation and
  release; uncertain cancel delivery must not reach the next borrower. A cancelled
  Rust wait alone does not stop source execution.
- Count open/creating/idle/cleaning and retiring resources in total capacity.
  Pool return is separate from physical destruction or compute suspension.

### Source RBAC and result processing

Current [result callbacks](result-processing.md) provide optional presentation
rules. Engine RBAC interpretation and mandatory masking remain planned.

Source roles own grants, RLS and restricted views. Use non-owner, non-superuser
logins without BYPASSRLS or escalation membership, and test actual role privileges,
including functions and views. Policy data, role mappings, missing-user behavior,
and any distinct discovery policy belong to the consuming application.

Output aliases alone do not establish lineage or prevent inference through
expressions/aggregates. Required policy and result failures deny the affected query;
unknown SQL cannot bypass a mandatory check. Masks preserve schema, using compatible
text or SQL NULL where appropriate.

### Hooks

[Extensions](extensions.md) owns stage requests, binders, ordering, and failure
contracts; [hook examples](../hooks/README.md) owns runnable registrations.
Lifecycle hooks and prepared-query rewrite contracts remain future work. Add their
bounded inputs, cancellation and failure semantics when that stage is active.

Keep authenticated identity separate from application values. Source results remain
raw through cache capture and classification; optional transformations run before
future mandatory masks. Validate schema, types, row counts and bytes after edits.
No later extension may restore raw data after mandatory masking.

Embedded Python is trusted code. Waiting timeouts cannot terminate an executing
function or release its retained resources. Hard termination, conflicting package
versions and parallel CPU isolation require processes; package upgrades use process
replacement. See [Python runtime](extensions.md#external-python) and
[embedding setup](development.md#python-embedding).

### Redis result cache

The current [cache contract](configuration.md#redis-result-cache) owns scope, TTL,
limits, eligibility and miss-on-error behavior. Validate the cache with the intended
BI client before enabling it. Redis server memory/eviction and access controls are
separate deployment responsibilities.

A hit skips source grants and RLS until expiry. Future engine policy and mandatory
masking must run on both hits and misses before output. Keep complete raw results
separate from caller-specific processing; reject partial or uncertain publications.

### Configuration and operations

Resolve secrets through environment references, validate settings before startup,
and preserve independent TLS policies. Use redacted logs and operational counters
for latency, routing, waits, hooks and cache outcomes; avoid credentials, bound/result
values and raw samples. Support bounded shutdown and cleanup.

Future prepared statements/portals need per-session count/byte limits in addition
to existing SQL/frame, active-session, hook, cache and write deadlines. Account for
Rust/Python copies and executing callbacks after their waiting timeout.

### Acceptance

The intended BI client discovers authorized metadata, runs supported parameterized
questions, and cancels work. Cover role changes on the same frontend session, source
access isolation, connection reuse, transaction/portal pinning, prepared statements
across roles, value fidelity, callback failures, cache isolation, outages, oversized
results and cleanup. Compare throughput, latency, memory and hook overhead against
direct PostgreSQL before performance claims. Production readiness requires its own
operational and security validation.

### Remaining Phase 1 order

Validate BI context/compatibility and deployment cache behavior. Add basic prepared
statements and portals, then broader SET/transaction support, suspended portals and
binary results as concrete client requirements demand.

## Phase 2 — Additional engines, dynamic masking, and management

**Outcome:** validated warehouse adapters, policy-driven dynamic masking, and
authenticated management tools.

| Capability              | Remaining requirement                                                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Backend adapters        | Prove real-source SQL, metadata, types, authentication, cancellation, roles and warehouse isolation beyond emulator coverage.            |
| SQL translation         | Define a supported subset, translate after target selection, then revalidate SQL and refresh lineage/query identity before cache lookup. |
| Dynamic masking         | Combine data contexts and application policy before output, including cache hits.                                                        |
| Management and UI       | Authorize inspection, configuration, scoped cache clearing and SQL Studio through the normal execution path.                             |
| Configuration lifecycle | Activate coherent immutable policy/hook/model/settings generations; retire affected pools and drain or cancel pinned work.               |
| Redis query queue       | Define bounded admission, deadlines, cancellation, worker ownership and recovery beyond local capacity waits.                            |

Current Snowflake and frontend limits are in [multi-engine compatibility](multi-engine.md).
A native warehouse client protocol is a separate gateway capability.

### Execution milestones

1. **Mask PostgreSQL results.** Prove deterministic policy with fixture classifications,
   then measure the selected model's accuracy, latency and failure behavior.
2. **Validate a warehouse.** Prove its frontend/backend matrix against the real source.
   Metadata adaptation and translation are separate gates; a PostgreSQL-compatible
   connection does not prove equivalent catalogs or types.
3. **Expose management, then UI.** Authorize changes and query execution; redact
   inspection responses. Cache invalidation must prevent in-flight old work from
   repopulating the cleared namespace/generation.
4. **Activate configuration generations.** Queries retain a consistent generation;
   transactions/portals retain compatible state until drained or cancelled. Rotate
   credentials by retiring pools; replace processes for Python package changes.
   Immediate revocation needs cancellation/invalidation rather than passive draining.

### System 1 decision-model integration and dynamic masking

[Classification](decision-model.md), [lineage](ast-lineage.md) and optional
[result hooks](result-processing.md) are implemented. They expose raw output data
contexts; dictionary matching and mandatory engine masks remain separate work.

- Combine named data contexts, application context and selected role in the future
  masking policy. The application supplies model endpoints/questions; confidence
  thresholds and mandatory-mask failure handling belong to that policy.
- Finish a column decision before emitting affected values. A bounded prefix can
  miss later sensitive values. Per-batch classification cannot retract earlier
  output; whole-result policy requires a size cap or conservative masking from
  the start. Never release an unmasked prefix while awaiting a required verdict.
- Identify outputs by ordinal and metadata; names, expressions and joins do not
  prove lineage. Accept bounded tags/scores, never executable code/SQL. Unknown
  types, invalid scores, inference failure and uncertainty must mask or deny under
  explicit policy. Prefer NULL when no compatible replacement can be encoded.
- Bound inference time, input and buffering. Probabilistic sampling cannot guarantee
  detection; source-role controls remain independently necessary.
- Models see pre-mask samples. Define approved endpoints, minimization, retention
  and logging policy; disable raw sample logging. Keep local/native/GPU model
  dependencies in the application environment and hard-kill work in processes.
- Apply current result hooks and future mandatory policy to both raw cache hits and
  misses. Preserve schema/type fidelity and prevent later hooks from restoring data.

Acceptance covers restricted/permitted columns, type fidelity, decisions before
emission, uncertainty, inference failure, resource bounds, and isolation across roles
and model/policy versions. Management actions cannot bypass execution policies.

## Phase 3 — Advanced analytics and integrations

**Outcome:** independently scoped capabilities built on validated execution boundaries.

| Capability               | Requirement                                                                           |
| ------------------------ | ------------------------------------------------------------------------------------- |
| Compute lifecycle        | Provider provisioning/suspension, coordination, readiness and recovery.               |
| Load distribution        | Round-robin/least-busy within the authorized type; preserve limits and cancellation.  |
| Governance               | Verified end-user context and external rules, with classification privacy controls.   |
| Cache evolution          | Metadata caching, explicit target equivalence and reliable data-version invalidation. |
| Semantic caching         | Correct superset reuse with ordering, NULLs, filters, limits and aggregates.          |
| Federation               | Explicit cross-engine type, consistency and failure semantics.                        |
| Natural-language queries | Model integrations using the authorized execution path.                               |

Each capability needs bounded scope and acceptance criteria before implementation.
SQL parsing alone establishes neither translation nor semantic equivalence.

## Test and documentation principles

Follow [development conventions](development.md#rust-structure-and-testing-norms)
and the [documentation ownership rules](README.md#maintaining-these-docs). Tests cover
observable contracts and regressions; documentation describes final behavior and
marks future work explicitly. Session history belongs in version control.

## References

See [dependency responsibilities](development.md#installed-dependency-responsibilities)
for the installed libraries and [protocol/client verification](../tests/README.md)
for the supported test matrix.
