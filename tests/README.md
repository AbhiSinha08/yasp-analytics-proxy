# Test strategy

Tests cover meaningful observable behavior of a component or function and prevent
real regressions. Prefer complete component scenarios and focused bug reproductions.
Do not duplicate small implementation logic, test trivial helpers independently,
or assert private structure. Refactoring should preserve tests when behavior is
unchanged; intended behavior changes can change contract expectations.

The scaffold contains no runtime features or placeholder success tests. Rust
integration tests live here as components become executable. A function-level test
is useful when it covers a meaningful contract, boundary case, or known bug that
is not adequately exercised by component scenarios.

For the current dependency scaffold, check locked default/`python` builds, formatting,
Clippy, and the [Python import probe](../docs/development.md#python-embedding).
Test imports from the application's venv inside the Rust process, including a native
standard-library module such as _ssl, and verify that a missing module fails. These
checks do not establish proxy, worker, or service compatibility.

## Phase 1 scenarios

1. **Gateway and execution:** simple/parameterized queries, prepared statements,
   metadata/type fidelity, session/transaction lifecycle, cancellation, errors,
   and disconnect cleanup through the client/backend boundary.
   Exercise Describe before Bind, text/binary binds and results, portal suspension,
   failed-transaction/Sync recovery, and explicit rejection of multi-statement queries.
   Cover read-only enforcement against writable CTEs, SELECT INTO, function side
   effects, and attempted SET ROLE/SESSION AUTHORIZATION/read-write changes.
2. **Metabase:** the supplied Docker instance connects, discovers authorized
   metadata, runs questions/dashboards, and cancels a query. Record its version
   and endpoint in the integration environment.
   Capture actual query metadata; test native/query-builder/prepared/background
   queries with BI caching disabled. Verify shared discovery and field samples are
   safe for BI consumers and that untrusted comments cannot elevate selection.
3. **Source RBAC and routing:** queries on one frontend session with different BI
   metadata select different source-role credentials and receive the data permitted
   by those roles. Exercise idle reuse, bounded creation/acquisition, denied missing
   or unmapped metadata, invalid target/login decisions, and restricted discovery.
   Transaction role changes fail; prepared statements and supported session settings
   work across role pools without leaking state. Optional masks preserve types.
   A bare BEGIN followed by a routed query pins that route; metadata-free rollback
   succeeds after a denied query. Prepared metadata cannot silently change when the
   role/target changes. Verify reset after cancellation and late cancellation after
   pool return cannot affect another query.
4. **Extensions:** application-supplied context derivation and hooks execute through
   library contracts without changes to core modules; the selector consumes policy without receiving
   passwords; selection runs before cache hits; rewrites preserve binds, result
   batches preserve schema, and errors/saturation follow documented limits.
   A timed-out Python callback keeps its worker permit; queue bytes and transformed
   output are bounded. Required imports fail at startup. Rust-only builds need no
   interpreter. Optional transformations cannot run after mandatory masks.
5. **Cache:** approved repeats avoid backend execution, security/session/target
   context and selected source role isolate results despite shared frontend credentials;
   expiry, size caps, corruption, outages, and
   interrupted execution preserve correctness.
   Exercise grant/RLS changes under the declared authorization-staleness policy,
   oversized cache reads, result-format differences, and bounded Redis timeouts.
6. **Operations:** backend loss, pool exhaustion, slow consumers, oversized results,
   and shutdown release resources. Compare performance with direct database access.
   Include one oversized row/frame, total pool capacity across targets, idle
   transactions/portals, and cache/hook buffers under concurrent slow consumers.

## Phase 2 dynamic masking

Exercise model classification and masking as a complete result-processing component:
role-dependent output, schema/type fidelity, no unmasked prefix, uncertain decisions,
inference failure, bounded inputs/buffers, and cache isolation across model/policy
versions. Use stable classification fixtures for deterministic regression scenarios;
assess model accuracy separately from transformation correctness.
Include duplicate output names, derived expressions, unsupported native types,
sensitive values appearing after the sample, invalid model responses, and failure
after earlier authorized batches. Verify external sample transmission is permitted
by policy and raw samples are excluded from logs.

## Phase 2 management and generations

Configuration replacement retains one consistent generation per query and compatible
state for transactions/portals. Test draining/cancellation, credential rotation,
scoped cache clearing during active fills, and package changes through process
replacement. Unauthorized management/Studio requests fail before access to secrets,
configuration mutation, or execution. UI execution uses the same policy path.

## Later adapters

Exercise the backend execution contract against each actual engine, including
metadata, types, unsupported capabilities, identity isolation, and failure recovery.
Do not require shared components to know a particular driver's internal structure.

Use dedicated databases and a unique Redis namespace. Document credentials and
fixture teardown with runnable tests. Share helpers only when tests need them.
