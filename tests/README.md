# Test strategy

Tests cover meaningful observable behavior of a component or function and prevent
real regressions. Prefer complete component scenarios and focused bug reproductions.
Do not duplicate small implementation logic, test trivial helpers independently,
or assert private structure. Refactoring should preserve tests when behavior is
unchanged; intended behavior changes can change contract expectations.

The gateway integration harness reads `tests/config.yml` by default. The test YAML
uses `version: 1` and the existing `targets.<target>.logins.<login>` layout, with one
PostgreSQL target and one login. It may also contain the supported optional `gateway`
block; gateway credentials remain environment-only. Set
`YASP_TEST_CONFIG` only to override the config file path. The test target password is
resolved from `YASP_TEST_TARGET_PASSWORD`. Frontend username, password, and database
label come from `YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD`, and
`YASP_GATEWAY_DATABASE`, supplied by process environment or optional repository-root
`.env`; process values take precedence.

The target database in the test config is the `postgres` maintenance database used to
create a fresh `yasp_test_<UUID>` database from `template0`. The test login must have
`CREATEDB`; it can be a non-superuser. The harness loads the SQL fixture into the
new database and drops only that database with `FORCE` during teardown. It does not
modify PostgreSQL roles or populate a shared development database.

Run on WSL/Linux with Rust 1.89, Python 3, `psql`/libpq, and a local PostgreSQL
server. The configured loopback address and `127.0.0.1:6432` must be available.

```sh
cargo test --locked --all-targets
```

To use a different test configuration file:

```sh
YASP_TEST_CONFIG=/path/to/test-config.yml cargo test --locked --all-targets
```

The optional gateway block can set PostgreSQL protocol, a loopback listener, frontend
session/frame/SQL limits, query/read/startup/shutdown/write timeouts, and
`tls.mode: local_development` (plaintext). Omitted values use defaults; backend frame
size stays fixed at 8 MiB. Prepared-statement and portal limits from the full
reference are not supported runtime settings.

The test scope exercises simple-query forwarding for supported SELECT/read-only
WITH queries, subqueries, joins, aggregates, unions, catalog queries, and SHOW
variable/ALL requests. It checks raw PostgreSQL metadata and text-row forwarding,
query/frame bounds, and backend cleanup. SQL inspection defaults to 1 MiB, with 4,096
significant tokens, and parser recursion limit 64. Unicode escaped identifiers
(`U&"..."`) are unsupported. `set_config` is allowed only with the constant
setting-name argument `application_name`; its value may be an expression. Calls
targeting serialization or resource settings are rejected. PostgreSQL-reported
protected text parameter changes cause the backend connection to be discarded.

The deferred scenarios below describe coverage beyond the current milestone.

## Deferred Phase 1 scenarios

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
