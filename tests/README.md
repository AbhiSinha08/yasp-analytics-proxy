# Test strategy

Tests cover meaningful observable behavior of a component or function and prevent
real regressions. Prefer complete component scenarios and focused bug reproductions.
Do not duplicate small implementation logic, test trivial helpers independently,
or assert private structure. Refactoring should preserve tests when behavior is
unchanged; intended behavior changes can change contract expectations.

The gateway integration harness reads `tests/config.yml` by default. The test YAML
uses `version: 1`, configured PostgreSQL targets/logins, and the supported optional
`gateway`, `hooks`, and `backend` blocks; gateway credentials remain
environment-only. Set
`YASP_TEST_CONFIG` only to override the config file path. The test target password is
resolved from `YASP_TEST_TARGET_PASSWORD`. The YAML also configures the existing
`local2` login under `primary.logins.secondary`; its password comes from
`YASP_TEST_SECOND_PASSWORD`. Usernames are configured directly in YAML.
Frontend username, password, and database
label come from `YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD`, and
`YASP_GATEWAY_DATABASE`, supplied by process environment or optional repository-root
`.env`; process values take precedence.

The target database in the test config is the `postgres` maintenance database used to
create a fresh `yasp_test_<UUID>` database from `template0`. The test login must have
`CREATEDB`; it can be a non-superuser. The harness loads the SQL fixture into the
new database and drops only that database with `FORCE` during teardown. It does not
modify PostgreSQL roles or populate a shared development database.

Run on WSL/Linux with Rust 1.89, Python 3, `psql`/libpq, a local PostgreSQL server,
and Redis for the cache integration scenario. The configured loopback address and
`127.0.0.1:6432` must be available.

```sh
cargo test --locked --all-targets
cargo test --locked --doc
```

To use a different test configuration file:

```sh
YASP_TEST_CONFIG=/path/to/test-config.yml cargo test --locked --all-targets
```

The optional gateway block can set PostgreSQL protocol, a loopback listener, frontend
session/frame/SQL limits, query/read/startup/shutdown/write timeouts, and
`tls.mode: local_development` (plaintext). Omitted values use defaults; backend frame
size stays fixed at 8 MiB. The loadable `config/example.yml` preserves future
settings in commented blocks; prepared-statement and portal limits remain unsupported.

The test scope exercises simple-query forwarding for supported SELECT/read-only
WITH queries, subqueries, joins, aggregates, unions, catalog queries, and SHOW
variable/ALL requests. It checks raw PostgreSQL metadata and text-row forwarding,
query/frame bounds, and backend cleanup. SQL inspection defaults to 1 MiB, with 4,096
significant tokens, and parser recursion limit 64. Unicode escaped identifiers
(`U&"..."`) are unsupported. `set_config` is allowed only with the constant
setting-name argument `application_name`; its value may be an expression. Calls
targeting serialization or resource settings are rejected. PostgreSQL-reported
protected text parameter changes cause the backend connection to be discarded.
The routed integration test sends PostgreSQL `CancelRequest` packets while two routes
are active, checks SQLSTATE `57014`, verifies the other route is unaffected, and then
reuses the frontend session. Idle, unknown, and late cancel keys cannot interrupt a
subsequent client query; cancelled backend connections are discarded.

`tests/routing.rs` covers two configured database routes, selector denial/failure and
invalid-pair results, recovery on the same frontend session, per-route pool reuse,
separate login pools on one target, and isolation during pool exhaustion.
Configuration tests load the active example without secrets or database access and
cover per-pool/total pool bounds, Rust object/string references, empty-option inference,
application params in both feature builds, malformed params, and rejected future
settings. The host harness checks unknown callback names, rejects the removed routing
block, and tests selection through explicit passthrough params. The routing integration
test requires `logins.secondary` and its configured password environment variable.
It verifies granted fixture reads, owner-only table denial, database connection
restrictions, and recovery on the same frontend session. Missing credentials fail
the test rather than skipping source-login isolation.
The second login must be pre-provisioned and restricted; the fixture grants access
only to disposable test data and never creates or alters PostgreSQL roles.

The cache integration scenario accepts `YASP_TEST_REDIS_URL` and defaults to
`redis://127.0.0.1:6379/`; it generates a unique namespace for each run. The gateway
cache remains disabled unless runtime configuration sets `cache.enabled`. A cache hit
does not recheck source authorization until the entry expires.

`tests/cache.rs` exercises normalized-query hits, TTL expiry, selected source-login
scope changes, malformed and oversized entries, response validation before replay,
eligibility callback bypass and failure, row limits after configuration changes,
insufficient buffer budget, and Redis outages. It also checks selector denial on a
filled key, full forwarding of oversized
results, lossless typed values, and cancellation without a cache fill. With the
`python` feature it also checks Python callback exceptions and
invalid return values. This scenario requires the isolated test database credentials
described above and a loopback Redis endpoint; setting `YASP_TEST_REDIS_URL` changes
the endpoint without changing the unique test namespace.

## Python callback checks

With CPython/libpython available, run both feature configurations:

```sh
cargo test --locked --all-targets --features python
cargo test --locked --doc --features python
```

Set `PYO3_PYTHON` for build-time selection and `LD_LIBRARY_PATH` if the matching
shared library is outside the loader's search path; see the development setup.
`tests/python_hooks.rs` is database-free and uses temporary application scripts with
arbitrary function names. It checks isolated imports, persistent and distinct modules,
fresh params, supported values, registration errors, invalid/oversized/deep output,
queue count/byte limits, expired queue entries, and retained worker capacity after
timeout or dropped invocation. File synchronization establishes when a callback
is executing. Shutdown wakes waiters while that callback is still occupied.

`tests/context.rs` checks asynchronous Rust derivation, per-request data, encoded
context bounds, and default context after errors or timeout. `tests/context_gateway.rs`
checks fixed-backend serving, cancellation and session recovery, disconnection, and
shutdown while a context callback is pending. The Python callback scenario also
checks context copies between hooks, extractor imports, and failure defaults. The
cache scenario checks ordered context propagation and hits across different contexts
within the existing replay scope.

The Python-feature routing scenario reuses disposable source databases. It selects
both source logins and checks denial, failure, invalid pairs, malformed output, timeout,
cancellation during selection, and recovery on the same frontend session. Python
exception text never reaches the client. No application hook is embedded or required
for the Rust-only tests.
The compiled-host harness checks missing script/import and non-callable startup
failures without exposing source or exception details; Rust-only hosts reject the
same Python references before serving with a clear missing-feature error. The host
harness compares Rust and external Python passthroughs using identical params and
checks YAML-relative script paths independently of the working directory.

The deferred scenarios below describe coverage beyond the current milestone.

## Deferred Phase 1 scenarios

1. **Gateway and execution:** simple/parameterized queries, prepared statements,
   metadata/type fidelity, session/transaction lifecycle, extended-query cancellation,
   error recovery, and disconnect cleanup through the client/backend boundary.
   Exercise Describe before Bind, text/binary binds and results, portal suspension,
   failed-transaction/Sync recovery, and explicit rejection of multi-statement queries.
   Cover read-only enforcement against writable CTEs, SELECT INTO, function side
   effects, and attempted SET ROLE/SESSION AUTHORIZATION/read-write changes.
2. **BI client compatibility:** the intended client connects, discovers authorized
   metadata, runs supported query forms, and cancels a query. Record its version and
   endpoint in the integration environment. Capture actual context availability and
   test discovery, native/query-builder/prepared/background work with BI caching
   disabled. Verify shared discovery is safe for that deployment and untrusted request
   data cannot elevate selection. Exercise context derivation and default-empty
   behavior with synthetic requests; this does not verify a real BI metadata format.
3. **Source RBAC and extended routing:** queries on one frontend session with different
   trusted BI metadata select different source-role credentials and receive the data
   permitted by those roles. Exercise idle reuse, denied missing or unmapped metadata,
   and restricted discovery.
   Transaction role changes fail; prepared statements and supported session settings
   work across role pools without leaking state. Optional masks preserve types.
   A bare BEGIN followed by a routed query pins that route; metadata-free rollback
   succeeds after a denied query. Prepared metadata cannot silently change when the
   role/target changes. Transaction-state cleanup after cancellation remains deferred.
4. **Extensions:** application-supplied context and selection callbacks execute
   through library contracts; selection receives context without backend passwords.
   Context runs once after validation and before selection/cache eligibility, including
   on hits. Verify empty context for callback errors, malformed/oversized output,
   timeout, and saturation, and verify cancellation stops the query. A timed-out
   Python callback keeps its worker reservation. Required imports fail at startup;
   Rust-only builds need no interpreter. Real BI metadata extraction remains a
   separate live-client verification. Rewrites, result batches, and masks remain
   future scenarios until those hooks are implemented.
5. **Operations:** backend loss, pool exhaustion, slow consumers, oversized results,
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
