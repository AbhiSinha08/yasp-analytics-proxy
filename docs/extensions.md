# Extension boundaries

A consuming application depends on `yasp`, resolves secrets and policy at startup,
registers callbacks, constructs backend pools and gateway configuration, then calls
`gateway::serve_routed`. `gateway::serve` supports a fixed PostgreSQL backend through
the same session handling. Custom compiled code belongs to that application or its
own crate; external Python files are deployment resources. Core modules invoke
contracts and validate decisions without interpreting organization policy. Shared
execution receives authenticated identity and bounded services; source driver and
frontend password/TLS objects remain in their adapters.

Hosts can also construct a shared [schema catalog](catalog.md), attach it with
`GatewayConfig::with_catalog`, and retain its `Arc<Snapshot>` for later AST binding.
Catalog source identity and refresh settings belong to the host; core owns bounded
loading, atomic publication, expiry and frontend response handling. Schema coordinates
are facts, not an authorization or masking decision.
The repository host attaches the PostgreSQL or Snowflake provider through
`schema_discovery.catalog`, sharing its configured discovery identity and adapter
capacity. Mixed or Snowflake-only hosts register bindings in `backend::Backends`
and use `gateway::serve_backends`.

For encrypted deployments, see [TLS setup](tls.md#library-applications) and the
[standalone library example](../examples/tls_gateway.rs). The application owns
certificate provisioning and paths; core owns negotiation and verification.

## Implemented callback stages

Query-hook names remain under [yasp::hooks](../src/hooks.rs); dataset APIs are under
[yasp::datasets](../src/datasets.rs). Private modules own each stage's contract.
Reusable repository callbacks are under [hooks/builtin](../hooks/builtin),
and Python examples are under [hooks/examples](../hooks/examples).

| Stage             | Binding and data                                                                                                                              | Result/failure policy                                                                                                                                                                           |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Input options | `GatewayConfig::with_input_options(f)`; synchronous `InputRequest` with byte-bounded SQL and listener dialect. | Typed input grammar and native-query classification. No credentials, routing decisions, or discovery privileges. |
| Context           | `bind_context(state, f)`; async borrowed `ContextRequest` with frontend identity/database, IDs, and immutable parsed query.                   | Owned JSON mapping; invalid/error/timeout/saturation supplies empty context. Cancellation stops the query.                                                                                      |
| Selection         | `bind_select_backend` or `bind_select_backend_async`; borrowed `SelectionRequest` with frontend user, IDs, parsed query, and derived context. | Partial `BackendDecision` (or an explicit pair convertible into it); invalid/ambiguous selection is `42501`, failure is `XX000`. The engine resolves configured defaults and allocates compute. |
| Cache eligibility | `bind_cache_eligibility` or `bind_cache_eligibility_async`; the same selection request.                                                       | Boolean that may bypass caching; errors bypass without failing execution. Cannot expand SQL or resource limits.                                                                                 |
| Dataset loading   | `bind_dataset_loader(state, f)`; async `DatasetLoadRequest` with dataset name and `startup` or `scheduled` reason.                            | JSON object loaded before service starts and published to the shared dataset registry. Startup failure prevents serving; refresh failure preserves the previous value.                          |

Library hosts can create `backend::ConnectionBudget::new(total)` and pass it to
`PostgresBackend::with_budget`, `PostgresBackends::with_bindings_and_budget`, or an
empty `Backends::with_budget` registry. Supplied PostgreSQL registry settings must
name the same total. Existing PostgreSQL constructors allocate a standalone budget;
`Backends::new(postgres)` adopts that registry's budget, while `without_postgres()`
uses the default total. `with_snowflake` automatically attaches an unused adapter.
Register bindings before issuing catalog or data requests. Reusing an adapter under
the same budget is valid; moving it between budgets is rejected, even when idle.
Standalone Snowflake use establishes its own budget on first admission. The YAML
host supplies one configured budget before loading metadata or accepting clients.

The router resolves and validates every selection, including cache hits, and delegates execution; the backend owns
credentials and connection acquisition. Callbacks receive no backend passwords or
driver objects. Authenticated frontend identity stays separate from derived context.
SQL comments are untrusted hints;
BI viewer identity and role policy require an application-defined trust contract.

When datasets are configured, context, selection, and eligibility requests also
provide an optional `DatasetReader`. `get(name, key)` reads one field,
`get_many(name, keys)` reads up to 64 fields, and `read(name)` reads the complete
dataset within the same limits. Missing fields are omitted from `get_many`;
`get` returns `None` for a missing field. Unknown dataset names and unavailable,
malformed, or oversized data return typed errors. Each lookup reads current Redis
data. A callback's reader expires when that callback completes or is cancelled.
See [shared application datasets](configuration.md#shared-application-datasets).

Rust state/functions must meet the binder's `Send + Sync + 'static` requirements.
Synchronous selection and eligibility must be fast and nonblocking.
Use `bind_select_backend_async` or `bind_cache_eligibility_async` for asynchronous
dataset reads; `gateway.async_hook_timeout_ms` bounds these waits.
Context uses cooperative async I/O; its deadline cannot interrupt blocking Rust.
See [binding examples](../hooks/README.md).

Input options run before tokenization and parsing. The callback must be cheap and
nonblocking. Repository hosts can bind `hooks::builtin::input_options::metabase`
with their logical database's native dialect, or `metabase_with_default` with an
optional unmarked-data dialect. `InputOptions::native_after_compatibility` preserves
frontend setup/catalog handling before using the configured native grammar for data.
Explicit MBQL stays in the frontend grammar. Other consumers can supply their
own closure without interpreting BI comments in the core engine. Original SQL,
immutable AST, and `ParsedQuery::input_options()` reach context and selection.
Adapter input validation runs after ordinary routing and before pool acquisition
or cache lookup. PostgreSQL enforces explicit native source affinity; Snowflake
forwards read-only data SQL without a dialect gate or translation. All backends and
frontend/dialect combinations use the shared cache path when enabled; adapter
validation still runs before a hit can replay.


## Owning shared datasets

Keep a `Datasets` value alive while serving. Register one `DatasetLoader` for each
configured name, then await `Datasets::start(settings, redis_url, loaders)` before
opening the listener. It requires a fresh successful load for every name, including
names already present in Redis. Pass `datasets.reader()` through
`GatewayConfig::with_datasets`; call `datasets.shutdown().await` after serving,
including when serving fails. Dropping the owner cancels its refresh admission.

Compiled loaders use `datasets::bind_dataset_loader(state, function)` and return
an owned `serde_json::Map<String, Value>`. Request `name` identifies the dataset;
`reason` is `startup` or `scheduled`. The engine owns storage and bounds; loaders
own fetching and data meaning. The supplied host provides
[`builtin.dataset.load_json`](../hooks/builtin/dataset.rs) for small JSON files;
[the Rust example](../hooks/examples/datasets.rs) shows an application-owned loader
that returns data in code. Application hosts can also register Python loaders with
`PythonRuntime::register_loader`; they follow the same plain JSON contract.

For Python loaders, create the process runtime with
`PythonRuntime::new_with_dataset_limit(settings, max_dataset_bytes)` and register
them using `register_loader(path, callback, params, max_dataset_bytes, timeout)`.
The separate loader queue has capacity one and a byte budget derived from the
larger of the dataset and Python payload limits. Ordinary `PythonRuntime::new`
creates only the query worker. Both modes initialize one interpreter per process.

Each hook invocation allows 32 dataset calls. Redis operations have a 100 ms wait;
`get_many` and `read` allow at most 64 fields and one `max_lookup_bytes` response.
Read buffers have an aggregate budget of at least 32 MiB, reserving three times the
lookup limit per active read. Python lookups charge the returned bytes against the
query worker's byte budget until its job finishes, including after a waiting
timeout. Each invocation also has a cumulative 32-response byte ceiling.
These are serialized-data limits, not a sandbox or a bound on arbitrary callback
heap allocations. The dataset helper itself is a service object; other request
fields and lookup results are copied JSON values.

## External Python

Enable the `python` feature and create one `PythonRuntime` per process before
serving. Clone it for additional registrations. The repository host shares one
runtime among context, selection, and eligibility and resolves references in
[host/hooks.rs](../src/host/hooks.rs).

```yaml
hooks:
  select_backend:
    type: python
    path: ../hooks/examples/select_backend.py
    callback: select_backend
    params:
      target: primary
      backend_login: reader
```

Files/functions are registered before listener startup. Every trusted synchronous
function receives `function(request, params)` with fresh copied JSON-compatible
objects. Selection/eligibility requests contain `authenticated_user`, `client_id`,
`query_id`, original `sql`, and `context`. Context input also includes
`frontend_database` and has no derived context yet. Selection returns exactly
a dictionary with optional `target`, `backend_login`, and `compute_type` keys,
or `None` to deny; context returns a dictionary
or `None`; eligibility returns a boolean.

The boundary accepts exact built-in null/bool/string/list/string-keyed dictionary,
signed/unsigned 64-bit integers, and finite float values, with nesting limited to 64. It is not a database value codec. Unsupported values/coroutines fail. Python
errors are redacted; callbacks remain responsible for their own logs and side effects.

One dedicated worker runs registration and invocation outside Tokio. Defaults admit
32 waiting jobs, a 4 MiB queued-plus-active input budget, 2 MiB individual payloads,
and a 2-second wait including queue time/registration. Executing timed-out or
abandoned Python retains its worker and byte reservation until it finishes; expired
queued jobs do not run. Shutdown stops admission and wakes waiters without joining
executing code. Embedded Python is trusted code, not a sandbox or hard-kill boundary.
All hooks share imports, package versions, interpreter state, and the CPython GIL.
See [interpreter/package setup](development.md#python-embedding).

For dataset-aware Python callbacks, `request["datasets"]` exposes synchronous
`get(name, key)`, `get_many(name, keys)`, and `read(name)` helpers. They return normal
Python JSON values, not encoded JSON strings, and do not keep an expiring process-local
copy. Dataset loaders use a dedicated worker but share the callback interpreter and
GIL. Existing `PythonCallback::call(request)` remains available; dataset-aware calls
use the optional reader. See the runnable [dataset examples](../hooks/README.md#shared-datasets).

## Planned query and result stages

Query rewrites, result callbacks, lifecycle hooks, engine policy/masking, and model
integration have no active runtime contracts or accepted configuration yet. This is
the intended direction, not the current [query pipeline](architecture.md#query-flow):

```mermaid
flowchart TD
    Context[Application context] --> Rewrite[Optional query rewrite]
    Rewrite --> Validate[Revalidate SQL and binds]
    Validate --> Route[Role selection and configured bounds]
    Route --> Source[Load source result from cache or backend]
    Source --> Transform[Optional result transformations]
    Transform --> Mask[Mandatory policy and masking]
    Mask --> Check[Validate schema, types, rows and bytes]
    Check --> Client[Client response]
```

Future stage requests should expose original/effective SQL, derived context,
engine-validated route/execution decisions and cache outcome when known, and bounded
result metadata/batches where appropriate. Before execution, callbacks may propose
supported rewrites, denial, rerouting, or cache bypass only where their stage allows;
the engine validates each proposal centrally. Later stages observe the decided flow
immutably. No stage may replace authenticated identity, exceed configured bounds,
bypass mandatory policy, or change transaction/portal decisions already pinned for
execution. These stage contracts remain future work.

Rewrites must preserve parameter positions/types and repeat dialect/read-only
validation. Result stages must preserve schema and ordering, use lossless native
values, and handle NULLs and duplicate column names by ordinal/metadata. Optional
transformations precede mandatory masks; later hooks cannot restore raw values.
Run policy and masking on both cache hits and misses. A partially emitted result
cannot be retracted; whole-result decisions require bounded buffering or conservative
masking before output. Model confidence/failure policy and cache generations belong
to that future contract. See [Phase 2](PLAN.md#phase-2--additional-engines-dynamic-masking-and-management).

## Adding capabilities when their phase is active

| Capability                                | Local ownership and required proof                                                                                                                                                                                                                                                                                                                         |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Another backend                           | Add its adapter for drivers, native values, pooling, session state, cancellation, and error conversion. Add concrete dispatch when needed. Define supported SQL/dialect and PostgreSQL frontend mappings, including native metadata discovery. Prove precision, NULLs, timestamps, transaction cleanup, and source-identity cache isolation before declaring support. Shared execution supplies cache policy and response encoding; adapters do not opt into caching. |
| Another client protocol or BI integration | Keep protocol framing/authentication/encoding in its frontend adapter. A warehouse driver does not provide another frontend protocol. Record real client/version/startup/discovery/query/cancel traffic; verify metadata trust, absent-context behavior, prepared/binary needs, and BI-side caching. Current extractor names are placeholders.             |
| RBAC and identity mapping                 | Supply application-owned trusted context, local policy, and role mappings. Preserve configured route bounds and source grants. Current source roles enforce access; engine policy must cover cached and uncached results before output.                                                                                                                    |
| Query/result/lifecycle hooks              | Define only the active stage's bounded request/result/failure contract. Preserve cancellation, lossless values, schema, and resource accounting; implement stages in query order rather than a generic plugin registry.                                                                                                                                    |
| Load balancing                            | The engine allocates configured compute instances in overflow order within the selected type and source login. The selector chooses the type, without live pool-state logic. Least-busy and round-robin strategies, provider lifecycle, and prepared-state pinning are later milestones.                                                       |
| Management or UI                          | Add an authenticated host/service boundary over supported engine operations. Authorize configuration inspection and query execution; redact secrets/results, enforce request limits, and define atomic config activation/cache invalidation. No management API or UI exists yet.                                                                           |

### Observed SQL context from local BI traffic

The 2026-10-18 local capture showed these chart-query formats:

| Tool | SQL shape observed | Context available in the captured SQL |
| ---- | ----------------- | ------------------------------------- |
| Metabase | A leading line comment, `-- Metabase:: userID: <id> queryType: <type> queryHash: <hash>`, followed by native SQL. The session also sent `SET application_name = 'Metabase v0.64.2.8 [<instance-id>]'`. | `userID`, `queryType` (observed as `native`), and `queryHash`; the application name identifies the client version and instance. Repeated runs retained the query hash. |
| Redash | A leading block comment, `/* user_id: <id>, query_id: <id>, Job ID: <uuid>, Query Hash: <hash>, Scheduled: <bool> */`, followed by SQL. | User, saved-query, job, hash, and scheduled-run fields. The capture showed `Scheduled: False`; repeated runs kept the saved-query ID and hash while changing the job ID. |
| Superset | Generated SQL wraps a query as `SELECT ... FROM ( <dataset SQL> ) AS virtual_table`, often with outer grouping, ordering, or a `LIMIT 1000`. | No native user/query comment or equivalent SQL marker was visible. The test charts deliberately included a string literal such as `'Superset' AS bi_tool_name`; this is a test marker, not metadata emitted by Superset, and the outer query may omit it from its output columns. |

Treat comment fields such as `userID` and `user_id` as client-supplied hints, never as authenticated identity or authorization evidence. The SQL log records query text and execution/cache events, but does not identify whether a run came from a dashboard refresh, a chart view, a download, or an embed. The failed support-ticket join still appeared in the received SQL, so failures do not change the observed envelope. A YASP cache hit appears as a cache-served event rather than a new backend execution.

The `metabase_pg` and Redash extractors recognize the leading PostgreSQL comments
shown above. They return normalized fields including `tool`, `user_id`,
`query_hash`, and tool-specific values such as `query_type`, `query_id`, `job_id`,
or `scheduled`. Malformed or nonleading comments produce no metadata.
Redash 26.9 emits `query_id: adhoc` for an unsaved SQL-editor query. The extractor
preserves that marker as a string while retaining numeric saved-query IDs and
numeric user IDs. The same context hook therefore handles saved and unsaved runs.

Metabase's Redshift driver overrides `query->remark` to prepend a JSON `partner`
comment containing dashboard, chart, user, account, and filter metadata before the
default `Metabase::` remark. The resulting SQL remark is
`-- /* partner: "metabase", { ... } */ Metabase:: userID: ... queryType: ... queryHash: ...`.
The `metabase_rs` extractor parses that envelope and returns the partner fields
alongside the common Metabase fields. This format is implemented from Metabase's
[Redshift driver source](https://github.com/metabase/metabase/blob/master/modules/drivers/redshift/src/metabase/driver/redshift.clj#L3427-L3445);
the same envelope was observed through the YASP Redshift gateway with Metabase
v0.64.2.8.

Superset extraction is deferred: the local 6.1.0 instance had no
`SQL_QUERY_MUTATOR` configured, and its deployment-controlled mutation path has not
been verified for every query path. When DEBUG logging is enabled, PostgreSQL and Redshift
queries recognized by the gateway include `bi_remark` as the normalized object or
`null` in the `query received` event; the raw leading BI comment is omitted from
that event's `sql` field. The metadata is diagnostic only and does not affect
context or routing.

Keep PostgreSQL-specific response types and parsing inside the relevant adapters
as new implementations arrive. The present concrete APIs do not promise arbitrary
cross-engine SQL translation or PostgreSQL transparency. Add reusable traits only
when another implementation needs them, and validate each new boundary with focused
[contract scenarios](../tests/README.md) rather than internal-structure tests.
