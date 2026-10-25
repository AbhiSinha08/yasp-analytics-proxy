# Extension boundaries

A consuming application depends on `yasp`, resolves secrets and policy at startup,
registers callbacks, constructs backend pools and gateway configuration, then calls
`gateway::serve_routed`. `gateway::serve` supports a fixed PostgreSQL backend through
the same session handling. Custom compiled code belongs to that application or its
own crate; external Python files are deployment resources. Core modules invoke
contracts and validate decisions without interpreting organization policy. Shared
execution receives authenticated identity and bounded services; source driver and
frontend password/TLS objects remain in their adapters.

Hosts must construct a shared [schema catalog](catalog.md), attach it with
`GatewayConfig::with_catalog`. AST analysis borrows its current snapshot only
while resolving constituents; callers do not retain a generation across execution.
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

| Stage              | Binding and data                                                                                                                                           | Result/failure policy                                                                                                                                                                           |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Query context      | `bind_query_context(state, f)` / `with_query_context_hook`; byte-bounded raw SQL, frontend identity, gateway and optional common target dialect, datasets. | `QueryContextOutput { dialect, values }`; errors reject the query. No AST, lineage, or final query ID is available yet.                                                                         |
| Query modification | `bind_modify_query(state, f)` / `with_query_modification_hook`; incoming SQL and AST, dialect, identity, query context, datasets.                          | Optional replacement SQL; reparse and validate before IDs, lineage, routing, and caching. Errors reject without fallback. No lineage or data contexts in the request.                           |
| Selection          | `bind_select_backend` or `bind_select_backend_async`; borrowed `SelectionRequest` with frontend user, IDs, parsed query, and derived context.              | Partial `BackendDecision` (or an explicit pair convertible into it); invalid/ambiguous selection is `42501`, failure is `XX000`. The engine resolves configured defaults and allocates compute. |
| Cache eligibility  | `bind_cache_eligibility` or `bind_cache_eligibility_async`; the same selection request.                                                                    | Boolean that may bypass caching; errors bypass without failing execution. Cannot expand SQL or resource limits.                                                                                 |
| Result processing  | `bind_process_result` / `with_result_hook`; bounded native batches and immutable request context.                                                          | Edit text/NULLs or filter rows, preserving schema/order; failures reject without raw fallback. See [result processing](result-processing.md).                                                   |
| Dataset loading    | `bind_dataset_loader(state, f)`; async `DatasetLoadRequest` with dataset name and `startup` or `scheduled` reason.                                         | JSON object loaded before service starts and published to the shared dataset registry. Startup failure prevents serving; refresh failure preserves the previous value.                          |

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

When datasets are configured, context, modification, selection, and eligibility requests also
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

## Query context

`hooks.query_context` runs once after frontend metadata/setup recognition and before
ordinary SQL parsing, including on cache hits. Metadata recognition always uses the
gateway profile and is independent of remarks or hook output. A recognized request
does not invoke query context, modification, selection, eligibility, or result processing.

The hook returns `{"dialect": "postgresql", "values": {"user_id": 7}}`. Both fields
are optional: the dialect defaults to the gateway and values to an empty object.
Supported dialects are `postgresql`, `redshift`, and `snowflake`. The dialect is the
grammar of incoming SQL; it selects no target, login, or compute. Hook failures,
invalid output, size/nesting violations, and timeouts reject the query with `XX000`;
cancellation reports `57014`. Authenticated identity remains engine-owned.

```yaml
hooks:
  query_context:
    type: rust
    callback: builtin.query_context.metabase
```

Params are optional. Metabase MBQL/UI exploration and absent/malformed/unknown
remarks use the gateway dialect. Explicit native remarks use `params.native_dialect`
when supplied, otherwise the common dialect of configured targets. Multiple logins
and compute instances do not affect inference. Mixed target dialects require an
explicit native dialect or a custom hook; inference never uses the discovery target.

`builtin.query_context.redash` extracts saved, `adhoc`, and scheduled query remarks.
Redash has no native/generated or dialect field, so its default is the gateway.
Optional `params.dialect` overrides the dialect for a valid Redash remark; missing
or malformed remarks retain the gateway default.

```yaml
hooks:
  query_context:
    type: rust
    callback: builtin.query_context.redash
    params:
      dialect: postgresql # Optional: Redash SQL written for PostgreSQL.
```

Superset remark formats and any other custom BI metadata belong to application
hooks. No default Superset parser is registered. Custom Rust/Python providers use
the same `dialect`/`values` output contract. Successful empty output `{}` uses defaults;
errors do not silently fall back. Downstream hooks read application values through
`request.query_context` in Rust or `request["query_context"]` in Python.

Rust providers return `QueryContextOutput`; its optional `dialect` defaults to the
gateway and `values` is an owned `serde_json::Map`. A values-only Rust provider may
return `values.into()`. Use cooperative async I/O: deadlines cannot interrupt
blocking Rust. Raw request fields are `authenticated_user`, `frontend_database`,
`client_id`, `sql`, `gateway_dialect`, and `default_target_dialect`; Python receives
their JSON equivalents plus an invocation-scoped dataset helper when configured.

```python
def query_context(request, params):
    # Parse the application's configured Superset/custom remark format here.
    return {"dialect": "postgresql", "values": {"user_id": 7}}
```

Modification receives the incoming parsed AST without lineage. Selection,
eligibility, and result hooks receive the effective AST and downstream data
contexts. A future translator compares the returned incoming dialect with
the selected target dialect; see [planned translation](multi-engine.md#planned-sql-translation).

## Query modification

Register `bind_modify_query(state, f)` with `GatewayConfig::with_query_modification_hook`,
or configure `hooks.modify_query`. The [query modification reference](query-modification.md)
owns the Rust/Python request, replacement validation, failure rules, and examples.

## Result processing

Register `bind_process_result(state, f)` with `GatewayConfig::with_result_hook`, or
configure `hooks.process_result`. The [result processing reference](result-processing.md)
owns native batch APIs, allowed edits, budgets, failures, and processing on raw cache hits.

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
runtime among context, selection, eligibility, and result processing and resolves references in
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
`query_id`, effective `sql`, `query_ast`, `data_contexts`, and `query_context`.
`query_ast` uses sqlparser 0.63 Serde syntax; `data_contexts.ast` contains optional
local output lineage. Rust requests borrow the same `DataContexts` alongside their
existing `ParsedQuery`. Python receives fresh copies, so callback mutations cannot
change later hooks. AST lineage performs no dictionary lookup or classification.
See [AST lineage](ast-lineage.md). Query-context input uses the raw request described above and contains no AST, lineage, or final query ID. Selection returns exactly
a dictionary with optional `target`, `backend_login`, and `compute_type` keys,
or `None` to deny; query context returns the `dialect`/`values` envelope; eligibility returns a boolean.

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

Current ordering is shown in [architecture](architecture.md#query-flow). Optional
[REST classification](decision-model.md) and result callbacks are implemented;
lifecycle hooks, translation, dictionary matching and engine-enforced policy/masking
remain planned. [Phase 1 hooks](PLAN.md#hooks) and
[Phase 2 masking](PLAN.md#system-1-decision-model-integration-and-dynamic-masking)
own the future stage requirements, including immutable decisions, prepared-query
rewrites, and mandatory processing on cache hits and misses.

## Adding capabilities when their phase is active

Keep source drivers, native values, session behavior and error conversion inside
adapters. Keep frontend framing/authentication in protocol adapters. A new adapter
must prove the SQL, type, metadata, isolation and cleanup contract described in
[the plan](PLAN.md#phase-2--additional-engines-dynamic-masking-and-management) and
[multi-engine compatibility](multi-engine.md). Extend concrete dispatch as needed;
add shared traits only when another implementation requires them.

### Observed SQL context from local BI traffic

These are supported diagnostic envelopes, not authenticated end-user identity.
Metabase formats were verified with v0.64.2.8; Redash with 26.9. The local Superset
6.1.0 deployment has no verified remark/mutator contract.

| Tool                | Leading remark and normalized context                                                                                                                                                                         |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Metabase PostgreSQL | `-- Metabase:: userID: <id> queryType: <type> queryHash: <hash>`; `metabase_pg` extracts user, query type and hash.                                                                                           |
| Metabase Redshift   | `-- /* partner: "metabase", { ... } */ Metabase:: ...`; `metabase_rs` also extracts dashboard, chart, account and filter fields.                                                                              |
| Redash              | `/* user_id: <id>, query_id: <id>, Job ID: <uuid>, Query Hash: <hash>, Scheduled: <bool> */`; `redash` extracts the corresponding fields. Unsaved `query_id: adhoc` stays a string; saved IDs remain numeric. |
| Superset            | No registered extractor. Generated wrappers, grouping and limits do not identify a user. Deployment-specific remarks require a custom context provider.                                                       |

Malformed or nonleading comments produce no metadata. Client application names and
remark fields do not prove viewer identity, dashboard/download/embed origin, or
source permissions. Application test literals such as `bi_tool_name` are not native
BI metadata.

DEBUG `query received` events include normalized `bi_remark` or `null` and omit the
recognized leading remark from the `sql` field. This diagnostic extraction does not
select a route or supply application context; the configured provider does that.
