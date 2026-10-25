# Application callbacks

The consuming host registers organization-specific context and policy; the engine
invokes narrow contracts and validates decisions. Public binders live under
`yasp::hooks`. Reusable repository implementations belong in [builtin/](builtin),
application Rust code in its consuming host/crate, and Python examples in [examples/](examples).
See [extension contracts](../docs/extensions.md) for stage data, bounds, and failure rules.

## Backend selection

```rust
use yasp::hooks::{bind_select_backend, BackendSelection, SelectionRequest};

let pair = BackendSelection {
    target: "primary".into(),
    backend_login: "reader".into(),
};
let selector = bind_select_backend(
    pair,
    |_request: &SelectionRequest<'_>, pair| Ok(pair.clone()),
);
```

The binder owns host state; state/function must be `Send + Sync + 'static`.
Selection receives borrowed identity/query metadata and derived context, never
credentials. It returns a concrete pair convertible into `BackendDecision`, a partial decision,
or `SelectionError`; the router resolves configured defaults and delegates compute
allocation to the backend. Denial/unconfigured pairs
report `42501`, failure reports `XX000`. Keep synchronous Rust callbacks fast and
nonblocking.

## Hook references

```yaml
hooks:
  select_backend:
    type: rust
    callback: builtin.select_backend.passthrough
    params:
      target: primary
      backend_login: reader
```

Both languages use `type`, `callback`, and application-owned `params`; Python also
requires `path`. The built-in passthrough accepts optional `target`, `backend_login`, and
`compute_type` keys. Empty params infer only unambiguous choices. Compiled string names are shorthand.
The repository host registers its documented built-ins, compiled examples, and Python references;
custom Rust names/typed params are mapped by the consuming application at startup.
See [runtime configuration](../docs/configuration.md#routing-and-pools).

## Query context

`bind_query_context(state, f)` returns bounded `QueryContextOutput { dialect, values }`;
attach it with `GatewayConfig::with_query_context_hook`. It runs after gateway
metadata/setup recognition and before ordinary SQL parsing. Errors reject the query.
The request has raw SQL, frontend identity, client ID, gateway/common target dialects,
and optional datasets; AST and lineage belong to downstream hooks.

`hooks.query_context` runs once after frontend metadata/setup recognition and before
ordinary SQL parsing, including on cache hits. Metadata recognition always uses the
gateway profile and is independent of remarks or hook output. A recognized request
does not invoke query context, selection, eligibility, or result processing.

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

[examples/context.py](examples/context.py) supplies `derive_query_context`, a dataset-aware
application example. Python `yasp_extractors.metabase_pg`, `metabase_rs`, and `redash`
remain reusable raw remark helpers, independent of the provider envelope. Redash
preserves numeric saved-query IDs and the string `adhoc` for unsaved queries.
Superset/custom extensions own their remark format and return the documented envelope.

[examples/rbac.py](examples/rbac.py) resolves the extracted user ID against `users`
to obtain the account for routing and AST/model email masks. Account policy stays
in the application callback. The local datasets map user `1` to the admin account
and user `2` to the limited account. Metabase Admin status alone does not imply a
source database role.
Existing `yasp-test` comments retain their explicit workload/report labels.
The Redshift remark has been observed through the gateway with Metabase v0.64.2.8.
These comments are untrusted hints, never
authenticated identity. See the
[BI metadata notes](../docs/extensions.md#observed-sql-context-from-local-bi-traffic)
for supported versions and observed formats.

## Query modification

`bind_modify_query(state, f)` binds an async Rust callback returning optional
replacement SQL. Attach it with `GatewayConfig::with_query_modification_hook`.
Python uses `modify_query(request, params)` returning a SQL string or `None`,
configured under `hooks.modify_query` using the existing reference format.

The request supplies original SQL and its parsed AST, resolved dialect, query
context, identity, and scoped datasets. It contains no lineage or query ID.
YASP reparses replacement SQL into a new AST before downstream lineage, routing,
caching, or execution. Errors reject without executing the original query.

[examples/query_modifier.rs](examples/query_modifier.rs) demonstrates a compiled
table rename for qualified names with explicit aliases. The repository host
registers it as `example.modify_query.rename_table` with required `from` and `to`
qualified-name params; [example configuration](../config/example.yml) shows the
reference. See [query modification](../docs/query-modification.md)
for configuration, filtered subqueries, and additional application use cases.

## Shared datasets

The host can load application JSON datasets at startup and pass a bounded reader to
context, modification, selection, and eligibility callbacks. A Python callback uses ordinary values:

```python
user = request["datasets"].get("users", user_id)
users = request["datasets"].get_many("users", [user_id, "bob"])
locations = request["datasets"].read("email_locations")
```

`get_many` omits missing fields. `read` returns the complete mapping. These helpers
read the latest shared Redis value on each call; they do not return JSON text or use an
expiring process-local cache. Dataset and lookup size, depth, and field-count limits
are enforced by the core. Python helpers wait synchronously on the async registry;
dataset loaders use a dedicated worker, while loader and callback code shares one
interpreter and the GIL.

The supplied host's `builtin.dataset.load_json` loader reads a JSON object from
`params.file`, resolved relative to the process working directory and bounded by
`application_contexts.max_dataset_bytes`. Values can be nested objects, arrays,
scalars, or `null`. [examples/datasets.rs](examples/datasets.rs) shows an
application-owned compiled loader that returns email locations in code;
`example.datasets.email_locations` remains registered by the supplied host.
[examples/dataset_selector.rs](examples/dataset_selector.rs) shows an async Rust
selector bound to application-owned department policy. Configure
`application_contexts.datasets` with the chosen loader references and keep its Redis
namespace separate from the result cache. The reference configuration uses the
built-in file loader for its sample JSON resources. A zero refresh interval means
startup-only loading; scheduled refreshes require a positive per-dataset interval.

`examples/context.py` demonstrates `get` and `read`, and
`examples/role_selector.py` demonstrates `get_many` plus reading the full email
location dataset. Its SQL-comment extractor is an application test fixture only:
comments are client-controlled and must never be treated as authenticated identity.
The selector example returns the `secondary` target and `restricted` login when its
sample mappings do not match; applications should replace that fallback with their
own policy. The examples pass field locations as context and do not mask data.

## External Python callbacks

Build with `--features python`, create one `PythonRuntime`, and register external
file/function references before serving. Clone the runtime for more callbacks.
Each trusted synchronous function receives `function(request, params)` with fresh
copied data. Query hooks share one bounded dedicated worker and interpreter state;
configured dataset loaders use a separate bounded worker in the same interpreter.
A timeout stops waiting while executing code retains capacity. Dataset helpers
expose only read operations and expire when their callback finishes or is cancelled.

Use [examples/select_backend.py](examples/select_backend.py) for runnable passthrough
selection. Replace the reference above with `type: python`,
`path: ../hooks/examples/select_backend.py`, and `callback: select_backend` while
keeping pair params. Paths resolve relative to YAML. The callback returns a
mapping with optional `target`, `backend_login`, and `compute_type` fields, or
`None` to deny. The engine chooses an instance by overflow within the selected type.
[examples/role_selector.py](examples/role_selector.py) demonstrates application-owned
user/workload/report maps and a hardcoded `primary/analyst` fallback for unmatched
sample values.
Its input identity still needs the consuming application's verified metadata contract. [examples/passthrough.py](examples/passthrough.py) also
contains future callback sketches; they are not wired into query/result stages.

See [Python setup](../docs/development.md#python-embedding) and
[the runtime contract](../docs/extensions.md#external-python) for supported values,
imports, registration, queue/payload limits, and trusted-code constraints.

## Cache eligibility

```yaml
cache:
  eligibility:
    type: rust
    callback: builtin.cache_eligibility.exclude_sql_containing
    params:
      excluded_sql_fragments: [pg_stat_activity, clock_timestamp]
```

This callback bypasses cache reads and writes when effective SQL contains a configured
case-insensitive literal fragment, including comments/string literals. It cannot
expand supported SQL or configured limits; errors bypass caching. For Python, use
[examples/cache_eligibility.py](examples/cache_eligibility.py) and
`callback: cache_eligibility` with the same params. The runtime is shared with other
callbacks. Context and selection still run before an eligible cache hit.

## Result processing

`hooks.process_result` registers one optional Rust or Python callback. Chunks contain
all columns; callbacks can edit text, set SQL NULL, and filter rows while keeping
schema and order. `builtin.process_result.uppercase` accepts `params: {column: name}`.
The library exposes `bind_process_result` and `GatewayConfig::with_result_hook`.
Python uses `function(request, batch, params)` with a native lazy batch.
Raw cache hits are processed again with current application context and existing
AST/model outcomes.
See [result processing](../docs/result-processing.md) for API, bounds, and examples.

## Planned hooks and masking

Lifecycle hooks and engine-enforced masking are future
stages. Their commented example settings are not accepted runtime configuration.
See [planned stage ordering](../docs/extensions.md#planned-query-and-result-stages)
and the [phase plan](../docs/PLAN.md) before implementing another contract.
