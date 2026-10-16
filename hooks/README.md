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
credentials. It returns a configured pair or `SelectionError`; the router validates
it and delegates connection acquisition to the backend. Denial/unconfigured pairs
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
requires `path`. The built-in passthrough accepts exactly these pair keys or infers
a sole configured pair from empty params. Compiled string names are shorthand.
The repository host registers only its documented built-ins and Python references;
custom Rust names/typed params are mapped by the consuming application at startup.
See [runtime configuration](../docs/configuration.md#routing-and-pools).

## Context derivation

`bind_context(state, f)` binds an async provider returning an owned JSON mapping;
attach it with `GatewayConfig::with_context_hook`. Context runs once per ordinary
validated query before selection and eligibility, including hits. Discovery/setup
bypass application hooks. Failures use an empty mapping; cancellation stops the query.
Derived data cannot replace authenticated frontend identity.

[examples/context.py](examples/context.py) is the Python scaffold. Metabase,
Superset, and Redash extractors under `builtin::extractors` are placeholders returning
`NotImplemented`; Python `yasp_extractors` raises `NotImplementedError`. SQL comments
are untrusted hints, and live BI metadata/identity contracts remain integration work.

## External Python callbacks

Build with `--features python`, create one `PythonRuntime`, and register external
file/function references before serving. Clone the runtime for more callbacks.
Each trusted synchronous function receives `function(request, params)` with fresh
copied data. All hooks share one bounded dedicated worker and interpreter state;
a timeout stops waiting while executing code retains capacity.

Use [examples/select_backend.py](examples/select_backend.py) for runnable passthrough
selection. Replace the reference above with `type: python`,
`path: ../hooks/examples/select_backend.py`, and `callback: select_backend` while
keeping pair params. Paths resolve relative to YAML. The callback returns a
`target`/`backend_login` mapping or `None` to deny. No fallback route is selected.
[examples/role_selector.py](examples/role_selector.py) is an unimplemented
application-policy scaffold. [examples/passthrough.py](examples/passthrough.py) also
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

## Planned hooks and masking

Query rewrites, result processing, lifecycle hooks, and dynamic masking are future
stages. Their commented example settings are not accepted runtime configuration.
See [planned stage ordering](../docs/extensions.md#planned-query-and-result-stages)
and the [phase plan](../docs/PLAN.md) before implementing another contract.
