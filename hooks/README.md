# Application callbacks

Use this page to choose and register runnable examples. [Extensions](../docs/extensions.md)
owns callback contracts, stage order, bounds, and failure behavior. Reusable
implementations live in [builtin/](builtin); application Rust code belongs in its
consuming host/crate, and external Python resources live in [examples/](examples).

## Backend selection

```rust
use yasp::hooks::{bind_select_backend, BackendSelection, SelectionRequest};

let selector = bind_select_backend(
    BackendSelection { target: "primary".into(), backend_login: "reader".into() },
    |_request: &SelectionRequest<'_>, pair| Ok(pair.clone()),
);
```

The binder owns shared application state. Use an async binder when policy needs
asynchronous dataset reads. [examples/dataset_selector.rs](examples/dataset_selector.rs)
shows a compiled department-policy selector;
[examples/role_selector.py](examples/role_selector.py) maps user/workload/report
context to a route. Its sample fallback is `primary/analyst`; replace it with the
application's intended missing-context policy. No example comment proves identity.

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

Both languages use `type`, `callback`, and optional application-owned `params`.
Python also needs a YAML-relative `path` and the `python` feature. To run
[examples/select_backend.py](examples/select_backend.py), change the reference to
`type: python`, `path: ../hooks/examples/select_backend.py`, and
`callback: select_backend`, keeping the pair params.

The built-in passthrough accepts optional `target`, `backend_login` and
`compute_type`; empty params work when choices are unambiguous. Custom compiled
names/params are registered by the consuming host at startup. See
[routing settings](../docs/configuration.md#routing-and-pools).

## Query context

| Callback/resource                          | Use                                                                                                                            |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `builtin.query_context.metabase`           | Derive Metabase fields and incoming dialect. Optional `params.native_dialect` resolves native SQL when target dialects differ. |
| `builtin.query_context.redash`             | Derive saved/adhoc/scheduled Redash fields; optional `params.dialect` overrides valid remarks.                                 |
| [examples/context.py](examples/context.py) | `derive_query_context` combines remark metadata with application datasets and test workload/report labels.                     |
| [examples/rbac.py](examples/rbac.py)       | Derive account context for source routing and the AST/model email-processing examples.                                         |

```yaml
hooks:
  query_context:
    type: rust
    callback: builtin.query_context.metabase
```

The local `users` dataset maps user `1` to the admin account and user `2` to the
limited account. Metabase Admin status alone grants no source role. Python
`yasp_extractors.metabase_pg`, `metabase_rs` and `redash` are also available as raw
remark helpers. Superset/custom formats need an application provider. See
[query context](../docs/extensions.md#query-context) for dialect inference and
[BI metadata](../docs/extensions.md#observed-sql-context-from-local-bi-traffic) for
supported envelopes/versions and trust limitations.

## Query modification

[examples/query_modifier.rs](examples/query_modifier.rs) renames qualified physical
tables with explicit aliases. The supplied host registers
`example.modify_query.rename_table` with required `from`/`to` names; see
[configuration](../docs/configuration.md#query-modification). The
[query modification reference](../docs/query-modification.md) covers the Rust/Python
API, filtered subqueries, tenant schemas, and other transformations.

## Shared datasets

```python
user = request["datasets"].get("users", user_id)
users = request["datasets"].get_many("users", [user_id, "bob"])
locations = request["datasets"].read("email_locations")
```

Helpers return ordinary JSON-compatible values from current shared Redis data.
`get_many` omits missing fields. See [dataset ownership and limits](../docs/extensions.md#owning-shared-datasets)
and [dataset settings](../docs/configuration.md#shared-application-datasets).

| Callback/resource                                            | Use                                                                                                |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| `builtin.dataset.load_json`                                  | Load a bounded JSON object from `params.file`, relative to the working directory.                  |
| [examples/datasets.rs](examples/datasets.rs)                 | Application-owned compiled loader; the supplied host registers `example.datasets.email_locations`. |
| [examples/dataset_selector.rs](examples/dataset_selector.rs) | Async Rust dataset lookups with application department policy.                                     |
| [examples/context.py](examples/context.py)                   | Python `get` and complete `read`.                                                                  |
| [examples/role_selector.py](examples/role_selector.py)       | Python `get_many` and reading field locations for routing context.                                 |

The reference configuration loads the sample JSON files. Zero refresh intervals
mean startup-only loads; positive intervals schedule refresh. The test comment
format and fallback mappings are application examples, not an authentication policy.

## External Python callbacks

Build with `--features python` and register file/function references before serving.
Use one `PythonRuntime` and clone it for further registration. See
[embedding setup](../docs/development.md#python-embedding) for imports/packages and
[the runtime contract](../docs/extensions.md#external-python) for supported values,
workers, deadlines and retained resources.

[examples/passthrough.py](examples/passthrough.py) includes sketches for future
stages; those functions are not registered query/result hooks.

## Cache eligibility

```yaml
cache:
  eligibility:
    type: rust
    callback: builtin.cache_eligibility.exclude_sql_containing
    params:
      excluded_sql_fragments: [pg_stat_activity, clock_timestamp]
```

This bypasses reads/writes for case-insensitive SQL fragments, including matches in
comments or string literals. For Python use
[examples/cache_eligibility.py](examples/cache_eligibility.py) with
`callback: cache_eligibility` and the same params. See the
[cache contract](../docs/configuration.md#redis-result-cache).

## Result processing

`builtin.process_result.uppercase` uses `params: {column: customer_name}`.
[examples/rbac.py](examples/rbac.py) demonstrates application-owned email permissions
using AST/model contexts; configure the examples with
[local-ast.yml](../config/local-ast.yml) or [local-model.yml](../config/local-model.yml).
The model hook treats scores at or above `hooks.process_result.params.threshold`
as yes; the default is `0.5`.
These examples use one source login and apply presentation rules in the result hook.
See [result processing](../docs/result-processing.md) for native Rust/Python batches,
NULL replacement, filtering, raw-cache replay, and bounds.

## Planned hooks and masking

Lifecycle hooks and engine-enforced masking remain planned. Commented future
settings in the reference YAML are not accepted runtime fields. See
[planned stages](../docs/extensions.md#planned-query-and-result-stages) before
implementing another contract.
