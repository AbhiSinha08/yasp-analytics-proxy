# Application hooks shape the query and its results {#hooks-guide}

Applications can derive context, choose a configured source role, replace SQL, and edit result batches through existing extension contracts.

- Context fields remain separate from authenticated integration identity.[](source:context)
- Replacement SQL is parsed and checked before routing or cache lookup.[](source:replace)
- Result processing runs on source results and raw cache replays.[](source:replay)

@answer scope="Current contracts · illustrative values and snippets"
@meta locale=en repo_url="git@github.com:AbhiSinha08/yasp-analytics-proxy.git" repo_revision=526debf6d497e5d1e1f18d0397a1279855683f0f
@source context title="Context inputs and outputs" path=src/hooks/query_context.rs lines=14:31
@source derive title="Derive context with limits" path=src/gateway/query.rs lines=293:322
@source selector title="Dataset-backed application selector" path=hooks/examples/dataset_selector.rs lines=18:49
@source resolve title="Resolve every configured route" path=src/backend/execution.rs lines=94:119
@source replace title="Modifier invocation and replacement parsing" path=src/gateway/query.rs lines=242:290
@source rename title="Application table-rename callback" path=hooks/examples/query_modifier.rs lines=12:59
@source order title="Effective query validation" path=src/gateway/query.rs lines=173:182
@source batch-contract title="Result batch operations" path=docs/result-processing.md lines=74:103
@source process title="Result callback and batch validation" path=src/execution/processing.rs lines=173:240
@source replay title="Current processing on raw replay" path=src/cache/execution.rs lines=85:130
@source result-failure title="Result failures and cache behavior" path=docs/result-processing.md lines=129:140
@source trust title="Application policy and configured bounds" path=docs/source-access-routing.md lines=9:32
@source modifier-limits title="Application correctness and isolation" path=docs/query-modification.md lines=276:290

## How does context select a backend target? {#context-panel}

@diagram workflow id=context-picture source=context-routing.json title="Derive app values, then resolve a configured route"

| Value | Meaning | Owner |
| --- | --- | --- |
| `authenticated_user = analytics_service` | Authenticated connection identity | Engine |
| `query_context.user_id = u17` | App-level person or viewer reference | Application |
| `query_context.department = finance` | Business mapping used by policy | Application |
| `query_context.workload = interactive` | Workload hint interpreted by callbacks | Application |
| `target = reporting` | Configured logical source | Selector, then engine validation |
| `backend_login = finance_reader` | Configured source-role credential profile | Selector, then adapter |
| `compute_type = interactive` | Optional configured compute class | Selector, then engine allocation |

The engine treats application values as opaque. Returning an input dialect changes parsing; it does not select a target or translate SQL.[](source:context)[](source:trust)

```json title="Example context values"
{
  "values": {
    "user_id": "u17",
    "department": "finance",
    "workload": "interactive"
  }
}
```

```text title="Example policy lookup"
users["u17"].department        = "finance"
department_logins["finance"]   = "finance_reader"

decision.target               = "reporting"
decision.backend_login        = "finance_reader"
decision.compute_type         = "interactive"  // only if configured
```

The repository includes a selector that looks up the user dataset and maps department to login. Compute-class selection is an additional supported decision field.[](source:selector)[](source:trust)

@callout level=warning title="Context is not authentication"
| The application must establish trust before using viewer metadata for access decisions. A user ID in a SQL remark is only a client-provided hint. |
| Unknown, denied, or ambiguous route decisions fail. The callback cannot supply arbitrary source credentials or expand configured choices. |

## How can a hook change the query? {#rewrite-panel}

@diagram workflow id=rewrite-picture source=sql-replacement.json title="Replace SQL while keeping downstream stages consistent"

```sql title="Original query"
SELECT o.id, o.total
FROM logical.orders AS o
WHERE o.status = 'paid';
```

```sql title="Effective query after a configured table rename"
SELECT o.id, o.total
FROM reporting.orders_v2 AS o
WHERE o.status = 'paid';
```

The existing Rust table-rename example edits a cloned syntax tree. It requires a qualified input table and an explicit alias on matching references.[](source:rename)

| Modifier result | Engine behavior |
| --- | --- |
| No replacement | Keep original SQL and its immutable syntax tree |
| Replacement string | Bound and parse a new immutable query in the resolved input dialect |
| Error, timeout, invalid SQL or unsupported statement | Reject; do not execute original SQL as fallback |

The engine validates the effective query before generating its ID. Lineage, selection, cache eligibility, and result processing all see that effective query.[](source:replace)[](source:order)

@callout level=note title="More transformations use the same contract"
| An application can construct tenant-specific filters, choose a tenant schema, use a compatible summary table, or cap returned rows. It owns semantic correctness and rejects unsupported shapes when a rule is mandatory. |
| Context is derived once. A replacement does not re-run context extraction. Policy-relevant source filters must appear in effective SQL or the validated route/scope; comment-only tags do not isolate raw cached data. |

## How can a hook change the result? {#result-panel}

@diagram workflow id=result-picture source=result-processing.json title="Edit bounded row batches before client output"

```text title="Raw source batch"
id  customer_name  email               status
11  Ada            ada@example.test    visible
12  Lin            lin@example.test    hidden
```

```python title="Application-owned batch callback sketch"
def process_result(request, batch, params):
    name = batch.column_index("customer_name")
    email = batch.column_index("email")
    status = batch.column_index("status")

    names = batch.text_column(name)
    batch.replace_text_column(
        name, [value.upper() if value is not None else None for value in names]
    )
    for row in range(len(batch)):
        batch.set_null(row, email)
    batch.retain_rows([value == "visible" for value in batch.text_column(status)])
```

```text title="Client-visible batch"
id  customer_name  email  status
11  ADA            NULL   visible

Completion count: 1 emitted row
```

The sketch uses existing native-batch operations and assumes unique text column names. The application supplies this callback; it is not a default engine policy.[](source:batch-contract)[](source:process)

| Supported edit | Constraint |
| --- | --- |
| Replace scalar text | Adapter-recognized text type; validate declared lengths and byte limits |
| Set SQL NULL | Any column; SQL NULL stays distinct from empty text |
| Filter rows | Retained rows keep their order |
| Inspect output data contexts | Lineage and optional model classifications describe raw results |
| Process successive batches | Schema stays fixed; no inserted rows or reordered retained rows |

Native values avoid conversion through floating point or generic JSON. An unchanged numeric or temporal column keeps its native representation.[](source:batch-contract)

@callout level=warning title="Failure and replay behavior"
| A failed batch is rejected without a raw fallback. Previously emitted chunks cannot be retracted. |
| The raw cache keeps original source rows. Current result processing runs again for every caller and may produce a different visible result. |
| Optional classifications are evidence for application behavior. Mandatory engine masking remains planned. |
