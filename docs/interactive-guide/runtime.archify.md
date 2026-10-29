# Shared facts and bounded resources support each query {#runtime-guide}

YASP shares schema facts and application datasets, caches raw results when enabled, and keeps source capacity and transaction affinity under engine control.

- Catalog generations describe source structure; application datasets describe application mappings.[](source:catalog)[](source:datasets)
- Raw cache replay still applies current application routing and result processing.[](source:route)[](source:replay)
- One occupied-capacity budget covers configured adapters and metadata work.[](source:budget)

@answer scope="Current implementation · example values are invented"
@meta locale=en repo_url="git@github.com:AbhiSinha08/yasp-analytics-proxy.git" repo_revision=526debf6d497e5d1e1f18d0397a1279855683f0f
@source catalog title="Schema startup and snapshot freshness" path=src/catalog.rs lines=198:248
@source refresh title="Atomic catalog generation publication" path=src/catalog.rs lines=282:329
@source lineage title="Lineage and actual output alignment" path=src/lineage.rs lines=110:175
@source datasets title="Dataset startup and scheduled refresh" path=src/datasets.rs lines=148:207
@source dataset-reader title="Scoped bounded dataset reads" path=docs/extensions.md lines=61:66
@source dataset-refresh title="Bounded dataset publication" path=src/datasets.rs lines=231:274
@source metadata title="Metadata responses use a snapshot" path=src/gateway/query.rs lines=70:107
@source route title="Current selection before cache dispatch" path=src/execution.rs lines=112:150
@source replay title="Replay through current processing" path=src/cache/execution.rs lines=85:130
@source publication title="Publish only complete clean raw results" path=src/cache/execution.rs lines=190:225
@source cache-scope title="Raw cache scope" path=src/cache/execution.rs lines=229:249
@source cache-policy title="Cache, discovery and transaction boundaries" path=docs/source-access-routing.md lines=51:63
@source budget title="Shared admission and acquisition deadline" path=src/backend/budget.rs lines=156:191
@source release title="Release resource capacity" path=src/backend/budget.rs lines=218:226
@source tx title="Retained route affinity" path=src/execution/transaction.rs lines=133:168
@source tx-failure title="Failed transaction handling" path=src/execution/transaction.rs lines=52:61
@source allocation title="Configured compute and resource ownership" path=docs/source-access-routing.md lines=34:49
@source controls title="Transaction controls and cleanup" path=src/execution/transaction.rs lines=116:130

## How does schema support a query? {#schema-panel}

@diagram workflow id=schema-picture source=schema-lineage.json title="A shared catalog supports discovery and lineage"

```text title="Example catalog facts"
source: configured discovery identity
schema: reporting
relation: orders_v2
columns:
  id       integer-like native type, key if reported
  total    exact numeric native type
  status   scalar text native type
```

```sql title="Example effective query"
SELECT o.total AS revenue
FROM reporting.orders_v2 AS o;
```

| Output fact | Example |
| --- | --- |
| Output ordinal | `0` |
| Output name | `revenue` |
| Physical source | Configured database + `reporting.orders_v2.total` |
| Meaning | Raw source lineage, before any result edits |
| Missing facts | Explicit unknown references or unknown output shape |

Lineage follows the effective syntax tree after SQL modification. Selection receives static AST facts before execution. Result hooks receive facts aligned with actual output columns by ordinal; uncertain shapes stay explicit.[](source:route)[](source:lineage)

Recognized discovery requests read a fresh catalog generation without acquiring a source session or invoking ordinary query hooks. Expired metadata fails rather than triggering live discovery.[](source:metadata)[](source:catalog)

@callout level=note title="One configured discovery view"
| The shared catalog reflects its configured source identity and schema scope. It is not a per-viewer authorization decision or an automatic union of all routed sources. |
| Failed refresh retains the previous complete generation without extending its age. Missing lineage facts remain uncertainty, not permission. |

## How do application mappings stay shared? {#datasets-panel}

@diagram workflow id=datasets-picture source=application-datasets.json title="Publish complete app datasets and read them inside callbacks"

```json title="Example application dataset: users"
{
  "u17": {"department": "finance"},
  "u29": {"department": "operations"}
}
```

```text title="Example callback lookup"
datasets.get("users", "u17")
=> {"department": "finance"}
```

Configured loaders run before serving and can refresh on a schedule. Startup failures prevent service; later refresh failures preserve the last good data.[](source:datasets)

Dataset reads are bounded and scoped to the current callback. Each lookup reads the current published value; the registry is separate from the result cache.[](source:dataset-reader)

## What happens when the raw result is cached? {#cache-panel}

@diagram sequence id=cache-picture source=raw-cache.json title="Re-evaluate decisions while reusing raw source results"

| Request | Raw cached value | Current app result rule | Client-visible value |
| --- | --- | --- | --- |
| First caller | `customer_name = Ada` | Uppercase text | `ADA` |
| Later caller, same raw scope | `customer_name = Ada` | Keep text | `Ada` |
| Different effective SQL or source scope | Separate cache identity | Evaluate current callback | Depends on that query |

These output rules illustrate configured callbacks. No caller-specific transformation is stored in the raw cache.[](source:replay)[](source:publication)

| Re-evaluated on a hit | Reused until expiry |
| --- | --- |
| Query context and optional SQL modification | Eligible raw source rows |
| Current source-role decision and configured bounds | Associated compatible classification metadata when enabled |
| Current result processing and output validation | Source result captured after verified completion |

Raw scope includes effective query identity, the selected source/login, frontend identity/database, input dialect, and relevant session/settings. Application values and hook versions are not automatically cache-key fields.[](source:cache-scope)[](source:cache-policy)

@callout level=warning title="The cache changes the access freshness boundary"
| A hit does not execute source grants or source row policies again until expiry. Route validation and current result hooks still run. |
| Put source-side tenant filtering in effective SQL or the validated route/scope. SQL comment tags alone do not isolate raw cached data. |
| Cache errors and ineligible requests use normal source execution. Explicit transactions bypass the result cache. |

## Who owns capacity and session continuity? {#sessions-panel}

@diagram workflow id=sessions-picture source=capacity-sessions.json title="Keep configured bounds through execution and cleanup"

| Trigger | Runtime behavior |
| --- | --- |
| Selected class has several instances | Try eligible instances within that configured class |
| No capacity is available | Wait under the acquisition deadline or fail |
| Query finishes but its connection stays pooled | Physical resource capacity can remain occupied |
| Cleanup or disposal is still running | Keep capacity until resource ownership ends |
| Valid raw cache hit or metadata snapshot response | Acquire no source execution capacity |
| Catalog refresh | Use normal source admission |

Overflow never selects a more privileged login, another target, or another compute class. Submitted SQL and source permission failures are not replayed on another instance.[](source:allocation)

```sql title="Example transaction block"
BEGIN;
SELECT o.id FROM reporting.orders_v2 AS o;
-- The first source query retains its physical route.

SELECT o.total FROM reporting.orders_v2 AS o;
-- Selection runs again and must be compatible with that route.
ROLLBACK;
```

Explicit blocks bypass result caching. A conflicting route is rejected; a failed transaction requires rollback before more data queries. Adapter cleanup ends the retained source state.[](source:tx)[](source:tx-failure)[](source:controls)
