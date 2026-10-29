# YASP puts application decisions in the query path {#yasp-overview}

YASP gives analytics clients one connection configuration while the consuming application controls query context, source-role selection, and query/result behavior.

- Reuse the library inside your application and bind your own callbacks.[](source:assembly)
- Keep protocol handling, execution, validation, and resource limits inside the engine.[](source:ownership)
- Change SQL before routing and process rows before client output.[](source:query-order)

@answer scope="Current implementation · examples use invented names"
@meta locale=en repo_url="git@github.com:AbhiSinha08/yasp-analytics-proxy.git" repo_revision=526debf6d497e5d1e1f18d0397a1279855683f0f
@source assembly title="Application assembly" path=src/host/mod.rs lines=30:74
@source ownership title="Library and application responsibilities" path=docs/PLAN.md lines=24:40
@source query-order title="Context, parsing and effective SQL" path=src/gateway/query.rs lines=119:181
@source route-stage title="Lineage and route validation" path=src/execution.rs lines=112:150
@source processing title="Current result callback invocation" path=src/execution/processing.rs lines=173:240
@source plan title="Delivered capabilities and future scope" path=docs/PLAN.md lines=52:54
@source contracts title="Implemented callback contracts" path=docs/extensions.md lines=27:48

## What sits between the client and its source? {#system}

@diagram architecture id=system-picture source=system-map.json title="Application decisions inside a shared query engine"

The gateway is the client-facing connection. A backend adapter connects to the selected source and owns its credentials and native behavior.[](source:ownership)

@steps
| enter | Analytics client | Use one connection configuration. Per-query source selection does not require a client connection for every source role. | system-picture | client |
| gateway-step | Client gateway | Keep authenticated integration identity and frontend session state inside the engine. | system-picture | gateway |
| engine-step | Shared query engine | Run ordered query stages and validate application decisions against configuration. | system-picture | core |
| app-step | Application callbacks | Supply context meanings, role mappings, SQL rules and row behavior from application code. | system-picture | callbacks |
| source-step | Selected source | Execute with the configured source role. Its grants enforce source access. | system-picture | source |

## How does a consuming application assemble it? {#assembly-panel}

@diagram workflow id=assembly-picture source=library-assembly.json title="Bind the application before opening the listener"

| Application supplies | Engine supplies |
| --- | --- |
| Configuration, secrets and configured source identities | Validated routes and adapter-owned credentials |
| Rust callbacks with bound state, or trusted Python resources | Callback invocation, deadlines and bounded payloads |
| Dataset loaders and organization mappings | Shared dataset publication and scoped lookup helpers |
| Query transformation and result rules | SQL revalidation, batch validation and client streaming |

These responsibilities use the existing library and callback contracts.[](source:contracts)

```rust title="Library assembly sketch: attach application callbacks"
// Bind application state/functions with the public binders first.
let gateway = gateway
    .with_query_context_hook(context)
    .with_query_modification_hook(modifier)
    .with_result_hook(result_hook, result_settings)?
    .with_catalog(catalog);
```

This sketch shows existing attachment points; the host also constructs adapters, a selector, listener, and shutdown future. Rust extensions compile with that host.[](source:assembly)[](source:ownership)

## What happens during one ordinary query? {#journey}

@diagram sequence id=journey-picture source=query-journey.json title="From original SQL to validated client rows"

| Moment | Concrete example |
| --- | --- |
| Client submits SQL | `SELECT o.id, o.total FROM logical.orders AS o` |
| App derives context | `user_id = u17`, `department = finance`, `workload = interactive` |
| App replaces SQL | Read `reporting.orders_v2 AS o` while preserving the selected columns |
| Engine prepares execution | Parse replacement SQL, check read-only rules, derive its ID, and analyze optional lineage |
| App chooses a route | `target = reporting`, `backend_login = finance_reader` |
| Source returns raw rows | Source values keep their native representation |
| App processes output | Replace text, set selected cells to SQL NULL, or filter rows |

The original context stays available downstream. Selection uses the effective query, and result hooks receive the validated route and raw data contexts.[](source:query-order)[](source:route-stage)[](source:processing)

## Which capabilities are available now? {#scope}

| Available in the current runtime | Still planned |
| --- | --- |
| Bounded read-only simple-query execution | Prepared statements and broader protocol/session coverage |
| Context, SQL modification and source-role selection | Lifecycle hooks and engine interpretation of organization RBAC |
| Shared datasets and schema catalog | Automatic cross-dialect translation |
| Optional lineage, result classification and result callbacks | Mandatory engine-enforced dynamic masking |
| Opt-in raw result caching | Broader management and orchestration capabilities |

Future items are outside the delivered runtime shown in the diagrams. Client protocols, backend adapters, and SQL translation have separate compatibility boundaries.[](source:plan)[](source:ownership)

@callout level=note title="Explore the diagrams"
| Click a node for its responsibilities and source evidence. Use guided views, focus, search, theme controls, Live/Still motion, and export in the native reader. |
| The companion pages cover query customization and runtime behavior. Each embedded diagram can open in its full interactive reader. |
