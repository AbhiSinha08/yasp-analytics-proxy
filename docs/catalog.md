# Shared schema catalog

The library stores source schema definitions in memory and refreshes them
periodically. PostgreSQL and optional Snowflake providers use their real backend
adapters, configured source identities, admission limits and cleanup. They read
only metadata. There is no additional database service or disk snapshot.

Hosts construct `catalog::Catalog::postgres` or, with the `snowflake` feature,
`catalog::Catalog::snowflake`, then attach the returned `Arc<Catalog>` using
`GatewayConfig::with_catalog`. One handle can be shared by frontend listeners and
later AST consumers. The repository YAML host can attach a PostgreSQL catalog from
its configured discovery binding.
Existing PostgreSQL forwarding remains the default without a catalog.
Current gateway entry points still require a PostgreSQL discovery binding for
startup and legacy services, including when a Snowflake catalog is attached.

```rust,no_run
use std::sync::Arc;
use yasp::{backend::PostgresBackend, catalog::{Catalog, CatalogSettings}, config::GatewayConfig};

async fn attach(config: GatewayConfig, source: Arc<PostgresBackend>) -> Result<(GatewayConfig, Arc<Catalog>), yasp::catalog::CatalogError> {
    let settings = CatalogSettings::new(vec!["public".into()]);
    let catalog = Catalog::postgres(source, settings).await?;
    Ok((config.with_catalog(catalog.clone()), catalog))
}
```

## Scope and stored facts

Each catalog belongs to one source identity and an explicit list of schemas with
their exact source spelling. PostgreSQL records endpoint/database and principal;
Snowflake records account or emulator endpoint, principal, role and warehouse.
Handles are not shared across these identities implicitly. A PostgreSQL catalog
must match the listener's configured discovery identity before startup succeeds.
Source grants remain authoritative; PostgreSQL column grants filter the imported
columns, and Snowflake uses its role-visible information-schema views.

A snapshot records database/schema names, relations, relation kind, comments,
columns, ordinals, nullability, defaults and native type details including available
length/precision/scale. PostgreSQL also reports native OIDs and type modifiers.
Snowflake datetime precision and generated-column status are unknown in this
provider; its documented [`DATETIME_PRECISION`](https://docs.snowflake.com/en/sql-reference/info-schema/columns)
field is not applicable. PostgreSQL reports declared primary-key membership per
column and the discovery identity's whole-relation SELECT privilege; Snowflake
leaves these properties unknown. Full constraints, view definitions, physical indexes, statistics and general privilege listings
are not part of the normalized model yet. No missing property is synthesized as
an enforcement guarantee.

`RelationId` is the exact `(database, schema, relation)` coordinate. `ColumnId`
contains that coordinate and the exact column name. Refreshes preserve coordinates
for unchanged names; these are not durable IDs across drops/recreates or renames.
Retain source identity and snapshot generation alongside any bound column reference.

## Refresh and failure behavior

### YAML host

```yaml
schema_discovery:
  target: primary
  backend_login: reader
  catalog:
    schemas: [public]
    refresh_interval_seconds: 60
    refresh_timeout_ms: 10000
    max_age_seconds: 300
    max_rows: 100000
    max_bytes: 16777216
```

Omitting `catalog` disables snapshots. When enabled, `schemas` is required and the
remaining fields use the defaults shown. The source is the existing discovery
binding, including `schema_discovery.compute` when configured. Its pool and
admission budgets are shared with live discovery. Invalid settings or an initial
load failure prevent startup; listener shutdown joins the refresh task.
This host currently configures PostgreSQL providers only. The typed library API
also accepts the optional Snowflake provider.

| Setting | Default | Meaning |
| --- | --- | --- |
| `schemas` | Required | Exact schema names, at most 256; not an access policy. |
| `refresh_interval` | 60 seconds | One periodic worker per catalog, with missed ticks skipped. |
| `refresh_timeout` | 10 seconds | Includes source acquisition, fetch, conversion and publication. |
| `max_age` | 300 seconds | Reads fail after the last successful generation exceeds this age. |
| `max_rows` | 100,000 | Maximum metadata rows in the candidate load. |
| `max_bytes` | 16 MiB | Bounds intermediate row payloads and snapshot-owned strings separately. |

Initial loading must succeed. Providers issue one bounded statement for the chosen
schemas; the extra limit row detects truncation. Only a complete, validated result
becomes the next immutable generation. This is one source observation, not a
guarantee of DDL consistency stronger than the source engine provides.

`Catalog::snapshot()` returns an `Arc<Snapshot>`. A metadata response pins that Arc
through completion. Existing readers retain their generation during a refresh.
`Catalog::status()` exposes generation, age, last attempt, refresh-in-progress state
and a typed error. A failed, timed-out or interrupted refresh retains the previous
generation without resetting its age. A valid empty schema differs from a failed
load. `Catalog::refresh()` permits a host-triggered refresh; overlapping refreshes
are rejected. `Catalog::stop_refreshing().await` stops and joins the periodic worker;
manual refreshes remain available. Hosts should await this before releasing catalog
and backend resources. Dropping the final catalog handle also aborts the task, with
asynchronous source cleanup. Generations are local to one catalog lifetime and are
not durable identifiers across process restarts.

The byte limit excludes allocator/collection overhead and connector allocations.
Old pinned generations and a candidate can coexist. Host code must bound the
lifetime and number of retained generations. Snowflake's native-memory production
gate remains open; dropping a timed-out refresh retains native capacity until its
underlying worker has actually stopped.

## Frontend responses

Both profiles recognize the captured PostgreSQL 42.7.12 and Redshift 2.2.5 JDBC
schema, table and column SQL shapes. Matching covers the entire token stream;
only designated literal schema/table/column predicates may vary. Metabase
v0.64.2.8's custom Redshift table and field scans are also recognized, including
optional literal schema/table IN lists of at most 256 entries. They return its
expected aliases, booleans, zero-based column positions, comments, types and
PostgreSQL declared primary-key membership. These custom scans currently require
a PostgreSQL catalog, whose provider filters for SELECT privileges. The scans are recorded in
[`metabase_redshift.json`](../tests/fixtures/metabase_redshift.json). The table
handler excludes the driver's internal schemas. A provider without primary-key
facts rejects Metabase field scans explicitly rather than reporting missing keys
as false. Source case and SQL LIKE escapes are preserved. Metabase field scans
retain PostgreSQL's native type names. JDBC tables and columns use frontend-specific result
columns and supported scalar type mappings. Unknown types fail explicitly. Snowflake
bounded text maps to frontend `varchar` with its declared character length; unknown
octet length remains NULL rather than being inferred from character count.

Matching occurs after SQL byte bounds and the host's typed input-options resolver,
before the general SQL parser. This permits the pinned Redshift driver's metadata
SQL, including syntax its source PostgreSQL cannot execute. Fixed templates contain
no application-row reads. Native input options skip these handlers; SQL comments
cannot elevate a query. Nested/altered queries and additional statements do not
match. Responses bypass context hooks, routing callbacks and result caching.

PostgreSQL catalogs retain the existing discovery fallback for other classified
metadata, using the same source identity. With a Snowflake catalog, unknown
classified metadata fails with `0A000` rather than borrowing PostgreSQL discovery
access. Full key definitions and other driver metadata therefore still need normalized handlers
before complete Snowflake schema-sync acceptance. Local snapshot responses acquire
no backend transaction; application data continues to use existing transaction
affinity. Failed frontend transactions reject catalog responses until rollback.

Redash 26.9's captured Redshift `svv_columns`/`SVV_EXTERNAL_SCHEMAS` schema query
also has an exact handler. It returns `table_name`, `table_schema`, `column_name`
and source-native `data_type` names, ordered by table and column ordinal. It uses
the PostgreSQL discovery identity's schema USAGE and whole-table SELECT facts;
column-only grants do not satisfy its table privilege predicate. Its internal and
temporary schema exclusions are retained. It currently requires a PostgreSQL
provider; no external-schema privileges are inferred for Snowflake. Additional
statements and queries modified to read application tables never match this handler.

## AST lineage and masking

The catalog supplies schema facts for a future AST binder: wildcard expansion,
qualified-column lookup and native types. A binder must pin a generation for its
whole analysis and resolve aliases, joins, CTEs, subqueries, expressions, ambiguity
and view boundaries. Coordinates alone do not establish the base-table origin of
a view or computed result column. Unknown or ambiguous lineage must remain explicit.
Dynamic masking, policy interpretation, result rewriting and their cache semantics
are separate planned work; no masking is implemented by this catalog.
