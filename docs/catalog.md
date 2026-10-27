# Shared schema catalog

YASP stores source definitions in bounded memory and refreshes immutable generations
periodically. Providers read metadata through the real backend adapters, using the
configured discovery identity, admission limits, deadlines and cleanup. Catalogs
contain no application rows and require no additional database service or disk snapshot.

Construct `Catalog::postgres`, `Catalog::redshift` or feature-gated `Catalog::snowflake`, then attach its
`Arc` with `GatewayConfig::with_catalog`. The YAML host constructs the provider
from `schema_discovery`; a Snowflake-only host needs no PostgreSQL discovery binding.
`Backends::catalog` exposes the same configured-source construction to library hosts.
One handle serves frontend listeners and optional AST lineage analysis.

## Scope and source facts

Each catalog has an explicit schema list preserving source spelling and one source
identity. PostgreSQL and Redshift identify their engine, endpoint/database and principal. Snowflake identifies
account or emulator endpoint/database, principal, role, warehouse and default schema.
Catalog identities are not implicitly shared across roles or warehouses. PostgreSQL
and Redshift catalogs must match their engine and discovery identity; a Snowflake discovery binding must match
its catalog before the listener starts.

The providers import schemas, relations, kinds, comments, columns, ordinals,
nullability, defaults, and native type details including available length/precision/
scale. PostgreSQL reports native OIDs/modifiers, declared primary-key membership,
and whole-relation SELECT privilege. Its source grants also filter schemas/columns.

The providers import view definitions and declared primary, unique and
foreign keys, with ordered local/referenced columns and referential actions. These
are declarations, not enforcement guarantees. Snowflake datetime precision and
generated-column status remain unknown; its information-schema datetime precision
is not applicable. Whole-table SELECT privilege is unknown for a real Snowflake
source: role-visible metadata alone is insufficient. The explicit local emulator
has unrestricted visibility and SELECT access; those facts do not establish real
Snowflake grants. Physical PostgreSQL indexes, owners, ACLs, distribution, encoding,
sort properties and row statistics are not fabricated.

Redshift imports native `SVV_*` definitions and declared keys into the same
normalized snapshot without PostgreSQL-specific catalog facts. Privilege queries
run separately against leader-node catalog interfaces; they are not combined with
`SVV_*` queries that may execute on compute nodes. A discovery login's accessible
schemas and whole-relation SELECT grants bound the imported relations. Native
SUPER/VARBYTE names are preserved even when their frontend representations differ.
Unknown view numeric precision, generated status and unavailable native facts
remain unknown. Key discovery follows the PG catalog interfaces used by the
[Amazon Redshift JDBC driver](https://github.com/aws/amazon-redshift-jdbc-driver/blob/master/src/main/java/com/amazon/redshift/jdbc/RedshiftDatabaseMetaData.java).
Real Redshift does not enforce its declared primary/foreign keys.

`RelationId` is the exact `(database, schema, relation)` coordinate. `ColumnId` adds
the column name. Virtual frontend schema/relation OIDs derive from these coordinates;
collisions reject the candidate generation. They remain stable for unchanged names,
but are not durable identities across drops/recreates. Retain source identity and
generation with a future bound column reference.

## Refresh and failure behavior

```yaml
schema_discovery:
  target: snowflake
  backend_login: reader
  catalog:
    schemas: [PUBLIC, ANALYTICS]
    refresh_interval_seconds: 60
    refresh_timeout_ms: 10000
    max_age_seconds: 300
    max_rows: 100000
    max_bytes: 16777216
```

`schemas` is required for all engines. Other fields above are defaults.
`catalog` is required for PostgreSQL, Redshift and Snowflake discovery. Missing or empty
schema scope prevents startup. Library hosts must attach a matching loaded catalog
with `GatewayConfig::with_catalog` before calling any serving entry point. Scope is
exact and bounded to 256 schema names, not an application access policy.

Initial loading must succeed before accepting clients. PostgreSQL collects shared
definitions and typed source-specific facts in one admitted, read-only repeatable-read
transaction. Redshift uses one admitted read-only transaction with the database's
native isolation level; PostgreSQL repeatable-read DDL semantics are not promised.
Provider statements share row/byte bounds and a refresh deadline. Snowflake
reads definitions and scoped SHOW KEYS using one admitted SQL API binding, with
independent autocommit requests. Row/byte bounds cover the combined candidate. Only a complete,
validated result becomes a generation; this does not promise stronger DDL consistency
than the source supplies. No partially loaded candidate is published.

A response pins `Arc<Snapshot>`. Refreshes preserve pinned readers. `status()` reports
generation, age, last attempt, refresh state and typed error. Failed/interrupted loads
retain the previous generation without resetting its age. Reads fail once `max_age`
is exceeded; an empty schema remains distinguishable from a load failure.
`refresh()` rejects overlapping attempts. `stop_refreshing().await` joins the periodic
worker; hosts call it before releasing backend resources.

The byte limit covers owned payloads/strings, excluding allocator and connector
memory. Old pinned generations and a candidate can coexist. Hosts must bound retained
generations. Native capacity remains occupied until underlying work actually stops,
including after a refresh timeout. Snowflake metadata responses obey the SQL API
response byte bound and the catalog row/byte limits.

## Frontend responses

Exact whole-statement handlers reproduce captured PostgreSQL/Redshift JDBC metadata,
Metabase PostgreSQL/Redshift sync, Redash PostgreSQL/Redshift autocomplete and SQLAlchemy
reflection. They return expected aliases, types, ordering, defaults, comments and
keys. Source identifiers and LIKE escapes retain their case. Unsupported source
types or missing facts needed for a response fail explicitly.

Metabase's Redshift driver lowercases schema/table filters for field discovery.
For Snowflake catalogs, these handlers resolve those filters against source names
without changing returned identifiers. Case-distinct names that make a folded
filter ambiguous fail explicitly. PostgreSQL-backed matching remains exact.
Both frontend profiles support the driver's inherited bulk foreign-key query,
including schema-filtered sync requests. These read declared keys from the snapshot
for every source engine.

BI tools may also sample application rows during schema sync to fingerprint fields
or populate filter values. Those are data queries and use ordinary authorization,
backend selection and source execution. Frontend `SET` and supported `SHOW` probes
are handled locally without source execution.

Capture slots accept designated bounded literal strings/IN lists. SQLAlchemy's
virtual relation OID slot accepts only a bounded unsigned integer. Nested SQL,
additional statements, changed catalog sources and nonliteral filters never match.
Recognition uses the gateway grammar before query context. Native or custom
remarks do not change metadata matching.

Snapshot responses bypass application context, selection and result caching. Failed
frontend transactions reject metadata until rollback. Previews and value scans stay
on ordinary routing, including when initiated by schema synchronization.

Both engines serve supported metadata entirely from the configured snapshot and its
explicit schema scope. Unknown classified catalog requests fail with `0A000`;
unavailable or expired snapshots fail with `55000`. Neither path falls back to a
source query, including inside frontend transactions. Source metadata reads occur
only during initial loading and refresh. Unsupported catalog SQL, including broader
psql/DBeaver inspection shapes, requires a reviewed snapshot handler.

PostgreSQL snapshots retain real owners, ACLs, native relation/schema identifiers,
view definitions, ordered constraints, index facts, scoped domains/enums, formatted
native types and identity sequence options. Version/profile probes use captured
source settings. Source grants filter relations, columns and keys; index definitions
require whole-relation SELECT access. Foreign keys outside the configured scope or
referencing inaccessible columns are excluded. Visible columns retain reported
primary-key membership even when the complete key is outside their visibility.
These metadata grants are observations
at refresh time, not a live authorization check.

Snowflake has no physical PostgreSQL indexes or user-defined PostgreSQL domains/enums.
Compatible empty results describe that absence rather than inventing source objects.
Snowflake SQLAlchemy identity-option reflection is explicitly unsupported when the
required sequence details are unavailable.

The Redshift client dialect does not request physical PostgreSQL indexes through
its index-reflection API; PostgreSQL-profile reflection exposes those captured facts.

The pinned `sqlalchemy-redshift` 0.8.14 dialect retains SQL quotes in primary and
foreign key identifiers parsed from constraint definitions: its reflection API
returns `"ID"` including the quote characters for an uppercase source column.
JDBC metadata and PostgreSQL SQLAlchemy reflection return `ID`. The catalog keeps
the exact source name and valid quoted definitions. Redshift SQLAlchemy consumers
must decode these identifier tokens; automatic key/relationship matching in a BI
application may require a client dialect correction.

## AST lineage and masking

The optional [AST lineage context](ast-lineage.md) reads the currently
available catalog during local analysis after query modification and releases it
before selection, result hooks, or execution.
It resolves output constituents and native JSON paths using stored columns/types;
missing facts remain explicit unknowns. Views and generated columns are source
boundaries. No schema generations are retained in lineage contexts, and no
per-query source metadata lookup occurs. Dictionary classification, dynamic masking,
and their policies remain separate work.
