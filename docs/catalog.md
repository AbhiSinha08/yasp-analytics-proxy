# Shared schema catalog

YASP stores source definitions in bounded memory and refreshes immutable generations
periodically. Providers read metadata through the real backend adapters, using the
configured discovery identity, admission limits, deadlines and cleanup. Catalogs
contain no application rows and require no additional database service or disk snapshot.

Construct `Catalog::postgres` or feature-gated `Catalog::snowflake`, then attach its
`Arc` with `GatewayConfig::with_catalog`. The YAML host constructs either provider
from `schema_discovery`; a Snowflake-only host needs no PostgreSQL discovery binding.
`Backends::catalog` exposes the same configured-source construction to library hosts.
One handle can serve both frontend listeners and later AST consumers.

## Scope and source facts

Each catalog has an explicit schema list preserving source spelling and one source
identity. PostgreSQL identifies endpoint/database and principal. Snowflake identifies
account or emulator endpoint/database, principal, role, warehouse and default schema.
Catalog identities are not implicitly shared across roles or warehouses. PostgreSQL
catalogs must match PostgreSQL discovery; a Snowflake discovery binding must match
its catalog before the listener starts.

Both providers import schemas, relations, kinds, comments, columns, ordinals,
nullability, defaults, and native type details including available length/precision/
scale. PostgreSQL reports native OIDs/modifiers, declared primary-key membership,
and whole-relation SELECT privilege. Its source grants also filter schemas/columns.

Snowflake additionally imports view definitions and declared primary, unique and
foreign keys, with ordered local/referenced columns and referential actions. These
are declarations, not enforcement guarantees. Snowflake datetime precision and
generated-column status remain unknown; its information-schema datetime precision
is not applicable. Whole-table SELECT privilege is unknown for a real Snowflake
source: role-visible metadata alone is insufficient. The explicit local emulator
has unrestricted visibility and SELECT access; those facts do not establish real
Snowflake grants. Physical PostgreSQL indexes, owners, ACLs, distribution, encoding,
sort properties and row statistics are not fabricated.

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

`schemas` is required when enabling a catalog. Other fields above are defaults.
Omitting `catalog` retains live PostgreSQL discovery; Snowflake discovery requires
one. Scope is exact and bounded to 256 schema names, not an application access policy.

Initial loading must succeed before accepting clients. PostgreSQL uses one bounded
metadata statement. Snowflake reads definitions and scoped SHOW KEYS using one
admitted SQL API binding, with independent autocommit requests. Row/byte bounds cover the combined candidate. Only a complete,
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
Both frontend profiles support the driver's inherited bulk foreign-key query.

Capture slots accept designated bounded literal strings/IN lists. SQLAlchemy's
virtual relation OID slot accepts only a bounded unsigned integer. Nested SQL,
additional statements, changed catalog sources and nonliteral filters never match.
Native remarks bypass metadata handling; comments never grant discovery access.

Snapshot responses bypass application context, selection and result caching. Failed
frontend transactions reject metadata until rollback. Previews and value scans stay
on ordinary routing, including when initiated by schema synchronization.

PostgreSQL retains live discovery for other classified metadata. SQLAlchemy Redshift
reflection over PostgreSQL uses real owners, ACLs, view definitions, source identifiers
and declared constraints through that adapter. Snowflake responses instead use the
configured snapshot, including its explicit schema scope; unknown catalog requests
fail with `0A000`. Snowflake has no physical PostgreSQL indexes or user-defined
PostgreSQL domains/enums. Compatible empty results describe that absence rather
than inventing source objects. SQLAlchemy identity-option reflection is explicitly
unsupported when the required sequence details are unavailable.

The pinned `sqlalchemy-redshift` 0.8.14 dialect retains SQL quotes in primary and
foreign key identifiers parsed from constraint definitions: its reflection API
returns `"ID"` including the quote characters for an uppercase source column.
JDBC metadata and PostgreSQL SQLAlchemy reflection return `ID`. The catalog keeps
the exact source name and valid quoted definitions. Redshift SQLAlchemy consumers
must decode these identifier tokens; automatic key/relationship matching in a BI
application may require a client dialect correction.

## AST lineage and masking

The catalog provides qualified coordinates, keys, views and native types for future
AST binding. A binder must pin a generation and resolve wildcards, aliases, joins,
CTEs, subqueries, expressions, ambiguity and view boundaries. Coordinates alone do
not establish base-column lineage. Dynamic masking and its policy/cache behavior
remain separate planned work.
