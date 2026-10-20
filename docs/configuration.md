# Runtime configuration

Library hosts can attach a shared, periodically refreshed schema catalog with
`GatewayConfig::with_catalog`. Its source identity, schema scope, refresh/age and
row/byte limits are described in [Shared schema catalog](catalog.md). The YAML host
enables the configured source provider through `schema_discovery.catalog`.

The host loads ignored `config/local.yml`; `--config PATH` selects another file.
[config/example.yml](../config/example.yml) is the full loadable reference. Its
commented future settings are not accepted runtime fields. Omitted supported fields
use defaults; unknown fields, duplicate keys, invalid types, versions other than 1,
and files larger than 64 KiB are rejected.

```sh
cp config/example.yml config/local.yml
cp .env.example .env
# Configure provisioned source databases, logins, and their secret variables.
cargo run --locked -- --config config/local.yml
```

## Secrets and targets

The YAML host accepts PostgreSQL targets and, with `--features snowflake`, Snowflake
targets. Each engine has its own settings. A Snowflake-only configuration uses its
own discovery binding and normalized catalog; no PostgreSQL placeholder is needed.

The process environment takes precedence over optional `.env` in the working
directory. The host parses `.env` without mutating the process environment. Keep
secret values out of YAML and logs.

| Resource                                  | Secret/configuration source                                                                       |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Frontend username/password/database label | `YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD`, `YASP_GATEWAY_DATABASE`.                        |
| Backend endpoint/database                 | `targets.<name>.host`, `port`, `database`; `engine: postgresql`, `tls.mode: disable` or `enable`. |
| Backend login                             | `targets.<name>.logins.<name>.username` and a `password_env` variable name.                       |
| Result-cache Redis URL                    | The variable named by `cache.url_env`, required only when caching is enabled.                     |
| Dataset Redis URL                         | The variable named by `application_contexts.url_env`, required when datasets are configured.      |
| Log level                                 | `LOG_LEVEL`: DEBUG, INFO, WARN, ERROR; defaults to INFO. Logs go to stdout.                       |

The frontend database is a connection label; each target names its actual source
database. Use the same name for browsing because database-alias adaptation is not
implemented. Ports must be nonzero. Each connection independently selects plaintext
or verified TLS, including private-network endpoints. See [TLS setup](tls.md) for modes,
certificate paths, outbound client identities, and library examples.
Provision source logins with the intended PostgreSQL grants and RLS policy.

## Routing and pools

The single selector may return `target`, `backend_login`, and `compute_type`.
Omitted target/login fields resolve only when there is exactly one choice in their
scope. A target without compute settings uses its ordinary endpoint. A target with
compute settings uses its explicit `default_type`, or its sole type. Ambiguous
omissions and invalid explicit choices deny the query; they never fall back to
another source identity.

```yaml
hooks:
  select_backend:
    type: rust
    callback: builtin.select_backend.passthrough
    params:
      target: primary
      backend_login: reader
```

The built-in passthrough uses the same resolution rules. Empty params are useful
for a single target/login. Python `None` denies explicitly; `{}` requests default
resolution. Custom policy remains application-owned.

### Optional compute types

Only targets that use compute classes need a `compute` block. The PostgreSQL
adapter supports configured local endpoints; other engines require their own
adapters. Instances are tried in list order within the selected type.

```yaml
targets:
  analytics:
    engine: postgresql
    database: analytics
    tls:
      mode: disable
    logins:
      reader:
        username: report_reader
        password_env: REPORT_PASSWORD
    compute:
      default_type: small
      types:
        small:
          max_in_flight: 4
          max_connections: 8
          instances:
            - { id: small_a, host: "127.0.0.1", port: 5433 }
            - { id: small_b, host: "127.0.0.1", port: 5434 }
        large:
          max_in_flight: 8
          max_connections: 12
          instances:
            - { id: large_a, host: "127.0.0.1", port: 5435 }
```

Type limits apply separately to each instance and are shared across all its logins.
A login can restrict `allowed_computes` to named instances and provide credential
`overrides.<instance>` containing a complete `username`/`password_env` pair. Omitted
eligibility permits all configured instances. The application must verify intended
access under every credential binding.

### Capacity and waiting

Pools open connections on demand. Defaults are eight physical connections per pool
and sixteen total. The total is enforced across actual pooled source connections,
including idle, creating, and cleanup states; it does not reserve every pool's
maximum. Each compute instance also enforces its shared `max_connections` and
`max_in_flight` bounds. Idle sessions can be reclaimed when another binding needs
physical capacity. Cancellation uses its separate control connection.

Overflow stays within the selected type and login. If all eligible instances or
the shared budget are full, acquisition waits asynchronously for capacity, up to
`backend.acquire_timeout_ms` (60 seconds by default), then reports SQLSTATE
`53300`. The frontend session can continue. Set this value in milliseconds; it
must be between 1 and 2,147,483,647.
Waiting consumes a bounded frontend session slot, not a dedicated worker thread;
there is no durable query queue. Redis-backed queuing remains planned for Phase 2.
Connection failures can try another eligible instance before application SQL is
submitted; source permission errors and submitted queries are not retried.

Hook references use `type`, `callback`, and an optional JSON-compatible `params`
mapping. Python also requires `path`. Compiled string names are shorthand with
empty params. The repository host registers the passthrough selector, the built-in
SQL-fragment cache eligibility callback, and Python references when the feature is
enabled. Rust context providers and other Rust names require a consuming host that
registers them. Names and application params are resolved before serving clients;
organization-specific params stay outside the engine. See [extensions](extensions.md).

## Gateway defaults and limits

| Setting under `gateway` | Default          | Meaning                                                                                 |
| ----------------------- | ---------------- | --------------------------------------------------------------------------------------- |
| `protocol`              | `postgresql`     | `postgresql` (SCRAM) or experimental `redshift` (password over required TLS). |
| `listen`                | `127.0.0.1:6432` | IP address and nonzero port; TLS is selected independently with `enable` or `disable`.  |
| `max_sessions`          | 32               | Normal frontend session capacity.                                                       |
| `max_message_bytes`     | 8,388,608        | Incoming frontend frame bound; backend frames have a fixed 8 MiB bound.                 |
| `max_sql_bytes`         | 1,048,576        | SQL input size.                                                                         |
| `max_context_bytes`     | 65,536           | Encoded derived-context bound; nesting is limited to 64.                                |
| `context_timeout_ms`    | 2,000            | Query-context derivation wait; failure supplies empty context.                          |
| `async_hook_timeout_ms` | 2,000            | Async Rust selection and cache-eligibility wait.                                        |
| `query_timeout_ms`      | 60,000           | Database query wait and PostgreSQL statement timeout.                                   |
| `read_timeout_ms`       | 600,000          | Next complete authenticated message, including idle/partial reads; not query execution. |
| `write_timeout_ms`      | 60,000           | Each frontend response write.                                                           |
| `startup_timeout_ms`    | 120,000          | Complete TLS negotiation and startup/authentication exchange.                           |
| `shutdown_timeout_ms`   | 10,000           | Session drain after listener shutdown.                                                  |
| `tls.mode`              | `disable`        | `disable` accepts plaintext; `enable` requires TLS with `cert_file` and `key_file`.     |

Session, SQL, context, and timeout limits must be positive. Frontend frames must be
between 8 and 2,147,483,647 bytes; query/write timers cannot exceed 2,147,483,647 ms.
Backend connection and cleanup deadlines are fixed at 10 seconds. Acquisition
uses `backend.acquire_timeout_ms`, which defaults to 60,000 ms. PostgreSQL
transaction-local idle timeout uses the larger query/write budget for autocommit
execution and `read_timeout_ms` for explicit client transactions.
Prepared-statement and portal settings remain future configuration.

The Redshift profile requires `tls.mode: enable`. It advertises version 8.0.2,
implements simple-query framing, and leaves Redshift protocol extensions disabled.
The YAML host supports PostgreSQL and optional Snowflake backends. Captured JDBC
and BI metadata queries have focused coverage; full query-builder compatibility remains
unvalidated. See [multi-engine support](multi-engine.md).

`hooks.input_options` optionally registers the compiled
`builtin.input_options.metabase` resolver with a required `native_dialect` param
(`postgresql`, `redshift`, or `snowflake`). Only a valid leading Metabase remark
whose `queryType` is `native` selects that dialect. MBQL, absent, malformed, and
unknown remarks retain the listener's dialect by default. Optional
`unmarked_data_dialect` selects a native data default after recognized frontend
setup/metadata handling; explicit MBQL still uses the frontend dialect. Input is
byte-bounded before this callback. Data SQL still runs read-only validation, context,
and selection. PostgreSQL rejects a mismatched explicit native dialect after selection.
Snowflake forwards accepted data SQL unchanged, including MBQL; backend SQL errors
are returned to the client. Native hints never select
the discovery identity. An omitted resolver preserves the listener's input grammar.

## Shared application datasets

`application_contexts` configures host-owned JSON datasets shared with callbacks.
It is optional; an empty `datasets` mapping loads nothing. Each configured name maps
to a registered Rust or Python loader. Loaders return a JSON object whose fields are
stored directly as Redis hash fields; there is no customer wrapper. Field values may
be any JSON value, including arrays, scalars, and `null`.

```yaml
application_contexts:
  url_env: YASP_CONTEXT_REDIS_URL
  namespace: "yasp:datasets"
  refresh_timeout_ms: 30000
  max_dataset_bytes: 16777216
  max_lookup_bytes: 65536
  datasets:
    users:
      refresh_interval_seconds: 0
      loader:
        type: rust
        callback: builtin.dataset.load_json
        params: { file: hooks/examples/users.json }
```

The built-in JSON file loader requires only `params.file`; relative paths resolve
from the process working directory (the project root when started there). The file
must contain a JSON object and fit within `max_dataset_bytes`. Use an application-
owned Rust or Python loader when data must be fetched from another service.

Each dataset's refresh interval defaults to zero, which loads at startup only. A
positive value, such as `900`, schedules refreshes for that dataset. Startup always
loads fresh data even when Redis already contains a prior value. Concurrent starts share the load; a failed startup
load prevents serving. Scheduled refreshes use a Redis lease and revision check so
stale work cannot overwrite newer data. Lookups see the latest Redis value rather
than a process-local copy. Failed refreshes keep the previous value and retry at the
next interval. Manual refresh/invalidation controls are Phase 2 management work.

For example, dataset `users` stores field `42` as JSON object data at Redis
hash key `yasp:datasets:users:data`; the separate `email_locations` dataset stores
field `columns` as a JSON array at `yasp:datasets:email_locations:data`. Registry
state records `format_version`, `revision`, `loaded_at_ms`, and
`next_refresh_at_ms`; its internal empty marker is hidden from callbacks.

The core accepts at most 64 nesting levels, 100,000 nonempty fields with names up to 256 bytes,
and `max_dataset_bytes` per dataset. Each value is limited by `max_lookup_bytes`.
Full reads and `get_many` are limited to 64 fields and the same reply-byte bound.
Python callbacks use `request["datasets"].get(name, key)`, `get_many(name, keys)`,
and `read(name)`; these synchronous helpers call the async registry and return ordinary
Python values. Python loaders use a dedicated loader worker and share one interpreter
and its GIL with the callback runtime. Keep loader work bounded and avoid expensive
CPU work. `refresh_timeout_ms` bounds each load. Dataset Redis uses separate
connections for reads and refresh coordination/uploads. Its transport supports
plaintext `redis://` or verified `rediss://` TLS. Configure optional CA/client identity
under `application_contexts.tls`; see [TLS setup](tls.md). Keep its namespace
separate from `cache.namespace`.

## Discovery and client setup

```yaml
schema_discovery:
  target: primary
  backend_login: reader
  catalog:
    schemas: [public]
```

The target is inferred only when exactly one exists. An omitted login uses the
first login declared in that target's YAML, preserved through config serialization.
The selected pair must exist. For multiple eligible compute instances, specify
`schema_discovery.compute` explicitly. Discovery uses the same connection and work
limits as ordinary queries. Recognized metadata queries use this binding independently
of ordinary-query callbacks and caching. Library hosts use
`GatewayConfig::with_schema_discovery` and optional
`with_schema_discovery_compute` to choose explicitly.

`schema_discovery.catalog` enables bounded, periodically refreshed definitions from
that same discovery target, login and optional compute. `schemas` lists exact source
names; it is required when the catalog is enabled. Initial loading must succeed
before the listener accepts clients. Omit `catalog` to retain PostgreSQL discovery
forwarding. Redshift Metabase's captured table/column scans require the catalog;
Redshift-specific views are answered locally rather than forwarded to PostgreSQL.
See [catalog configuration](catalog.md#refresh-and-failure-behavior) for refresh, age and memory bounds.

### JDBC clients

Use the driver matching `gateway.protocol`. Both profiles require simple-query
mode; extended-query execution is unsupported. Use the configured gateway database
and frontend credentials, rather than backend login credentials.

For DBeaver, use autocommit, connect to the configured gateway/source database, and
set PostgreSQL driver properties `preferQueryMode=simple`, `prepareThreshold=0`,
`binaryTransfer=false`, and local `sslmode=disable`. Refresh cached database trees
after configuration changes. Discovery supports the recorded psql 18 and DBeaver
query fixtures; this does not establish general BI compatibility.

For the Redshift gateway, use the Amazon Redshift JDBC driver with these connection
properties in Metabase or DBeaver:

| Property | Value | Purpose |
| --- | --- | --- |
| `preferQueryMode` | `simple` | Use the supported wire protocol. |
| `compression` | `off` | Disable Redshift protocol compression. |
| `ApplicationName` | `yasp` | Supply an explicit name within the 63-byte limit. |
| `ssl` | `true` | Redshift password authentication requires gateway TLS. |
| `sslmode` | `verify-full` | Verify both the certificate chain and hostname. |
| `sslrootcert` | Path to the issuing CA certificate | Trust a private CA without disabling verification. |

In Metabase, choose **Amazon Redshift**, enable SSL, and paste the following into
**Additional JDBC connection string options**, adjusting the certificate path:

```text
ssl=true&sslmode=verify-full&sslrootcert=/tmp/yasp-ca.crt&preferQueryMode=simple&compression=off&ApplicationName=yasp
```

For Metabase in Docker, the CA path must exist inside its container. Containers
reach the local gateway through `host.docker.internal:6432`; the server certificate
must include that hostname in its subject alternative names. With the repository's
local CA, a one-time copy to the existing container is:

```sh
docker cp config/local-tls/ca.crt yasp-metabase:/tmp/yasp-ca.crt
```

The copy survives restarts but not container recreation. For a durable deployment,
mount only the public CA certificate read-only at the same path:

```yaml
volumes:
  - /home/abhisinha08/code/yasp/config/local-tls/ca.crt:/tmp/yasp-ca.crt:ro
```

In DBeaver, set these values under **Driver properties**, using a CA path readable
by the desktop JVM and a hostname covered by the certificate, such as `localhost`.
Use autocommit for ordinary reads when YASP result caching is desired. The
PostgreSQL-only `prepareThreshold` and `binaryTransfer` settings above are not
requirements for the Redshift driver. See the
[Redshift JDBC options](https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-configuration-options.html)
and [Metabase certificate setup](https://www.metabase.com/docs/latest/databases/ssl-certificates).

### Python clients (psycopg2 and SQLAlchemy)

Redash and Superset use Python database drivers. JDBC properties such as
`ApplicationName`, `preferQueryMode`, and `compression` must not be added to their
connection parameters. Use libpq's `application_name` spelling when needed.
Psycopg2 uses the shared PostgreSQL wire protocol, so its PostgreSQL runner can
also connect to a Redshift listener. The configured listener still determines
the frontend dialect and metadata profile; the driver name does not select one.

For all BI tools using psycopg2, enable autocommit so ordinary reads can use YASP's
result cache. For SQLAlchemy connections, set the engine's `isolation_level` to
`AUTOCOMMIT`. In BI connection settings that accept `engine_params` JSON, use:

```json
{"engine_params":{"isolation_level":"AUTOCOMMIT"}}
```

For direct psycopg2 connections, set `connection.autocommit = True`.
Psycopg2 uses simple queries and does not accept JDBC's `preferQueryMode` option.
Its default transactions are supported, but explicit transactions bypass YASP's
result cache. Disable the BI tool's query-result cache so YASP owns result caching.

For Superset's **Amazon Redshift** connection, install a compatible
`sqlalchemy-redshift` dialect in its Python environment. The tested local image
uses Superset 6.1.0, SQLAlchemy 1.4.54, psycopg2-binary 2.9.13 and
`sqlalchemy-redshift==0.8.14`. Version 1.0 of that dialect requires SQLAlchemy 2;
do not upgrade Superset's SQLAlchemy dependency just to install the latest dialect.
Restart Superset after installing the driver. Include it in the image build so
container recreation retains it. See [Superset's Redshift setup](https://superset.apache.org/user-docs/databases/supported/amazon-redshift/).

Use this SQLAlchemy URI, substituting the frontend password:

```text
redshift+psycopg2://yasp:PASSWORD@host.docker.internal:6432/development?sslmode=verify-full&sslrootcert=/tmp/yasp-ca.crt&application_name=superset
```

Mount the public CA into the Superset container at that path. In **Advanced →
Other → Engine Parameters**, enter `{"isolation_level":"AUTOCOMMIT"}`. This editor
accepts the engine options directly; the surrounding `engine_params` key belongs
only in the full Extra JSON representation shown above. Test and save the
connection after clearing any JDBC-only parameters.

Connection testing, native SQL and the captured table/column/constraint reflection
queries work with this local combination and a PostgreSQL discovery source.
Snowflake discovery supports the captured reflection surface through normalized
catalog facts; see [the local environment](../dev/snowflake/README.md).
The Redshift dialect queries fields such as `pg_class.reldiststyle`, late-binding
view columns and external catalogs. YASP recognizes the complete captured SQL,
accepts only bounded literal schema/table filters, and translates these metadata
requests inside the PostgreSQL adapter. They bypass ordinary context, selection
and result caching. Types, comments, defaults, view definitions, declared keys,
owners and ACLs come from PostgreSQL; Redshift distribution, encoding and sort-key
properties are NULL. Unknown SQL remains outside this compatibility surface.
These live PostgreSQL discovery responses use the configured identity rather than
the snapshot's imported schema list. Snowflake reflection instead uses the snapshot
scope, virtual identifiers and declared key definitions. Unknown source owners,
privileges and physical Redshift properties remain NULL. Unknown catalog requests
fail explicitly.

Redash 26.9's **Redshift** runner has an SSL-mode field but no CA-upload or JDBC
properties field. It supplies its bundled CA file at
`/app/redash/query_runner/files/redshift-ca-bundle.crt`. For a private issuing CA,
append the public CA certificate to that bundle, preserving the existing AWS
certificates, in the server and every query-worker/scheduler image. A read-only
mount of a prebuilt combined bundle at that same path is another option. Merely
copying a CA to `/tmp` does not configure this runner to trust it. Select
`verify-full`, use a hostname in the server certificate, and leave the optional
ad-hoc and scheduled query-group fields empty: those fields prepend a second
statement, outside the current single-statement query surface.

The captured Redash schema queries use the configured PostgreSQL or Snowflake
catalog snapshot. Refresh Redash's cached schema after restarting YASP with
catalog support. Saved query remarks contain a numeric `query_id`; new queries
contain `query_id: adhoc`. Both retain their numeric `user_id` for the example
context hook. A missing or invalid user remark still takes the configured fallback
route. Comments do not authenticate the BI user.

`SET application_name` accepts at most 63 UTF-8 bytes. `SET search_path`
accepts at most 1,024 bytes and 16 entries; an explicit `pg_catalog` must be first.
Matching SHOW requests read frontend state. These startup parameters use the same
bounds. Configure an explicit JDBC `ApplicationName` within this bound, including
for the Redshift driver. Both settings are replayed for PostgreSQL ordinary and metadata reads and participate
in ordinary result-cache scope. Changes inside a client transaction survive commit
and are restored on rollback.

Clients may reaffirm fixed UTF8 encoding, standard strings on, DateStyle `ISO, MDY`,
TimeZone `UTC`, IntervalStyle `postgres`, bytea output `hex`, extra float digits `3`,
read-only mode on, and `read committed` isolation. `SHOW TRANSACTION ISOLATION LEVEL`
also reports this fixed isolation level. Other settings, roles, and extended-query
execution are unsupported. PostgreSQL startup probes the
source version; restart after a source version upgrade. The experimental Redshift
profile accepts its driver's advisory `SET readonly=0/1` commands without changing
the engine's read-only enforcement and advertises its compatibility version.

### Read-only client transactions

`BEGIN` and `START TRANSACTION` support the fixed `READ ONLY`, `READ COMMITTED`
policy, followed by `COMMIT` or `ROLLBACK`. A pending transaction acquires no backend
until its first source query. That query pins the actual target/login/compute
connection; later decisions must permit it. A conflicting discovery binding also
fails. Empty commit/rollback needs no backend or routing metadata.

Errors leave the transaction failed: subsequent queries receive `25P02` until
rollback, and commit in failed state rolls back. `read_timeout_ms` bounds idle and
partial-message transactions. Explicit transactions bypass result-cache reads and
writes. Read-write requests, other isolation levels, nested BEGIN, savepoints,
chained transactions, and procedural BEGIN blocks are unsupported.

Native transactions support psycopg2/SQLAlchemy connection probes and ping.
SQL-based query cancellation is outside this supported subset; PostgreSQL wire
cancellation remains supported.

## Redis result cache

The engine applies caching to ordinary autocommit reads for every backend and both
frontend profiles. `cache.enabled` is the only opt-in; there is no backend-specific
cache setting. Metadata/setup requests and explicit transactions bypass reads and
writes. Backend SQL errors and incomplete results are never cached.

| Setting under `cache`      | Default                                                              |
| -------------------------- | -------------------------------------------------------------------- |
| `enabled`                  | `false`; the only cache opt-in.                                      |
| `url_env`                  | `YASP_REDIS_URL`                                                     |
| `namespace`                | `yasp:dev:v1`                                                        |
| `ttl_seconds`              | 3,600; hits do not extend expiry.                                    |
| `max_rows_per_query`       | 10,000                                                               |
| `max_buffered_bytes_total` | 33,554,432                                                           |
| `eligibility`              | None; an optional Rust/Python callback can bypass supported queries. |

Redis keys use `<namespace>:<target>_<login>:<query-id>`, for example
`yasp:cache:primary_analyst:<query-id>`. The query ID is the normalized SQL hash.
Within target and login names, `%`, `:`, and `_` are percent-escaped so the pair
occupies one Redis path segment and cannot collide with another pair.
Different named pairs keep separate entries even when they share source credentials.
Scope validates frontend profile, input dialect/mode, authenticated user/database,
actual backend endpoint/database/principal, role/warehouse/schema or compute instance,
fixed source settings, session search path/application name, query/write timers,
frame bound and response codec/execution revision. Different scopes may replace
the same query slot; a scope mismatch always executes against the selected source.
Dynamic masking will still load the selected source pair's cache entry and apply
current masks before output; masking is not implemented yet.

Hits skip the backend, so source grants and row-level security can remain stale until expiry. Scope
checks isolate identity, source, and fixed execution context. Redis transport is
plaintext `redis://` TCP or verified `rediss://` TLS with a fixed 100 ms operation
deadline, including connection establishment; runtime failures become
misses. Invalid URL/configuration fails startup. `cache.tls` accepts optional
`ca_file`, `cert_file`, and `key_file`; see [TLS setup](tls.md).

The byte setting is an aggregate in-process budget, separate from Redis storage.
The internal serialized-entry guard is one third of it; each active cached query
reserves three times that guard for buffered, Redis, and response copies. A result
that exceeds row/byte limits streams fully without caching. Configure Redis memory
and eviction separately.

To exclude freshness-sensitive SQL, configure
`builtin.cache_eligibility.exclude_sql_containing` with
`params.excluded_sql_fragments: [pg_stat_activity, clock_timestamp]`. Matching is a
case-insensitive literal substring check including comments/string literals. It
bypasses both reads and writes. See [hook examples](../hooks/README.md#cache-eligibility).

## Python runtime defaults

`hooks.python` is shared by all Python callbacks: `search_paths: []`,
`queue_capacity: 32`, `queue_max_bytes: 4194304`, `max_payload_bytes: 2097152`, and
`execution_timeout_ms: 2000`. Script/search paths in YAML are relative to the YAML
file. Limits must be positive, a payload must fit the byte budget, and counters must
fit their platform bounds. Build with `--features python`; install and expose the
matching interpreter and packages as described in [development](development.md#python-embedding).

## Snowflake targets

Build the normal host with `--features snowflake`. The checked-in
[demo YAML](../config/snowflake-demo.yml) connects either frontend to the persistent
[Docker emulator](../dev/snowflake/README.md) on a separate gateway port, 6433.
Gateway credentials and TLS retain their ordinary configuration.

A target uses `engine: snowflake`, `database`, and a `snowflake` mapping:

| Field | Meaning |
| --- | --- |
| `account`, `schema`, `warehouse` | Explicit Snowflake connection coordinates. |
| `emulator` | Optional numeric loopback HTTP endpoint for local tests only; omit for verified Snowflake HTTPS. |
| `max_response_bytes` | Bound on each decompressed JSON response, default 64 MiB; valid range 1 KiB–128 MiB. Includes one result partition and its metadata. |

Each Snowflake login supplies `username`, `role` and `private_key_file` (a PKCS#8
RSA PEM file, 2048–4096 bits). Paths resolve relative to the YAML file. An optional
`private_key_passphrase_env` names the environment variable for an encrypted key;
`password_env` is reserved for PostgreSQL. Register the corresponding public key
with the real Snowflake user. The adapter signs short-lived RS256 JWTs and uses
verified HTTPS at `https://<account>.snowflakecomputing.com`.

PostgreSQL host/port/TLS/compute settings cannot be mixed into a Snowflake target.
Admission is bounded per binding by the smaller backend pool and total limit;
acquisition uses `backend.acquire_timeout_ms`. The total is not yet a shared
cross-engine budget. Read-only frontend transaction blocks preserve routing and
failure states while each Snowflake statement autocommits. They provide no shared
snapshot or interactive source transaction.

Snowflake accepts routed native and frontend-generated data queries after normal
read-only validation. The adapter does not require a native remark or perform SQL
translation. The configured input dialect still controls parsing before routing.

Snowflake discovery requires `schema_discovery.catalog` with exact source schema
spelling. Catalog refreshes use the same configured source identity and adapter
capacity. An unavailable initial catalog prevents startup. Snowflake data results
use the shared cache when enabled. Configuration without the compiled feature fails
explicitly.

## Experimental Snowflake library host

Build with `--features snowflake` and construct a `SnowflakeConfig` for each
configured source identity. Register it with `Backends::with_snowflake`, then call
`gateway::serve_backends` using the existing selection hook. The PostgreSQL
registry can supply the designated discovery authority, or construct
`Backends::without_postgres()` and register Snowflake discovery. Eligible data bindings must
expose the same logical schema. A binding fixes the role and warehouse; remarks
only select SQL grammar. Snowflake results use the same engine-owned cache as
PostgreSQL results.

The compiled [Snowflake host example](../examples/snowflake_gateway.rs) reads
`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE` for metadata,
`YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD`, and `YASP_GATEWAY_DATABASE` for
the frontend, and `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_FILE`,
`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` (optional), `SNOWFLAKE_DATABASE`,
`SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE` and `SNOWFLAKE_WAREHOUSE` for data execution.

```sh
cargo run --locked --features snowflake --example snowflake_gateway
```

The example advertises the PostgreSQL profile and expects the normal Metabase
native remark. It uses verified HTTPS for Snowflake. The typed `emulator` endpoint
is reserved for explicit loopback HTTP fixtures. Capacity and acquisition timeout
apply to each binding. Capacity is retained during abandoned-request cleanup,
including interrupted submissions without a returned handle. See [SQL API execution
limits](multi-engine.md#sql-api-execution-and-values). The [normal test
runner](../tests/README.md) owns disposable Docker fixtures and temporary keys;
`python3 scripts/test.py --full` includes this coverage.
