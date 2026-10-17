# Runtime configuration

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

The process environment takes precedence over optional `.env` in the working
directory. The host parses `.env` without mutating the process environment. Keep
secret values out of YAML and logs.

| Resource | Secret/configuration source |
| --- | --- |
| Frontend username/password/database label | `YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD`, `YASP_GATEWAY_DATABASE`. |
| Backend endpoint/database | `targets.<name>.host`, `port`, `database`; `engine: postgresql`, `tls_mode: disable`. |
| Backend login | `targets.<name>.logins.<name>.username` and a `password_env` variable name. |
| Result-cache Redis URL | The variable named by `cache.url_env`, required only when caching is enabled. |
| Dataset Redis URL | The variable named by `application_contexts.url_env`, required when datasets are configured. |
| Log level | `LOG_LEVEL`: DEBUG, INFO, WARN, ERROR; defaults to INFO. Logs go to stdout. |

The frontend database is a connection label; each target names its actual source
database. Use the same name for browsing because database-alias adaptation is not
implemented. Target hosts must be `localhost` or loopback IP addresses; ports must
be nonzero. Frontend and backend transports are plaintext local development only.
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
    tls_mode: disable
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
            - {id: small_a, host: "127.0.0.1", port: 5433}
            - {id: small_b, host: "127.0.0.1", port: 5434}
        large:
          max_in_flight: 8
          max_connections: 12
          instances:
            - {id: large_a, host: "127.0.0.1", port: 5435}
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

| Setting under `gateway` | Default | Meaning |
| --- | --- | --- |
| `protocol` | `postgresql` | Only supported frontend protocol. |
| `listen` | `127.0.0.1:6432` | Loopback IP and nonzero port required. |
| `max_sessions` | 32 | Normal frontend session capacity. |
| `max_message_bytes` | 8,388,608 | Incoming frontend frame bound; backend frames have a fixed 8 MiB bound. |
| `max_sql_bytes` | 1,048,576 | SQL input size. |
| `max_context_bytes` | 65,536 | Encoded derived-context bound; nesting is limited to 64. |
| `context_timeout_ms` | 2,000 | Query-context derivation wait; failure supplies empty context. |
| `async_hook_timeout_ms` | 2,000 | Async Rust selection and cache-eligibility wait. |
| `query_timeout_ms` | 60,000 | Database query wait and PostgreSQL statement timeout. |
| `read_timeout_ms` | 600,000 | Next complete authenticated message, including idle/partial reads; not query execution. |
| `write_timeout_ms` | 60,000 | Each frontend response write. |
| `startup_timeout_ms` | 120,000 | Complete startup/authentication exchange. |
| `shutdown_timeout_ms` | 10,000 | Session drain after listener shutdown. |
| `tls.mode` | `local_development` | Plaintext mode; TLS is not implemented. |

Session, SQL, context, and timeout limits must be positive. Frontend frames must be
between 8 and 2,147,483,647 bytes; query/write timers cannot exceed 2,147,483,647 ms.
Backend connection and cleanup deadlines are fixed at 10 seconds. Acquisition
uses `backend.acquire_timeout_ms`, which defaults to 60,000 ms. PostgreSQL
transaction-local idle timeout uses the larger query/write budget.
Prepared-statement and portal settings remain future configuration.

## Shared application datasets

`application_contexts` configures host-owned JSON datasets shared with callbacks.
It is optional; an empty `datasets` mapping loads nothing. Each configured name maps
to a registered Rust or Python loader. Loaders return a JSON object whose fields are
stored directly as Redis hash fields; there is no customer wrapper. Field values may
be any JSON value, including arrays, scalars, and `null`.

```yaml
application_contexts:
  url_env: YASP_CONTEXT_REDIS_URL
  namespace: "yasp::datasets"
  refresh_timeout_ms: 30000
  max_dataset_bytes: 16777216
  max_lookup_bytes: 65536
  datasets:
    users:
      refresh_interval_seconds: 0
      loader:
        type: python
        path: ../hooks/examples/datasets.py
        callback: load_json
        params: {file: users.json}
```

Each dataset's refresh interval defaults to zero, which loads at startup only. A
positive value, such as `900`, schedules refreshes for that dataset. Startup always
loads fresh data even when Redis already contains a prior value. Concurrent starts share the load; a failed startup
load prevents serving. Scheduled refreshes use a Redis lease and revision check so
stale work cannot overwrite newer data. Lookups see the latest Redis value rather
than a process-local copy. Failed refreshes keep the previous value and retry at the
next interval. Manual refresh/invalidation controls are Phase 2 management work.

For example, dataset `users` stores field `42` as JSON object data at Redis
hash key `yasp::datasets::users::data`; the separate `email_locations` dataset stores
field `columns` as a JSON array at `yasp::datasets::email_locations::data`. Registry
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
connections for reads and refresh coordination/uploads. Its initial transport is
loopback TCP. Keep its namespace separate from `cache.namespace`.

## Discovery and client setup

```yaml
schema_discovery:
  target: primary
  backend_login: reader
```

The target is inferred only when exactly one exists. An omitted login uses the
first login declared in that target's YAML, preserved through config serialization.
The selected pair must exist. For multiple eligible compute instances, specify
`schema_discovery.compute` explicitly. Discovery uses the same connection and work
limits as ordinary queries. Recognized metadata queries use this binding independently
of ordinary-query callbacks and caching. Library hosts use
`GatewayConfig::with_schema_discovery` and optional
`with_schema_discovery_compute` to choose explicitly.

For DBeaver, use autocommit, connect to the configured gateway/source database, and
set PostgreSQL driver properties `preferQueryMode=simple`, `prepareThreshold=0`,
`binaryTransfer=false`, and local `sslmode=disable`. Refresh cached database trees
after configuration changes. Discovery supports the recorded psql 18 and DBeaver
query fixtures; this does not establish general BI compatibility.

`SET application_name` accepts at most 63 UTF-8 bytes. Discovery-only `SET search_path`
accepts at most 1,024 bytes and 16 entries; an explicit `pg_catalog` must be first.
Matching SHOW requests read frontend state. These startup parameters use the same
bounds. Ordinary reads retain the source's normal search path.

Clients may reaffirm fixed UTF8 encoding, standard strings on, DateStyle `ISO, MDY`,
TimeZone `UTC`, IntervalStyle `postgres`, bytea output `hex`, extra float digits `3`,
read-only mode on, and `read committed` isolation. Other settings, roles, client
transactions, and extended-query execution are unsupported. Startup probes the
source version; restart after a source version upgrade.

## Redis result cache

| Setting under `cache` | Default |
| --- | --- |
| `enabled` | `false`; the only cache opt-in. |
| `url_env` | `YASP_REDIS_URL` |
| `namespace` | `yasp:dev:v1` |
| `ttl_seconds` | 3,600; hits do not extend expiry. |
| `max_rows_per_query` | 10,000 |
| `max_buffered_bytes_total` | 33,554,432 |
| `eligibility` | None; an optional Rust/Python callback can bypass supported queries. |

Redis keys include a version marker, configured target and login names, and the
normalized SQL query ID. Names are length-prefixed to avoid delimiter collisions.
Different named pairs keep separate entries even when they share source credentials.
Compute-instance and physical source settings remain part of entry scope validation.
Dynamic masking will still load the selected source pair's cache entry and apply
current masks before output; masking is not implemented yet.

Hits skip PostgreSQL, so source grants and RLS can remain stale until expiry. Scope
checks isolate identity, source, and fixed execution context. Redis transport is
loopback TCP only with a fixed 100 ms operation deadline; runtime failures become
misses. Invalid URL/configuration fails startup.

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
