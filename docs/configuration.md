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
| Redis URL | The variable named by `cache.url_env`, required only when caching is enabled. |
| Log level | `LOG_LEVEL`: DEBUG, INFO, WARN, ERROR; defaults to INFO. Logs go to stdout. |

The frontend database is a connection label; each target names its actual source
database. Use the same name for browsing because database-alias adaptation is not
implemented. Target hosts must be `localhost` or loopback IP addresses; ports must
be nonzero. Frontend and backend transports are plaintext local development only.
Provision source logins with the intended PostgreSQL grants and RLS policy.

## Routing and pools

Each named target/login pair gets a lazy pool. The optional `backend` block defaults
to 8 connections per pool and 16 total. Limits must be positive, and total capacity
must cover every configured pool at its per-pool limit. For four pairs of eight,
set `max_connections_total: 32`, as in the reference configuration.

```yaml
hooks:
  select_backend:
    type: rust
    callback: builtin.select_backend.passthrough
    params:
      target: primary
      backend_login: reader
```

The default selector is built-in passthrough. Empty params infer the sole configured
pair; multiple pairs require explicit `target` and `backend_login` strings. An
explicit passthrough mapping accepts exactly those two keys.

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
| `context_timeout_ms` | 2,000 | Context derivation wait; failure supplies empty context. |
| `query_timeout_ms` | 60,000 | Database query wait and PostgreSQL statement timeout. |
| `read_timeout_ms` | 600,000 | Next complete authenticated message, including idle/partial reads; not query execution. |
| `write_timeout_ms` | 60,000 | Each frontend response write. |
| `startup_timeout_ms` | 120,000 | Complete startup/authentication exchange. |
| `shutdown_timeout_ms` | 10,000 | Session drain after listener shutdown. |
| `tls.mode` | `local_development` | Plaintext mode; TLS is not implemented. |

Session, SQL, context, and timeout limits must be positive. Frontend frames must be
between 8 and 2,147,483,647 bytes; query/write timers cannot exceed 2,147,483,647 ms.
Backend connect, acquire, and cleanup deadlines are fixed separately at 10 seconds
each. PostgreSQL transaction-local idle timeout uses the larger query/write budget.
Prepared-statement and portal settings remain future configuration.

## Discovery and client setup

```yaml
schema_discovery:
  target: primary
  backend_login: reader
```

The target is inferred only when exactly one exists. An omitted login uses the
first login declared in that target's YAML, preserved through config serialization.
The selected pair must exist. Recognized metadata queries use this pair independently
of ordinary-query callbacks and caching. Library hosts use
`GatewayConfig::with_schema_discovery` to choose explicitly.

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
