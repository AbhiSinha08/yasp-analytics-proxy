# Local BI tools

Metabase, Superset and Redash use the host PostgreSQL server on port **5432** with
the development login **local / password**. Their metadata databases are
`metabase`, `superset` and `redash`. Redash uses the host Redis on **6379**, database
**0**. Redis also holds its task queues. These services must accept connections
from Docker through `host.docker.internal`.

Application metadata stays in PostgreSQL. There are no BI data volumes. The only
bind mount is the public gateway CA, exposed at `/tmp/yasp-ca.crt`; create it using
[TLS setup](../../docs/tls.md) before starting the stack.

## Configure and initialize

Run from the repository root:

```sh
python3 dev/bi/configure.py
PGPASSWORD=password psql -h localhost -p 5432 -U local -d postgres -f dev/bi/init.sql
docker compose -f dev/bi/compose.yml build
```

`configure.py` writes an ignored, owner-readable `dev/bi/.env` from
[.env.example](.env.example). It preserves credentials and encryption keys from
the existing `yasp-superset` and `yasp-redash-server` containers when available,
including stopped containers; fresh installations get generated keys. An existing
`.env` is left unchanged. Keep this file when recreating containers or restoring
metadata databases: replacing encryption keys can make saved connection passwords
unreadable. If the old containers are gone, restore their keys from your saved
environment before using their metadata databases.

The database script creates only missing databases and requires `CREATEDB` for
`local`. It does not reset or replace existing metadata. To initialize or migrate
Superset metadata:

```sh
docker compose -f dev/bi/compose.yml run --rm superset superset db upgrade
```

On a fresh database, create its administrator before `superset init`:

```sh
docker compose -f dev/bi/compose.yml run --rm superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password password
```

Then initialize its roles and permissions:

```sh
docker compose -f dev/bi/compose.yml run --rm superset superset init
```

Only on a fresh Redash database, create its tables:

```sh
docker compose -f dev/bi/compose.yml run --rm redash-server create_db
```

## Start and stop

If the old standalone containers are running, stop them before starting Compose
so they do not compete for ports or run duplicate background jobs:

```sh
docker stop yasp-metabase yasp-superset yasp-redash-server yasp-redash-worker yasp-redash-scheduler
docker compose -f dev/bi/compose.yml up -d
docker compose -f dev/bi/compose.yml logs -f
```

| Tool | Browser URL | Services |
| --- | --- | --- |
| Metabase | <http://localhost:3000> | `metabase` |
| Superset | <http://localhost:8088> | `superset` |
| Redash | <http://localhost:5000> | `redash-server`, `redash-worker`, `redash-scheduler` |

Existing users, data sources, charts and dashboards are loaded from the same
metadata databases. On a fresh installation, complete Metabase and Redash's setup
wizards to create their initial administrators. The existing local accounts use
password login with `password`; creating an empty metadata database does not
recreate those accounts or dashboards.

```sh
docker compose -f dev/bi/compose.yml down
```

`down` removes only this Compose project's containers and network. Host PostgreSQL
data, host Redis data and the existing Snowflake environment remain available.

## Connect to YASP

YASP runs separately in WSL. Use **host.docker.internal:6432**, gateway database
**development**, and login **yasp / yasp_password** for the current PostgreSQL
gateway configuration. The [Snowflake demo](../snowflake/README.md) uses **6433**.
Use the gateway protocol and credentials configured for that listener.

For JDBC clients, set `preferQueryMode=simple`, `prepareThreshold=0` and
`binaryTransfer=false`; Redshift JDBC also needs `compression=off`. For psycopg2
through SQLAlchemy, use engine parameter `isolation_level=AUTOCOMMIT`, represented
as `{"engine_params":{"isolation_level":"AUTOCOMMIT"}}` in connection JSON that
accepts that wrapper. Direct psycopg2 clients set `connection.autocommit = True`.
See [client setup](../../docs/configuration.md#discovery-and-client-setup) for TLS,
driver-specific UI fields and supported metadata requests.

Superset's image includes pinned PostgreSQL and Redshift drivers, disables its
query caches with `NullCache`, and retains local sessions for one year. Its session
cookie has its own name so BI tools on different localhost ports can stay logged
in together. The public gateway CA is mounted for libpq. Redash's server, workers
and scheduler append that CA to the image's original Redshift trust bundle on
every start.

Disable query-result caching in each BI connection/chart. In Redash, request a
fresh execution with API `max_age: 0` or the UI refresh action; saved results and
task queues remain part of Redash's normal application state. YASP's own result
cache remains controlled by the gateway configuration.
