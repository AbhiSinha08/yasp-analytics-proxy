# Local development environments

The BI tools and Snowflake emulator run in Docker Compose. Laya runs as a native
Python service. Follow each setup guide before starting its service.

| Environment                                | Setup                                                                   | Published endpoints                                                           |
| ------------------------------------------ | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| [BI tools](bi/README.md)                   | `docker compose -f dev/bi/compose.yml up -d --build`                    | Metabase `localhost:3000`, Redash `localhost:5000`, Superset `localhost:8088` |
| [Decision model](decision-model/README.md) | Install its Python package and run `python dev/decision-model/serve.py` | Laya `localhost:8001`                                                         |
| [Snowflake emulator](snowflake/README.md)  | `docker compose -f dev/snowflake/compose.yml up --build --wait`         | Snowflake HTTP `localhost:64616`                                              |

These environments keep developer state. The [test runner](../tests/README.md)
uses separate disposable services and storage. Automated YASP tests mock the
decision-model client and do not download or run Laya.

The BI tools use the existing host PostgreSQL on port **5432** and Redis on
**6379**. Each tool has its own PostgreSQL metadata database; BI containers have
no persistent data volumes. The Snowflake emulator owns its documented named
volume. Run YASP separately from the BI containers on the host. Containers reach
host services through `host.docker.internal`.
