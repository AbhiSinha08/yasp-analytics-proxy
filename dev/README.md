# Development containers

Use Docker Engine or Docker Desktop with Docker Compose available. Run commands
below from the repository root.

| Environment | Setup | Published endpoints |
| --- | --- | --- |
| [BI tools](bi/README.md) | `docker compose -f dev/bi/compose.yml up -d --build` | Metabase `localhost:3000`, Redash `localhost:5000`, Superset `localhost:8088` |
| [Decision model](decision-model/README.md) | `docker compose -f dev/decision-model/compose.yml -f dev/decision-model/compose.cuda.yml up -d --build --wait` | Laya `localhost:8000` |
| [Snowflake emulator](snowflake/README.md) | `docker compose -f dev/snowflake/compose.yml up --build --wait` | Snowflake HTTP `localhost:64616` |

Complete each environment's initial setup before starting it. They have separate
Compose projects and can run together. Stop them with the same Compose command
and `down`.

The BI tools use the existing host PostgreSQL on port **5432** and Redis on
**6379**. Each tool has its own PostgreSQL metadata database; BI containers have
no persistent data volumes. The Snowflake emulator owns its documented named
volume. Run YASP separately from the BI containers on the host. Containers reach
host services through `host.docker.internal`.

These environments keep developer state. The [test runner](../tests/README.md)
uses separate disposable services and storage.
