# Redshift development server

This uses the latest published [Oblako Redshift image](https://github.com/almostly/oblako/tree/main/oblako/images/redshift)
(`deburky/redshift-local:latest`). The YASP image adds application fixtures and
Amazon's `redshift-connector==2.2.0` in an isolated Python directory. It does not
replace Oblako's source, SQL rewrites, catalog definitions, or installed dependencies.
Each build with `--pull` resolves the current upstream image; no commit or digest
is pinned. Qualification reports record the actual server and connector versions.

## Start the persistent server

First create local certificates using [TLS setup](../../docs/tls.md).
`config/local-tls` must contain `server.crt`, `server.key`, and `ca.crt`.
The source certificate must name `localhost` and `127.0.0.1`.

Run from the repository root:

```sh
docker compose -f dev/redshift/compose.yml build --pull
docker compose -f dev/redshift/compose.yml up -d --wait
docker compose -f dev/redshift/compose.yml exec -T redshift python3 /yasp/qualify.py seed
export YASP_REDSHIFT_READER_PASSWORD=yasp_reader_password
cargo run -- --config config/redshift-demo.yml
```

The source publishes **localhost:5439** and the demo YASP gateway listens on
**localhost:6434**. The source database is `yasp_demo`; its fixture administrator
is `yasp_admin` with password `yasp_local_password`. Set `YASP_REDSHIFT_PASSWORD`
before first initialization to choose another administrator password. Changing
the environment does not change a password already stored in the volume.

The seed creates `public.users`, `analytics.orders`, `analytics.events` with a
SUPER payload, an aggregate view, and composite-key metadata fixtures. It creates
read-only `yasp_reader` and ungranted `yasp_denied` logins with local fixture
passwords `yasp_reader_password` and `yasp_denied_password`. To override these
initial passwords, pass `YASP_REDSHIFT_READER_PASSWORD` and
`YASP_REDSHIFT_DENIED_PASSWORD` to the seed process. Existing logins are preserved.

Connect BI tools using the existing Redshift or PostgreSQL driver settings and
the gateway identity from `.env`. The source database/login is independent of
the gateway database label. The gateway and source have separate TLS settings;
the official connector trusts the local CA through `SSL_CERT_FILE`.

The Compose-owned `redshift-data` volume survives container recreation. `seed` is
idempotent application setup, but it replays application DDL; it is not a
persistence test.

```sh
docker compose -f dev/redshift/compose.yml down
```

Only `down --volumes` deliberately removes this environment's data.

## Qualify the actual emulator

```sh
docker compose -f dev/redshift/compose.yml exec -T redshift python3 /yasp/qualify.py check --baseline
docker compose -f dev/redshift/compose.yml exec -T redshift python3 /yasp/qualify.py check
```

Both commands execute every conformance query and print a JSON report. `--baseline`
requires scalar SQL, metadata, supported SUPER functions, driver
negotiation and access enforcement. The full command additionally requires native
Redshift behavior and returns nonzero when it differs. A passing baseline does not
mean full Redshift conformance.

The image qualified on 27 October 2026 has these native validation gaps:

- Bare SUPER values carry PostgreSQL JSONB OID `3802`, not Redshift SUPER OID `4000`.
- `JSON_SERIALIZE(NULL::SUPER)` returns SQL NULL rather than JSON text `null`.
- PartiQL array iteration, `AT` indexes and object iteration fail in the proxy/parser.
- SUPER dot navigation is learned from DDL in process memory and is lost after
  container recreation. Persisted data and definitions alone do not restore it.

These cases remain visible in reports. YASP does not rewrite native queries to
hide them or patch the emulator. Native Redshift wire handling is checked
separately. Redshift MPP planning, service authentication, WLM, data sharing and
[informational key constraints](https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html)
require the corresponding service.
Late-binding views, Spectrum tables and materialized views also require service
qualification; the local fixture exercises ordinary tables and views.

## Automated tests

```sh
cargo test --locked --test redshift_protocol
python3 scripts/test.py -- --features redshift-tests --test redshift --test redshift_catalog
python3 scripts/test.py -- --features redshift-tests --test redshift_emulator
python3 scripts/test.py -- --all-features
```

The runner builds from the latest image, starts an owned disposable container,
generates TLS certificates, and writes the full connector capability report to
`target/compatibility/redshift-emulator.json`. Container names, images and storage
belong to that run and are removed on exit. PostgreSQL-only runs do not provision
the Redshift image.

To use this same persistent server for configured runs:

```sh
python3 scripts/test.py --configured -- --features redshift-tests --test redshift
```

The usual configured PostgreSQL/Redis/TLS prerequisites still apply. The runner
discovers the Compose container, or uses `YASP_REDSHIFT_CONTAINER`. Tests create
isolated databases, using Oblako's bundled initialization scripts unchanged and
then the application seed. Cleanup drops only the owned test database; it never
drops the development database.

The persistence check recreates its own container against an owned volume without
reseeding, writes `target/compatibility/redshift-persistence.json`, and removes
only those resources. The baseline checks durable data and metadata while
reporting the native SUPER-navigation restart gap. The same script without
`--baseline` returns nonzero until that native behavior works.
