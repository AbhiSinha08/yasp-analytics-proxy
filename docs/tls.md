# TLS connections

YASP supports TLS 1.2 and 1.3 for PostgreSQL client sessions, PostgreSQL source
connections, and Redis cache/dataset connections. TLS protects each connection
separately. Source grants, SCRAM credentials, Redis ACLs, and application policy
still control access.

## Choose TLS separately for each connection

Each connection has its own setting. An entirely private VPC deployment can use
plaintext throughout, or enable TLS only on selected links.

| Connection | Plaintext | Encrypted |
| --- | --- | --- |
| BI client → YASP | `gateway.tls.mode: disable` | `gateway.tls.mode: enable`, plus server certificate/key |
| YASP → PostgreSQL | `targets.<name>.tls.mode: disable` | `tls.mode: enable` |
| YASP → Redis result cache | `redis://` in the cache URL | `rediss://` in the cache URL |
| YASP → Redis datasets | `redis://` in the dataset URL | `rediss://` in the dataset URL |

Gateway and target TLS modes accept only `enable` and `disable`, defaulting to
`disable` when omitted. Plaintext listeners and backend endpoints may use private
IPs or DNS names. YASP uses your configured modes without classifying the network
as public or private. The default gateway listen address is `127.0.0.1:6432`.

For example, this host configuration uses plaintext for every connection:

```yaml
version: 1
gateway:
  listen: "0.0.0.0:6432"
  tls:
    mode: disable
targets:
  primary:
    engine: postgresql
    host: "10.0.1.20"
    port: 5432
    database: analytics
    tls:
      mode: disable
    logins:
      reader:
        username: analytics_reader
        password_env: YASP_SOURCE_PASSWORD
cache:
  enabled: true
  url_env: YASP_REDIS_URL
application_contexts:
  url_env: YASP_CONTEXT_REDIS_URL
  datasets:
    email_locations:
      loader:
        type: rust
        callback: example.datasets.email_locations
```

Set `YASP_REDIS_URL=redis://redis.internal:6379/0` and
`YASP_CONTEXT_REDIS_URL=redis://redis.internal:6379/1`, adding Redis credentials
to those URLs if configured. Supply the normal `YASP_GATEWAY_*` identity and
source password variables. No certificate files are needed for these plaintext
connections. Caching remains opt-in; omit the cache block to keep it disabled.

To encrypt only BI → YASP, set the gateway mode to `enable` and provide its
certificate/key as shown below; PostgreSQL and both Redis URLs can stay plaintext.
When TLS is disabled, omit that connection's certificate settings. When enabled,
server verification remains required and failures never fall back to plaintext.
Outbound client certificates are optional unless the PostgreSQL or Redis server
requires them.

## Host configuration

The example below requires certificates issued by your deployment's certificate
authority (CA). Paths resolve relative to the YAML file, including when `--config`
points outside the working directory. Store secrets in the named environment
variables. The Redis URLs must use `rediss://`, for example
`rediss://yasp:password@redis.example.com:6379/0`; URL-encode special characters
in credentials and never commit the URL.

```yaml
version: 1
gateway:
  listen: "0.0.0.0:6432"
  tls:
    mode: enable
    cert_file: certs/yasp-chain.pem
    key_file: certs/yasp-key.pem
targets:
  primary:
    engine: postgresql
    host: postgres.example.com
    port: 5432
    database: analytics
    tls:
      mode: enable
      ca_file: certs/source-ca.pem
      # Optional client identity, when required by PostgreSQL:
      cert_file: certs/source-client-chain.pem
      key_file: certs/source-client-key.pem
    logins:
      reader:
        username: analytics_reader
        password_env: YASP_SOURCE_PASSWORD
hooks:
  select_backend:
    type: rust
    callback: builtin.select_backend.passthrough
    params: {}
schema_discovery:
  target: primary
  backend_login: reader
cache:
  enabled: true
  url_env: YASP_REDIS_URL
  namespace: yasp:results:v1
  tls:
    ca_file: certs/redis-ca.pem
    cert_file: certs/redis-client-chain.pem
    key_file: certs/redis-client-key.pem
application_contexts:
  url_env: YASP_CONTEXT_REDIS_URL
  namespace: yasp:datasets:v1
  tls:
    ca_file: certs/redis-ca.pem
    cert_file: certs/redis-client-chain.pem
    key_file: certs/redis-client-key.pem
  datasets:
    email_locations:
      loader:
        type: rust
        callback: example.datasets.email_locations
```

Set `YASP_GATEWAY_USERNAME`, `YASP_GATEWAY_PASSWORD`, and
`YASP_GATEWAY_DATABASE` as in the [configuration guide](configuration.md), then run
`cargo run --locked -- --config /path/to/config.yml`.

The result cache remains opt-in. Enable it only after validating the intended
client's cache isolation and freshness requirements; a hit does not recheck source
grants or RLS. Datasets use their own Redis URL and namespace independently.

## Certificates and verification

- The gateway certificate's subject alternative names (SANs) must contain the DNS
  name used by BI clients. Connecting by IP requires an IP SAN. PostgreSQL and Redis
  certificates must similarly match their configured endpoint names. There is no
  hostname override or verification bypass.
- Certificate PEM files contain the leaf first, followed by intermediate
  certificates. Private keys must be unencrypted PEM files readable by the service
  account; restrict them with `chmod 600`. Keep keys and credential-bearing URLs
  outside version control. YASP does not issue certificates or decrypt keys.
- Outbound connections use the OS certificate store when `ca_file` is absent.
  A supplied CA PEM bundle replaces those roots, allowing a private CA to define
  trust. An empty or malformed bundle fails configuration.
- Client `cert_file` and `key_file` are optional together. They authenticate YASP
  to PostgreSQL or Redis where the server requests a client certificate; database
  username/password and Redis URL credentials are still configured normally.
  One PostgreSQL target's settings apply to discovery and all its compute instances.
- Certificate material loads before serving clients. Restart YASP after replacing
  certificates, keys, or trust bundles. Existing processes keep their loaded material.

For a local smoke test, create a disposable CA and a server certificate with SANs
`DNS:localhost,IP:127.0.0.1`, then explicitly trust that CA in clients. The isolated
[test runner](../scripts/test.py) demonstrates OpenSSL issuance and cleans up its keys.
Use your organization's certificate management for deployment.

## PostgreSQL and Redis servers

Enable PostgreSQL TLS in `postgresql.conf`:

```ini
ssl = on
ssl_cert_file = '/etc/postgresql/certs/server-chain.pem'
ssl_key_file = '/etc/postgresql/certs/server-key.pem'
# Required when verifying YASP's optional client certificate:
ssl_ca_file = '/etc/postgresql/certs/client-ca.pem'
```

In `pg_hba.conf`, restrict the application's network and require encrypted SCRAM
connections. The following also requires a trusted client certificate:

```text
hostssl analytics analytics_reader 10.0.0.0/24 scram-sha-256 clientcert=verify-ca
hostnossl all all 10.0.0.0/24 reject
```

Omit `clientcert=verify-ca` when client certificates are not required. Review rule
ordering and remove broader rules that admit plaintext connections. Retain source
role grants and read-only authorization. Follow the
[PostgreSQL TLS documentation](https://www.postgresql.org/docs/18/ssl-tcp.html).

Redis can expose only its TLS port:

```text
port 0
tls-port 6379
tls-cert-file /etc/redis/certs/server-chain.pem
tls-key-file /etc/redis/certs/server-key.pem
tls-ca-cert-file /etc/redis/certs/client-ca.pem
tls-auth-clients yes
```

Configure Redis ACL users/passwords and restrict network access. Redis requires
client certificates by default; configure the optional client identity in YASP,
or explicitly use `tls-auth-clients no` for server verification plus ACL/password
authentication. See [Redis TLS setup](https://redis.io/docs/latest/operate/oss_and_stack/management/security/encryption/).

## BI clients and cancellation

Use certificate and hostname verification in each client. For psql:

```sh
psql 'host=yasp.example.com port=6432 dbname=analytics user=bi_service sslmode=verify-full sslrootcert=/path/to/yasp-ca.pem channel_binding=require'
```

Supply the frontend password through a protected password file or your normal
secret mechanism. `channel_binding=require` verifies SCRAM's binding to the gateway
certificate. A compatible JDBC connection uses:

```text
jdbc:postgresql://yasp.example.com:6432/analytics?sslmode=verify-full&sslrootcert=/path/to/yasp-ca.pem&preferQueryMode=simple&binaryTransfer=false
```

Paths must exist in the BI application's runtime/container. Configure its username
and password separately. A BI tool's “require SSL” option may only request
encryption; select CA and hostname verification explicitly.

Cancellation opens another connection. When TLS is enabled, YASP accepts only TLS-encrypted
CancelRequest packets, including when normal session slots are full. YASP also
encrypts and verifies its separate PostgreSQL cancellation connection against the
original hostname while connecting to the original peer address. Legacy client
APIs that send plaintext cancellation cannot cancel through this listener; use a
client with encrypted cancellation support. Query deadlines remain enforced.
PostgreSQL documents the [legacy cancellation limitation](https://www.postgresql.org/docs/18/libpq-cancel.html).

Manual BI version, metadata, browsing, query, and cancellation validation remains
deployment-specific. TLS adds no extended-query or prepared-statement support.

## Library applications

The [compiling TLS host example](../examples/tls_gateway.rs) uses public library
APIs only and runs either `gateway::serve` or `gateway::serve_routed`:

```sh
# Source endpoint, credentials and optional outbound trust/client identity:
export PGHOST=postgres.example.com PGPORT=5432 PGUSER=analytics_reader PGDATABASE=analytics
export PGSSLROOTCERT=/path/to/source-ca.pem
export PGSSLCERT=/path/to/source-client-chain.pem PGSSLKEY=/path/to/source-client-key.pem
# Set PGPASSWORD and the YASP_GATEWAY_* credentials using your secret mechanism.
export YASP_TLS_CERT=/path/to/yasp-chain.pem YASP_TLS_KEY=/path/to/yasp-key.pem
export YASP_LISTEN=0.0.0.0:6432
cargo run --locked --example tls_gateway -- fixed
# Or choose the application-registered selector and routed pools:
cargo run --locked --example tls_gateway -- routed
```

Use `BackendConfig::new_with_tls(..., ClientTlsSettings)` for verified source
connections. Host targets use `targets.<name>.tls.mode` plus optional `ca_file`,
`cert_file`, and `key_file` fields inside the same `tls` block. Apply
`GatewaySettings` with `GatewayTls` through
`GatewayConfig::with_settings`. Configure `CacheSettings.tls` for `RedisCache::new`
and `ApplicationContextsSettings.tls` for `Datasets::start`, passing their `rediss://`
URLs separately. Supply explicit paths; library use does not implicitly read YAML,
environment variables, or the bundled host's organization hooks.

Use `BackendConfig::new` for plaintext PostgreSQL, `GatewayTls { mode: "disable".into(),
..Default::default() }` for a plaintext frontend on any configured listener, and
`redis://` with empty TLS settings for plaintext Redis. Gateway and target modes
default to `disable`. TLS failures never fall back to plaintext. Invalid material
prevents construction/startup; cache transport outages
become misses, dataset initial-load failures prevent serving, and dataset refresh
failures retain the previous value. Existing operation deadlines and frame bounds
apply with TLS enabled.

This milestone supports PostgreSQL SSLRequest negotiation. Direct TLS startup,
mixed plaintext/TLS listeners, frontend mutual TLS, certificate hot reload, and
automatic certificate provisioning are not supported.
