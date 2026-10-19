# Client and Snowflake driver compatibility gates

These tools separate connector diagnostics, runtime execution, and client capture.
The standalone probes validate connector behavior. The separate runtime
probe exercises the optional `snowflake` adapter through the shared pgwire gateway.
The comparison Cargo package has independent locks. Default PostgreSQL builds omit
the optional ADBC/Arrow runtime dependencies.

## Client inventory

Read the existing Metabase's public version endpoint and bundled JAR resources:

```sh
python3 scripts/compatibility/inventory_metabase.py \
  --container yasp-metabase --url http://127.0.0.1:3000
```

The checked-in [client inventory](clients.json) pins the observed Metabase image
digest, version, revision, and bundled PostgreSQL/Redshift JDBC versions. It records
the selected simple-query mode and disabled Redshift compression as requirements,
not as changes to the running application's configuration. It does not establish
client compatibility. The JDBC SQL subset below is captured; full Metabase
workflow and protocol capture remain pending.

## Bundled JDBC SQL capture

```sh
python3 scripts/compatibility/jdbc_probe.py
```

This uses the pinned image already installed for `yasp-metabase`; it does not
change that application's configuration. Disposable client containers execute
the bundled JDBC drivers via the image's existing Clojure/JVM runtime. The runner
owns its PostgreSQL/Redis fixtures, mounts only the test CA certificate, and removes
its labelled client containers. PostgreSQL uses TLS `require`; Redshift uses the
fixture CA with `verify-ca`, following the driver's
[documented SSLMode values](https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-configuration-options.html).
The application name is explicitly bounded to YASP's 63-byte contract.

The ignored `jdbc` test records SQL and result shapes under
`target/compatibility/jdbc-*.json`. The checked-in SQL fixtures pin the captured
PostgreSQL 42.7.12 and Redshift 2.2.5 requests. The test attaches a refreshed
PostgreSQL catalog and checks populated table/column definitions plus schemas,
keys/indexes, previews and transactions for both drivers. Keys/indexes still use
the same PostgreSQL discovery authority. The Redshift pass also loads the bundled
Metabase driver and executes its own table-listing and filtered field-scan SQL,
checking boolean selectability, declared primary keys and column positions.
This is a captured compatibility subset;
the probe does not execute Metabase's MBQL compiler or capture every protocol frame.

## Snowflake network probe

The runtime fixture covers mixed-case schema and column names. Fakesnow 0.11.22
fails in its internal schema-switch SQL when creating a schema containing an
apostrophe or dot; those identifier cases need real Snowflake coverage. PostgreSQL
catalog tests cover both through its source provider.

Run in Linux/WSL with Rust 1.89.0 and Python 3.12. Create a dedicated environment;
do not install these packages into the gateway's Python-hook environment:

```sh
python3 -m venv /tmp/yasp-snowflake-probe
/tmp/yasp-snowflake-probe/bin/python -m pip install \
  -r scripts/compatibility/requirements.lock
/tmp/yasp-snowflake-probe/bin/python scripts/compatibility/snowflake_probe.py
```

Build and exercise the selected native candidate after installing the fixture:

```sh
python3 scripts/compatibility/build_native_driver.py
/tmp/yasp-snowflake-probe/bin/python scripts/compatibility/snowflake_probe.py \
  --native-driver target/compatibility/native-build/libadbc_driver_snowflake.so \
  --report target/compatibility/snowflake-patched-report.json
```

The native build supports Linux x86-64/WSL. It needs `patch` and a C/C++ compiler;
Go 1.26.1 and Apache ADBC 1.11.0 source archives are verified by pinned SHA-256
hashes. Caches, build products, upstream LICENSE/NOTICE, and `build.json` stay in
`target/compatibility/native-build`. A repeated build validates the formatted
source patch and reuses Go's cache. Runtime dependencies are enabled only with
the `snowflake` Cargo feature; default PostgreSQL builds omit Arrow and ADBC.

The patch limits outstanding streams until consumption rather than just limiting
producer goroutines. The unmodified wheel remains a negative comparison: running
the probe without `--native-driver` fails the read-ahead gate. Foundry 1.15.0 has a
similar upstream stream-window fix, but the tested artifact fails the separate
blocked-fetch cancellation gate. An explicit candidate path and digest appear in
every report; the runner never substitutes a different native driver silently.

The runner starts disposable Fakesnow HTTP fixtures on free loopback ports and
builds two Rust probes. `adbc_probe.rs` retains the rejected Rust manager as a
comparison. `adbc_ffi_probe.rs` exercises the replacement native C ABI directly
from Rust: exact decimals/NULLs, read-only transactions, concurrent cancellation,
and connection reuse. The shared implementation in
[`src/backend/snowflake/connection.rs`](../../src/backend/snowflake/connection.rs) exclusively borrows
connections and streams, supplies operation-scoped cancellation handles, and retains
native parents until every child is released. The probe checks stale-handle
rejection, error recovery, repeated statements, and balanced HTTP session cleanup.

## Native runtime gate

```sh
/tmp/yasp-snowflake-probe/bin/python scripts/compatibility/runtime_probe.py \
  --native-driver target/compatibility/native-build/libadbc_driver_snowflake.so
```

This launcher creates an HTTP fixture and disposable PostgreSQL/Redis services,
then runs the ignored `snowflake` integration test with the optional feature.
It checks lossless values, read-only transactions, source affinity, discovery
isolation, SQL errors, timeout, CancelRequest, disconnect, restored capacity, and
balanced native session cleanup. The report is
`target/compatibility/snowflake-runtime-report.json`.
Fakesnow's login fixture supplies the missing master token so the Python setup
client can delete its own session. Authentication/grants still require real-source
validation. The runtime adapter does not use Python.

`fakesnow_chunks.py` splits a normal seventeen-row Fakesnow Arrow result into an
inline row and sixteen HTTP chunks. The probe checks consumption backpressure and
cancellation while a real native download is blocked, including remote disconnect
and session cleanup.

`fakesnow_cancel.py` fixes the pinned emulator's no-op abort endpoint for testing.
It matches session/request UUIDs, interrupts the actual query, and records that its
handler stopped. A negative control preserves the original no-op behavior and
shows why client cancellation alone is insufficient. This shim uses private
Fakesnow APIs and is confined to this disposable fixture.

The Python manager independently checks read-only transactions. Its synthetic
write/rollback fixture remains a diagnostic and cannot fail the read-only gate.
No application configuration, secrets, or accounts are loaded. Native processes
have deadlines and emulator shutdown completes before the runner exits.

The result is written to `target/compatibility/snowflake-report.json`. The command
exits nonzero if a required read-only gate or negative control fails. `--skip-rust` is a diagnostic
option and cannot pass the gate. `--report PATH` selects another report path.
Cargo caches and build products default to `target/compatibility`; an explicit
`CARGO_HOME` is honored. Rust dependencies are pinned by the probe's Cargo.lock.

The Python requirements lock pins the full tested environment. `requirements.txt`
names the candidate direct dependencies. Fakesnow currently requires Snowflake
Connector 4.x converter APIs; the pinned 4.8.0 version supplies them.

The measured gate status and remaining implementation work are in the
[multi-engine compatibility guide](../../docs/multi-engine.md).
