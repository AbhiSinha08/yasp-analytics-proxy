# Optional Snowflake connector diagnostics

Functional acceptance belongs to `python3 scripts/test.py --full`. These experiments
compare the rejected Rust driver manager, the native C ABI and negative controls;
they are not prerequisites for ordinary PostgreSQL testing. The separate Cargo
workspace keeps rejected dependencies out of YASP.

```sh
python3 -m venv /tmp/yasp-snowflake-probe
/tmp/yasp-snowflake-probe/bin/pip install -r tests/support/snowflake/requirements.lock
python3 scripts/snowflake/build_native_driver.py
/tmp/yasp-snowflake-probe/bin/python tests/diagnostics/snowflake/snowflake_probe.py \
  --native-driver target/compatibility/native-build/libadbc_driver_snowflake.so
```

The diagnostic includes synthetic write/rollback outside YASP's read-only surface.
Its report records failed candidates as diagnostics. Accepted native execution,
handle ownership, cancellation and bounded-prefetch checks also run in the normal
Snowflake functional tests. Native artifacts and reports live under `target/compatibility`.

To update the pinned client inventory deliberately, inspect a running Metabase:

```sh
python3 scripts/inventory_metabase.py --container yasp-metabase \
  --output tests/fixtures/clients/clients.json
```

Functional JDBC tests use the manifest's image digest directly; a running Metabase
application is not required.
