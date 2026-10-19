#!/bin/sh
set -eu

# Redshift's runner uses its bundled trust store rather than libpq's default CA.
cat /app/redshift-ca-base.crt /tmp/yasp-ca.crt > /app/redash/query_runner/files/redshift-ca-bundle.crt
exec /app/bin/docker-entrypoint "$@"
