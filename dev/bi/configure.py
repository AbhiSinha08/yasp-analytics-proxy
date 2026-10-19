"""Create local BI settings without replacing existing encryption keys."""

import json
import os
from pathlib import Path
import secrets
import subprocess

root = Path(__file__).resolve().parent
destination = root / ".env"
if destination.exists():
    raise SystemExit(f"{destination} already exists; left unchanged")

subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL)
values = {}
for container in ["yasp-superset", "yasp-redash-server"]:
    result = subprocess.run(
        ["docker", "inspect", container], capture_output=True, text=True, timeout=15
    )
    if result.returncode == 0:
        values.update(
            entry.split("=", 1)
            for entry in json.loads(result.stdout)[0]["Config"]["Env"]
        )

lines = []
for line in (root / ".env.example").read_text().splitlines():
    key, separator, value = line.partition("=")
    if separator and not line.startswith("#"):
        if key in values:
            value = values[key]
        elif key in {"SUPERSET_SECRET_KEY", "REDASH_SECRET_KEY", "REDASH_COOKIE_SECRET"}:
            value = secrets.token_hex(32)
        # Compose treats $ as interpolation even in values imported from .env.
        line = key + "='" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    lines.append(line)

with os.fdopen(os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
    output.write("\n".join(lines) + "\n")
print(f"Created {destination}; preserved available legacy container settings")
