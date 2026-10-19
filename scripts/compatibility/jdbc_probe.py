"""Capture setup/metadata SQL from the installed, pinned Metabase JDBC drivers."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", default="yasp-metabase")
    parser.add_argument("--host", default="host.docker.internal")
    args = parser.parse_args()
    image = subprocess.check_output(["docker", "inspect", args.container, "--format", "{{.Image}}"], text=True, timeout=15).strip()
    expected = json.loads((ROOT / "scripts/compatibility/clients.json").read_text())["metabase"]["image_digests"]
    digests = json.loads(subprocess.check_output(["docker", "image", "inspect", image, "--format", "{{json .RepoDigests}}"], text=True, timeout=15))
    if not set(expected).intersection(digests):
        raise RuntimeError("installed image differs from the pinned client inventory")
    run_id = uuid.uuid4().hex
    try:
        return subprocess.run(["python3", "scripts/test.py", "--", "--test", "jdbc", "--", "--ignored", "--nocapture"], cwd=ROOT,
                              env=dict(os.environ, YASP_JDBC_IMAGE=image, YASP_JDBC_HOST=args.host, YASP_JDBC_RUN=run_id), timeout=300).returncode
    finally:
        owned = subprocess.check_output(["docker", "ps", "-aq", "--filter", f"label=yasp.compatibility={run_id}"], text=True, timeout=15).split()
        if owned:
            subprocess.run(["docker", "rm", "-f", *owned], check=True, timeout=30, capture_output=True)


if __name__ == "__main__":
    raise SystemExit(main())
