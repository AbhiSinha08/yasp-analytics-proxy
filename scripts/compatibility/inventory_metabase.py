#!/usr/bin/env python3
"""Read a running Metabase's version and bundled JDBC versions without changing it."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import urllib.request


def docker(*args):
    return subprocess.check_output(["docker", *args], timeout=15)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", default="yasp-metabase")
    parser.add_argument("--url", default="http://127.0.0.1:3000")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    with urllib.request.urlopen(args.url.rstrip("/") + "/api/session/properties", timeout=10) as response:
        version = json.load(response)["version"]
    image = docker("inspect", args.container, "--format", "{{.Image}}").decode().strip()
    digests = json.loads(docker("image", "inspect", image, "--format", "{{json .RepoDigests}}"))

    def resource(path):
        return docker("exec", args.container, "unzip", "-p", "/app/metabase.jar", path)

    postgres = re.search(rb"PostgreSQL JDBC Driver (\d+(?:\.\d+){2,3})", resource("org/postgresql/util/DriverInfo.class"))
    redshift = re.search(rb"(?m)^version=([^\r\n]+)", resource("redshift_jdbc_driver.properties"))
    if postgres is None or redshift is None:
        raise RuntimeError("bundled JDBC version resources were not recognized")
    result = {
        "metabase": {"version": version["tag"], "revision": version["hash"], "image_digests": digests},
        "postgresql_jdbc": postgres[1].decode(),
        "redshift_jdbc": redshift[1].decode(),
        "query_protocol": "simple",
        "redshift_compression": "off",
        "traffic_capture": "pending",
    }
    text = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(text)
    print(text, end="")


if __name__ == "__main__":
    main()
