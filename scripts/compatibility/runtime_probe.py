"""Run the optional Snowflake runtime against its owned Fakesnow HTTP fixture."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

from fakesnow_cancel import server

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-driver", type=Path, required=True)
    args = parser.parse_args()
    environment = dict(os.environ,
                       YASP_SNOWFLAKE_DRIVER=str(args.native_driver.resolve()),
                       YASP_SNOWFLAKE_DRIVER_SHA256=hashlib.sha256(args.native_driver.read_bytes()).hexdigest())
    with server() as (settings, counts):
        import snowflake.connector
        with snowflake.connector.connect(**settings) as connection:
            connection.cursor().execute("CREATE DATABASE YASP_RUNTIME")
            connection.cursor().execute("CREATE SCHEMA IF NOT EXISTS YASP_RUNTIME.PUBLIC")
            connection.cursor().execute('CREATE SCHEMA YASP_RUNTIME."MixedSchema"')
            connection.cursor().execute('CREATE TABLE YASP_RUNTIME.PUBLIC.METADATA_TYPES (AMOUNT NUMBER(38,9) NOT NULL, "MixedCase" VARCHAR(30), CREATED TIMESTAMP_NTZ(6))')
        environment["YASP_SNOWFLAKE_ENDPOINT"] = f"127.0.0.1:{settings['port']}"
        result = subprocess.run(["python3", "scripts/test.py", "--", "--test", "snowflake", "--features", "snowflake", "--", "--ignored", "--nocapture"],
                                cwd=ROOT, env=environment, timeout=600)
        deadline = time.monotonic() + 3
        while (counts["sessions_opened"] != counts["sessions_closed"] or counts["started"] != counts["stopped"]) and time.monotonic() < deadline:
            time.sleep(0.02)
        passed = (result.returncode == 0 and counts["started"] >= 1
                  and counts["started"] == counts["stopped"] == counts["aborted"]
                  and counts["sessions_opened"] == counts["sessions_closed"])
        report = {"passed": passed, "gateway_test_exit": result.returncode, "server": dict(counts)}
        target = ROOT / "target/compatibility/snowflake-runtime-report.json"
        target.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
        return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
