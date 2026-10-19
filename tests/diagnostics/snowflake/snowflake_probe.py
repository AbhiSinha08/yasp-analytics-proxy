#!/usr/bin/env python3
"""Check candidate Snowflake drivers over a disposable Fakesnow HTTP server.

The Rust probe runs in child processes: a blocked native cancellation call must
not hang this runner or claim that a timed-out worker released its resources.
"""

import argparse
import importlib.metadata
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "tests/support/snowflake"))
VERSIONS = {
    "fakesnow": "0.11.22",
    "snowflake-connector-python": "4.8.0",
    "adbc-driver-snowflake": "1.11.0",
    "adbc-driver-manager": "1.12.0",
    "pyarrow": "26.0.0",
    "duckdb": "1.5.6",
}


def database_options(port):
    return {
        "adbc.snowflake.sql.account": "fakesnow",
        "username": "fake",
        "password": "snow",
        "adbc.snowflake.sql.uri.host": "127.0.0.1",
        "adbc.snowflake.sql.uri.port": str(port),
        "adbc.snowflake.sql.uri.protocol": "http",
        "adbc.snowflake.sql.client_option.login_timeout": "3s",
        "adbc.snowflake.sql.client_option.request_timeout": "3s",
        "adbc.snowflake.sql.client_option.disable_telemetry": "true",
        "adbc.snowflake.sql.client_option.use_high_precision": "true",
    }


def transaction_worker(port, read_only):
    import adbc_driver_manager as manager
    import adbc_driver_snowflake as snowflake
    import pyarrow

    def query(connection, sql):
        with manager.AdbcStatement(connection) as statement:
            statement.set_sql_query(sql)
            stream, _ = statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(stream.address) as reader:
                return reader.read_all().to_pylist()

    # Writes are exclusively synthetic fixture setup and a rollback observation.
    with snowflake.connect(db_kwargs=database_options(port)) as database:
        with manager.AdbcConnection(database) as connection:
            if read_only:
                connection.set_autocommit(False)
                try:
                    rows = query(connection, "SELECT 1 AS N")
                    if int(rows[0]["N"]) != 1:
                        raise AssertionError("transactional SELECT returned an unexpected value")
                    connection.rollback()
                finally:
                    connection.set_autocommit(True)
                return
            for sql in [
                "CREATE DATABASE YASP_ADBC_GATE",
                "CREATE SCHEMA YASP_ADBC_GATE.PUBLIC",
                "CREATE TABLE YASP_ADBC_GATE.PUBLIC.ITEMS (N NUMBER(38,0))",
                "INSERT INTO YASP_ADBC_GATE.PUBLIC.ITEMS VALUES (1)",
            ]:
                query(connection, sql)
            connection.set_autocommit(False)
            try:
                query(connection, "INSERT INTO YASP_ADBC_GATE.PUBLIC.ITEMS VALUES (2)")
                connection.rollback()
                rows = query(connection, "SELECT N FROM YASP_ADBC_GATE.PUBLIC.ITEMS ORDER BY N")
                if [int(row["N"]) for row in rows] != [1]:
                    raise AssertionError("rolled-back fixture row remained visible")
            finally:
                connection.rollback()
                connection.set_autocommit(True)


def run_json(command, timeout, *, env=None):
    result = subprocess.run(command, text=True, capture_output=True, timeout=timeout, env=env)
    records = [json.loads(line[7:]) for line in result.stdout.splitlines() if line.startswith("RESULT ")]
    if result.returncode or not records:
        raise RuntimeError(f"probe exited {result.returncode}: {result.stderr[-2000:]}")
    return records[-1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=ROOT / "target/compatibility/snowflake-report.json")
    parser.add_argument("--native-driver", type=Path, help="explicit candidate ADBC shared library; defaults to the pinned Python wheel library")
    parser.add_argument("--skip-rust", action="store_true", help="diagnostic Python run; cannot pass the Rust gate")
    parser.add_argument("--worker-port", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--read-only", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker_port is not None:
        try:
            transaction_worker(args.worker_port, args.read_only)
        except Exception as error:
            print("RESULT " + json.dumps({"passed": False, "error": f"{type(error).__name__}: {error}"}))
        else:
            print('RESULT {"passed": true}')
        return 0

    versions = {name: importlib.metadata.version(name) for name in VERSIONS}
    if versions != VERSIONS:
        raise RuntimeError("use the candidate versions pinned in tests/support/snowflake/requirements.txt")
    import adbc_driver_snowflake
    from fakesnow_cancel import server as fixed_server

    report = {"versions": versions, "rust_toolchain": "1.89.0", "checks": {}}
    report["checks"]["rust_build"] = {"passed": False, "error": "not run"}
    target = ROOT / "target/compatibility"
    target.mkdir(parents=True, exist_ok=True)
    binary = target / "rust/debug/yasp-snowflake-probe"
    environment = dict(os.environ, RUSTUP_TOOLCHAIN="1.89.0", CARGO_TARGET_DIR=str(target / "rust"))
    # The standard Cargo cache can be mounted read-only by the desktop host.
    environment.setdefault("CARGO_HOME", str(target / "cargo-home"))
    if not args.skip_rust:
        with tempfile.TemporaryFile(mode="w+") as log:
            try:
                build = subprocess.run(
                    ["cargo", "build", "--locked", "--manifest-path", str(HERE / "Cargo.toml")],
                    env=environment, stdout=log, stderr=log, timeout=600,
                )
                log.seek(0)
                output = log.read()
                report["checks"]["rust_build"] = {
                    "passed": build.returncode == 0,
                    "error": None if build.returncode == 0 else output[-4000:],
                }
            except subprocess.TimeoutExpired:
                report["checks"]["rust_build"] = {"passed": False, "error": "build exceeded 600 seconds"}

    native_driver = args.native_driver or Path(adbc_driver_snowflake._driver_path())
    report["native_driver"] = {"filename": native_driver.name, "sha256": hashlib.sha256(native_driver.read_bytes()).hexdigest()}
    report["connector_candidate"] = "Snowflake ADBC C ABI from Rust; Rust driver manager rejected"
    report["diagnostics"] = {}

    def native_probe(port, *options):
        process = subprocess.run(
            [str(target / "rust/debug/yasp-snowflake-ffi-probe"), str(native_driver), str(port), *options],
            text=True, capture_output=True, timeout=20,
        )
        if process.returncode:
            raise RuntimeError(f"native Rust probe exited {process.returncode}: {process.stderr[-2000:]}")
        return json.loads(process.stdout.splitlines()[-1])

    # Compare the two managers against the same corrected server. Local CANCELLED
    # status alone does not prove remote work stopped; verify the server counters.
    with fixed_server() as (settings, counts):
        port = settings["port"]
        try:
            report["checks"]["read_only_transaction"] = run_json(
                [sys.executable, str(Path(__file__).resolve()), "--worker-port", str(port), "--read-only"], 30,
            )
            report["diagnostics"]["synthetic_write_rollback"] = run_json(
                [sys.executable, str(Path(__file__).resolve()), "--worker-port", str(port)], 30,
            )
        except Exception as error:
            report["checks"]["read_only_transaction"] = {"passed": False, "error": str(error)}
        if report["checks"]["rust_build"]["passed"]:
            try:
                result = native_probe(port)
                deadline = time.monotonic() + 1
                while counts["stopped"] != counts["started"] and time.monotonic() < deadline:
                    time.sleep(0.01)
                result["server"] = dict(counts)
                result["passed"] = (
                    result.get("execute_status") == 9
                    and result.get("decimal_exact") is True
                    and result.get("read_only_transaction") is True
                    and result.get("connection_recovery") is True
                    and result.get("cancel_returned_success") is True
                    and result.get("cancel_call_ms", 20000) < 1000
                    and result.get("execution_error_recovery") is True
                    and result.get("inactive_cancel_rejected") is True
                    and result.get("owned_handle_cleanup") is True
                    and all(counts[key] == 1 for key in ("started", "aborted", "stopped"))
                    and counts["sessions_opened"] == counts["sessions_closed"]
                )
                report["checks"]["native_cancel_and_recovery"] = result
            except Exception as error:
                report["checks"]["native_cancel_and_recovery"] = {"passed": False, "error": str(error)}
            for mode in ("connection", "statement"):
                try:
                    process = subprocess.run(
                        [str(binary), str(native_driver), str(port), mode],
                        text=True, capture_output=True, timeout=20,
                    )
                    if process.returncode:
                        raise RuntimeError(f"Rust probe exited {process.returncode}: {process.stderr[-2000:]}")
                    result = json.loads(process.stdout.splitlines()[-1])
                    result["cancellation_supported"] = result.get("outcome") != "completed"
                    report["diagnostics"][f"rust_manager_{mode}_cancel"] = result
                except Exception as error:
                    report["diagnostics"][f"rust_manager_{mode}_cancel"] = {"error": str(error)}

    if report["checks"]["rust_build"]["passed"]:
        with fixed_server(interrupt=False) as (settings, counts):
            try:
                result = native_probe(settings["port"], "no-recovery")
                result["server_at_client_exit"] = dict(counts)
                result["remote_work_stopped"] = counts["stopped"] == counts["started"]
                report["diagnostics"]["unmodified_abort_endpoint"] = result
                report["checks"]["emulator_negative_control"] = {"passed": (
                    result.get("execute_status") == 9 and counts["started"] == 1 and counts["aborted"] == 1 and counts["stopped"] == 0
                )}
            except Exception as error:
                report["checks"]["emulator_negative_control"] = {"passed": False, "error": str(error)}

    if report["checks"]["rust_build"]["passed"]:
        for mode, fixture in [("prefetch", "prefetch"), ("fetch-cancel", "cancel")]:
            name = "bounded_prefetch" if mode == "prefetch" else "fetch_cancellation"
            with fixed_server(chunk_mode=fixture) as (settings, counts):
                try:
                    result = native_probe(settings["port"], mode)
                    deadline = time.monotonic() + 1
                    while counts["chunk_active"] and time.monotonic() < deadline:
                        time.sleep(0.01)
                    result["server"] = dict(counts)
                    balanced = counts["sessions_opened"] == counts["sessions_closed"]
                    if mode == "prefetch":
                        result["passed"] = (
                            result.get("rows") == 17 and result.get("recovery") is True and balanced
                            and result["before_consumption"]["chunk_requests"] <= 1
                        )
                    else:
                        result["passed"] = (
                            result.get("execute_returned") is True and result.get("cancel_returned_success") is True
                            and result.get("cancel_call_ms", 20000) < 1000 and result.get("recovery") is True
                            and result.get("outcome") != "completed" and counts["chunk_active"] == 0
                            and counts["chunk_cancelled"] >= 1 and balanced
                        )
                    report["checks"][name] = result
                except Exception as error:
                    report["checks"][name] = {"passed": False, "error": str(error)}

    report["passed"] = (
        not args.skip_rust
        and "native_cancel_and_recovery" in report["checks"]
        and all(check["passed"] for check in report["checks"].values())
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except importlib.metadata.PackageNotFoundError:
        raise SystemExit("install tests/support/snowflake/requirements.txt in an isolated virtual environment")
