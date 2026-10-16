#!/usr/bin/env python3
"""Run Cargo tests against disposable PostgreSQL 18 and Redis instances."""

import argparse
import contextlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def postgres_bin():
    if os.environ.get("YASP_TEST_PG_BIN"):
        directory = Path(os.environ["YASP_TEST_PG_BIN"])
    elif Path("/usr/lib/postgresql/18/bin/initdb").exists():
        directory = Path("/usr/lib/postgresql/18/bin")
    else:
        directory = Path(subprocess.check_output(["pg_config", "--bindir"], text=True).strip())
    version = subprocess.check_output([directory / "postgres", "--version"], text=True)
    if " 18." not in version:
        raise RuntimeError("PostgreSQL 18 is required by the captured discovery scenarios")
    return directory


@contextlib.contextmanager
def services():
    if os.getuid() == 0:
        raise RuntimeError("initdb requires an unprivileged user; run this script as your WSL/Linux user")
    binaries = postgres_bin()
    for program in ["psql", "redis-server", "redis-cli", "cargo"]:
        if not shutil.which(program):
            raise RuntimeError(f"required test command is missing: {program}")
    with tempfile.TemporaryDirectory(prefix="yasp-tests-") as temporary:
        directory = Path(temporary)
        data = directory / "postgres"
        password_file = directory / "admin-password"
        admin_password = secrets.token_hex(24)
        password_file.write_text(admin_password + "\n")
        password_file.chmod(0o600)
        subprocess.run([binaries / "initdb", "-D", data, "-U", "yasp_admin", "--pwfile", password_file,
                        "--auth-host=scram-sha-256", "--auth-local=trust", "--no-locale", "--encoding=UTF8"],
                       check=True, stdout=subprocess.DEVNULL)
        password_file.unlink()
        postgres_port, redis_port = free_port(), free_port()
        with (data / "postgresql.conf").open("a") as config:
            config.write(f"\nlisten_addresses = '127.0.0.1'\nport = {postgres_port}\n"
                         f"unix_socket_directories = '{directory}'\n"
                         "fsync = off\nsynchronous_commit = off\nfull_page_writes = off\n")
        redis = None
        started = False
        try:
            subprocess.run([binaries / "pg_ctl", "-D", data, "-l", directory / "postgres.log", "-w", "start"],
                           check=True, stdout=subprocess.DEVNULL)
            started = True
            environment = os.environ | {
                "PGHOST": "127.0.0.1", "PGPORT": str(postgres_port), "PGUSER": "yasp_admin",
                "PGDATABASE": "postgres", "PGPASSWORD": admin_password, "PGSSLMODE": "disable",
                "PGPASSFILE": "/dev/null", "PGCONNECT_TIMEOUT": "3",
            }
            primary_password, secondary_password = secrets.token_hex(24), secrets.token_hex(24)
            subprocess.run(["psql", "-X", "-w", "-v", "ON_ERROR_STOP=1"], env=environment,
                           input=f"CREATE ROLE yasp_owner LOGIN NOSUPERUSER CREATEDB PASSWORD '{primary_password}';\n"
                                 f"CREATE ROLE yasp_reader LOGIN NOSUPERUSER NOCREATEDB PASSWORD '{secondary_password}';\n",
                           text=True, check=True, stdout=subprocess.DEVNULL)
            with (directory / "redis.log").open("w") as log:
                redis = subprocess.Popen(["redis-server", "--bind", "127.0.0.1", "--port", str(redis_port),
                                          "--save", "", "--appendonly", "no", "--dir", directory],
                                         stdout=log, stderr=log, start_new_session=True)
            redis_url = f"redis://127.0.0.1:{redis_port}/"
            deadline = time.monotonic() + 5
            while True:
                assert redis.poll() is None, "isolated Redis exited during startup"
                ready = subprocess.run(["redis-cli", "-u", redis_url, "PING"], capture_output=True)
                if ready.returncode == 0 and ready.stdout.strip() == b"PONG":
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError("isolated Redis did not become ready")
                time.sleep(0.03)
            config = {
                "version": 1,
                "targets": {"primary": {
                    "engine": "postgresql", "host": "127.0.0.1", "port": postgres_port,
                    "database": "postgres", "tls_mode": "disable",
                    "logins": {
                        "restricted": {"username": "yasp_owner", "password_env": "YASP_TEST_TARGET_PASSWORD"},
                        "secondary": {"username": "yasp_reader", "password_env": "YASP_TEST_SECOND_PASSWORD"},
                    },
                }},
                "hooks": {"select_backend": {
                    "type": "rust", "callback": "builtin.select_backend.passthrough",
                    "params": {"target": "primary", "backend_login": "restricted"},
                }},
            }
            config_path = directory / "config.yml"
            config_path.write_text(json.dumps(config))
            print("Testing with disposable PostgreSQL 18 and Redis; both source logins are non-superusers.", flush=True)
            yield os.environ | {
                "YASP_TEST_CONFIG": str(config_path), "YASP_TEST_REDIS_URL": redis_url,
                "YASP_TEST_TARGET_PASSWORD": primary_password, "YASP_TEST_SECOND_PASSWORD": secondary_password,
                "YASP_GATEWAY_USERNAME": "yasp_frontend", "YASP_GATEWAY_DATABASE": "analytics",
                "YASP_GATEWAY_PASSWORD": secrets.token_hex(24),
            }
        finally:
            if redis is not None:
                redis.terminate()
                try:
                    redis.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    redis.kill()
                    redis.wait()
            if started or (data / "postmaster.pid").exists():
                subprocess.run([binaries / "pg_ctl", "-D", data, "-m", "immediate", "-w", "stop"],
                               check=True, stdout=subprocess.DEVNULL)
            print("Disposable test services stopped and their data removed.", flush=True)


def stop_test_processes(process):
    # Cargo may exit before its integration binaries or gateway children.
    # The new session belongs only to this run; wait for the entire group.
    def signal_group(value):
        try:
            os.killpg(process.pid, value)
            return True
        except ProcessLookupError:
            return False

    if signal_group(signal.SIGTERM):
        deadline = time.monotonic() + 5
        while signal_group(0) and time.monotonic() < deadline:
            process.poll()  # Reap Cargo even when another group member remains.
            time.sleep(0.03)
        signal_group(signal.SIGKILL)
    process.wait()


def run_tests(arguments, environment):
    command = ["cargo", "test", "--locked", *arguments]
    process = subprocess.Popen(command, cwd=ROOT, env=environment, start_new_session=True)
    try:
        return process.wait()
    finally:
        stop_test_processes(process)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--configured", action="store_true", help="use existing services and YASP_TEST_CONFIG/.env")
    parser.add_argument("cargo_arguments", nargs=argparse.REMAINDER, help="cargo test arguments after --")
    arguments = parser.parse_args()
    cargo_arguments = arguments.cargo_arguments
    if cargo_arguments[:1] == ["--"]:
        cargo_arguments = cargo_arguments[1:]
    cargo_arguments = cargo_arguments or ["--all-targets"]
    if arguments.configured:
        return run_tests(cargo_arguments, os.environ.copy())
    with services() as environment:
        return run_tests(cargo_arguments, environment)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    sys.exit(main())
