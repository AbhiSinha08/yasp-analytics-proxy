#!/usr/bin/env python3
"""Run Cargo tests against disposable PostgreSQL 18 and Redis instances."""

import argparse
import contextlib
import hashlib
import uuid
import urllib.request
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


def tls_certificates(directory):
    """Disposable CA and identities; never write private keys into the checkout."""
    def openssl(*arguments):
        subprocess.run(["openssl", *map(str, arguments)], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    for ca in ["ca", "other-ca"]:
        openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                "-subj", f"/CN=YASP Test {ca}", "-keyout", directory / f"{ca}.key",
                "-out", directory / f"{ca}.crt", "-addext", "basicConstraints=critical,CA:TRUE")
    for name, usage, san, days in [
        ("server", "serverAuth", "DNS:localhost,IP:127.0.0.1", 2),
        ("client", "clientAuth", "DNS:client", 2),
        ("wrong", "serverAuth", "DNS:wrong.invalid", 2),
        ("expired", "serverAuth", "DNS:localhost,IP:127.0.0.1", 0),
    ]:
        openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={name}",
                "-keyout", directory / f"{name}.key", "-out", directory / f"{name}.csr")
        extension = directory / f"{name}.ext"
        extension.write_text(f"basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage={usage}\nsubjectAltName={san}\n")
        openssl("x509", "-req", "-in", directory / f"{name}.csr", "-CA", directory / "ca.crt",
                "-CAkey", directory / "ca.key", "-CAcreateserial", "-days", days,
                "-extfile", extension, "-out", directory / f"{name}.crt")
    # A zero-day certificate expires at issuance; advance beyond that second.
    time.sleep(1.1)
    for key in directory.glob("*.key"):
        key.chmod(0o600)


@contextlib.contextmanager
def services():
    if os.getuid() == 0:
        raise RuntimeError("initdb requires an unprivileged user; run this script as your WSL/Linux user")
    binaries = postgres_bin()
    for program in ["psql", "redis-server", "redis-cli", "cargo", "openssl"]:
        if not shutil.which(program):
            raise RuntimeError(f"required test command is missing: {program}")
    with tempfile.TemporaryDirectory(prefix="yasp-tests-") as temporary:
        directory = Path(temporary)
        tls_certificates(directory)
        data = directory / "postgres"
        password_file = directory / "admin-password"
        admin_password = secrets.token_hex(24)
        password_file.write_text(admin_password + "\n")
        password_file.chmod(0o600)
        subprocess.run([binaries / "initdb", "-D", data, "-U", "yasp_admin", "--pwfile", password_file,
                        "--auth-host=scram-sha-256", "--auth-local=trust", "--no-locale", "--encoding=UTF8"],
                       check=True, stdout=subprocess.DEVNULL)
        password_file.unlink()
        postgres_port, redis_port, redis_tls_port = free_port(), free_port(), free_port()
        with (data / "postgresql.conf").open("a") as config:
            config.write(f"\nlisten_addresses = '127.0.0.1'\nport = {postgres_port}\n"
                         f"unix_socket_directories = '{directory}'\n"
                         f"ssl = on\nssl_cert_file = '{directory / 'server.crt'}'\n"
                         f"ssl_key_file = '{directory / 'server.key'}'\nssl_ca_file = '{directory / 'ca.crt'}'\n"
                         "fsync = off\nsynchronous_commit = off\nfull_page_writes = off\n")
        hba = data / "pg_hba.conf"
        hba.write_text("hostssl all all 127.0.0.1/32 scram-sha-256 clientcert=verify-ca\n" + hba.read_text())
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
                                          "--tls-port", str(redis_tls_port),
                                          "--tls-cert-file", str(directory / "server.crt"),
                                          "--tls-key-file", str(directory / "server.key"),
                                          "--tls-ca-cert-file", str(directory / "ca.crt"),
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
                    "database": "postgres", "tls": {"mode": "disable"},
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
                "YASP_TEST_TLS_DIR": str(directory),
                "YASP_TEST_REDIS_TLS_URL": f"rediss://localhost:{redis_tls_port}/",
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
    print("Running", *command, flush=True)
    process = subprocess.Popen(command, cwd=ROOT, env=environment, start_new_session=True)
    try:
        return process.wait(timeout=1800)
    finally:
        stop_test_processes(process)



def requested_features(arguments):
    features = set()
    for i, argument in enumerate(arguments):
        if argument == "--all-features":
            features.update(["python", "snowflake", "client-tests"])
        elif argument == "--features" and i + 1 < len(arguments):
            features.update(arguments[i + 1].replace(",", " ").split())
        elif argument.startswith("--features="):
            features.update(argument.partition("=")[2].replace(",", " ").split())
    return features


@contextlib.contextmanager
def external_services(environment, features):
    """Own only this run's Docker resources; never touch the developer volume."""
    if not features.intersection({"snowflake", "client-tests"}):
        yield environment
        return
    subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL)
    run_id = uuid.uuid4().hex
    environment = environment | {"YASP_JDBC_RUN": run_id}
    container = None
    images = []
    try:
        if "client-tests" in features:
            inventory = json.loads((ROOT / "tests/fixtures/clients/clients.json").read_text())
            image = inventory["metabase"]["image_digests"][0]
            if subprocess.run(["docker", "image", "inspect", image], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
                subprocess.run(["docker", "pull", image], check=True)
            client_image = "yasp-bi-clients-test:" + run_id
            images.append(client_image)
            subprocess.run(["docker", "build", "--target", "clients", "-f", "dev/snowflake/Dockerfile", "-t", client_image, "."], cwd=ROOT, check=True)
            environment["YASP_SQLALCHEMY_IMAGE"] = client_image
            environment |= {"YASP_JDBC_IMAGE": image, "YASP_JDBC_HOST": os.environ.get("YASP_JDBC_HOST", "host.docker.internal")}
        if "snowflake" in features:
            build = ROOT / "target/compatibility/native-build/build.json"
            patch = ROOT / "scripts/snowflake/snowflake-v1.11.0-prefetch.patch"
            native = json.loads(build.read_text()) if build.exists() else {}
            library = Path(native.get("library", "/missing"))
            digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
            if not library.is_file() or native.get("library_sha256") != digest(library) or native.get("patch_sha256") != digest(patch):
                subprocess.run([sys.executable, str(ROOT / "scripts/snowflake/build_native_driver.py")], check=True)
                native = json.loads(build.read_text())
                library = Path(native["library"])
            environment |= {"YASP_SNOWFLAKE_DRIVER": str(library), "YASP_SNOWFLAKE_DRIVER_SHA256": digest(library)}
            snowflake_image = "yasp-fakesnow-test:" + run_id
            images.append(snowflake_image)
            subprocess.run(["docker", "build", "--target", "server", "-f", "dev/snowflake/Dockerfile", "-t", snowflake_image, "."], cwd=ROOT, check=True)
            environment["YASP_SNOWFLAKE_IMAGE"] = snowflake_image
            container = "yasp-snowflake-test-" + run_id
            subprocess.run(["docker", "run", "-d", "--name", container,
                "--label", "yasp.compatibility=" + run_id,
                "-p", "127.0.0.1::64616", "--tmpfs", "/data:rw,size=256m",
                environment["YASP_SNOWFLAKE_IMAGE"], "--test"], check=True, stdout=subprocess.DEVNULL)
            port = subprocess.check_output(["docker", "inspect", container, "--format",
                '{{(index (index .NetworkSettings.Ports "64616/tcp") 0).HostPort}}'], text=True).strip()
            endpoint = "127.0.0.1:" + port
            environment["YASP_SNOWFLAKE_ENDPOINT"] = endpoint
            environment["YASP_SNOWFLAKE_FIXTURE"] = endpoint
            deadline = time.monotonic() + 90
            while True:
                state = subprocess.check_output(["docker", "inspect", container, "--format", "{{.State.Running}}"], text=True).strip()
                if state != "true":
                    subprocess.run(["docker", "logs", container], check=False)
                    raise RuntimeError("disposable Snowflake fixture exited during startup")
                try:
                    ready = subprocess.run(["docker", "exec", container, "python", "sql.py", "--health"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=8).returncode == 0
                except subprocess.TimeoutExpired:
                    ready = False
                if ready:
                    break
                if time.monotonic() >= deadline:
                    subprocess.run(["docker", "logs", container], check=False)
                    raise RuntimeError("disposable Snowflake fixture did not become ready")
                time.sleep(0.2)
        yield environment
        if container:
            deadline = time.monotonic() + 5
            while True:
                with urllib.request.urlopen("http://" + environment["YASP_SNOWFLAKE_ENDPOINT"] + "/_yasp_probe/status", timeout=3) as response:
                    counts = json.load(response)
                if counts["started"] == counts["stopped"] and counts["sessions_opened"] == counts["sessions_closed"]:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("Snowflake workers or sessions did not stop: " + json.dumps(counts))
                time.sleep(0.05)
    finally:
        owned = subprocess.check_output(["docker", "ps", "-aq", "--filter", "label=yasp.compatibility=" + run_id], text=True).split()
        if owned:
            subprocess.run(["docker", "rm", "-f", "-v", *owned], check=True, stdout=subprocess.DEVNULL)
        volumes = subprocess.check_output(["docker", "volume", "ls", "-q", "--filter", "label=yasp.compatibility=" + run_id], text=True).split()
        if volumes:
            subprocess.run(["docker", "volume", "rm", *volumes], check=True, stdout=subprocess.DEVNULL)
        for image in images:
            subprocess.run(["docker", "image", "rm", image], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--configured", action="store_true", help="use existing PostgreSQL/Redis services and YASP_TEST_CONFIG/.env")
    parser.add_argument("--full", action="store_true", help="default, Python, Snowflake and pinned JDBC functional/documentation matrix")
    parser.add_argument("cargo_arguments", nargs=argparse.REMAINDER, help="cargo test arguments after --")
    arguments = parser.parse_args()
    cargo_arguments = arguments.cargo_arguments
    if cargo_arguments[:1] == ["--"]:
        cargo_arguments = cargo_arguments[1:]
    if arguments.full and cargo_arguments:
        parser.error("--full runs the fixed acceptance matrix; use Cargo arguments for focused runs")
    matrix = [["--all-targets"], ["--doc"],
        ["--all-targets", "--features", "python"], ["--doc", "--features", "python"],
        ["--all-targets", "--all-features"], ["--doc", "--all-features"]] if arguments.full else [cargo_arguments or ["--all-targets"]]
    features = set().union(*(requested_features(args) for args in matrix))
    with contextlib.ExitStack() as stack:
        environment = os.environ.copy() if arguments.configured else stack.enter_context(services())
        if "python" in features:
            interpreter = environment.get("PYO3_PYTHON", sys.executable)
            library = subprocess.check_output([interpreter, "-c", "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"], text=True).strip()
            environment |= {"PYO3_PYTHON": interpreter,
                "LD_LIBRARY_PATH": library + (":" + environment["LD_LIBRARY_PATH"] if environment.get("LD_LIBRARY_PATH") else "")}
        environment = stack.enter_context(external_services(environment, features))
        for args in matrix:
            result = run_tests(args, environment)
            if result:
                raise SystemExit(result)
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    sys.exit(main())
