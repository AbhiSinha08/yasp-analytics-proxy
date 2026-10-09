"""Run the host with real psql/libpq and exercise its observable wire contract."""

import concurrent.futures
import contextlib
import ctypes
import ctypes.util
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import uuid

BINARY = str(Path(sys.argv[1]).resolve())
ADDRESS = ("127.0.0.1", 6432)
ROOT = Path(__file__).resolve().parents[1]
INPUT = json.loads(sys.stdin.read())
CONFIG, GATEWAY = INPUT["config"], INPUT["gateway"]
TARGET_NAME, TARGET = next(iter(CONFIG["targets"].items()))
LOGIN_NAME, LOGIN = next(iter(TARGET["logins"].items()))
MAINTENANCE_TARGET = TARGET.copy()
PASSWORDS = {settings["password_env"]: os.environ.get(settings["password_env"]) for settings in [GATEWAY, LOGIN]}
FRONTEND_ENV = {"YASP_GATEWAY_USERNAME": GATEWAY["username"], "YASP_GATEWAY_DATABASE": GATEWAY["database"]}
ENV = {k: v for k, v in os.environ.items() if not k.startswith(("PG", "YASP_")) and k not in PASSWORDS}
ENV["RUST_LOG"] = "info"
DEFAULT_PASSWORD = object()
LIBPQ = ctypes.CDLL(ctypes.util.find_library("pq"))
for name, args, result in [
    ("PQconnectdb", [ctypes.c_char_p], ctypes.c_void_p),
    ("PQstatus", [ctypes.c_void_p], ctypes.c_int),
    ("PQsocket", [ctypes.c_void_p], ctypes.c_int),
    ("PQfinish", [ctypes.c_void_p], None),
]:
    function = getattr(LIBPQ, name)
    function.argtypes, function.restype = args, result


def psql(*queries, password=DEFAULT_PASSWORD, user=None, database=None, ssl="disable", backend=False, maintenance=False, timeout=80, file=None):
    environment = ENV | {"PGPASSFILE": "/dev/null", "PSQL_HISTORY": "/dev/null", "PGSSLMODE": ssl, "PGCONNECT_TIMEOUT": "2"}
    settings = (MAINTENANCE_TARGET if maintenance else TARGET) if backend or maintenance else GATEWAY
    login = LOGIN if backend or maintenance else GATEWAY
    if password is DEFAULT_PASSWORD:
        password = PASSWORDS[login["password_env"]]
    user, database = user or login["username"], database or settings["database"]
    if password is not None:
        environment["PGPASSWORD"] = password
    host, port = (settings["host"], settings["port"]) if backend or maintenance else ADDRESS
    environment.update(PGHOST=host, PGPORT=str(port), PGUSER=user, PGDATABASE=database)
    command = ["psql", "-X", "-w", "-At", "-v", "VERBOSITY=verbose"]
    if file:
        command.extend(["-v", "ON_ERROR_STOP=1", "-f", str(file)])
    for query in queries:
        command.extend(["-c", query])
    return subprocess.run(command, env=environment, text=True, capture_output=True, timeout=timeout)


def health(**kwargs):
    result = psql("SELECT 1;", **kwargs)
    assert result.returncode == 0 and result.stdout.strip() == "1", result.stderr


def wait_ready(process, **kwargs):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        assert process.poll() is None, "gateway exited before becoming ready"
        result = psql("SELECT 1", **kwargs)
        if result.returncode == 0:
            assert result.stdout.strip() == "1"
            return
        time.sleep(0.03)
    raise AssertionError("gateway never became ready")


@contextlib.contextmanager
def server(directory, config_file=None, **overrides):
    with open(Path(directory) / "server.log", "w+") as log:
        command = [BINARY] + (["--config", str(config_file)] if config_file else [])
        process = subprocess.Popen(command, cwd=directory, env=ENV | overrides, stdout=log, stderr=log)
        try:
            yield process
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=13)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise AssertionError("gateway failed to shut down")
            log.seek(0)
            output = log.read()
            for value in [*PASSWORDS.values(), overrides.get("YASP_GATEWAY_PASSWORD")]:
                assert not value or value not in output, "password appeared in logs"
            assert "SECRET_MARKER" not in output, "invalid input appeared in logs"
            assert "panicked" not in output, output


def packet(kind, payload=b""):
    return kind + struct.pack("!I", len(payload) + 4) + payload


def read_exact(stream, count):
    data = bytearray()
    while len(data) < count:
        chunk = stream.recv(count - len(data))
        assert chunk, "unexpected connection close"
        data.extend(chunk)
    return bytes(data)


def responses(stream):
    result = []
    while True:
        kind = read_exact(stream, 1)
        length = struct.unpack("!I", read_exact(stream, 4))[0]
        assert 4 <= length <= 8 * 1024 * 1024
        result.append((kind, read_exact(stream, length - 4)))
        if kind == b"Z":
            return result


@contextlib.contextmanager
def authenticated_socket(backend=False):
    # Let the installed PostgreSQL client library perform SCRAM. Borrow its
    # authenticated socket only to send deliberately unusual protocol sequences.
    settings = TARGET if backend else GATEWAY
    login = LOGIN if backend else GATEWAY
    host, port = (settings["host"], settings["port"]) if backend else ADDRESS
    values = {"host": host, "port": port, "user": login["username"], "dbname": settings["database"],
              "password": PASSWORDS[login["password_env"]], "sslmode": "disable", "connect_timeout": 2}
    conninfo = " ".join(key + "='" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'" for key, value in values.items())
    connection = LIBPQ.PQconnectdb(conninfo.encode())
    try:
        assert connection and LIBPQ.PQstatus(connection) == 0, "libpq authentication failed"
        with socket.fromfd(LIBPQ.PQsocket(connection), socket.AF_INET, socket.SOCK_STREAM) as stream:
            stream.settimeout(3)
            yield stream
    finally:
        if connection:
            LIBPQ.PQfinish(connection)


def closed(stream):
    try:
        assert stream.recv(1) == b"", "connection should have closed"
    except ConnectionResetError:
        pass


def wire_query(query, backend=False):
    with authenticated_socket(backend=backend) as stream:
        stream.sendall(packet(b"Q", query.encode() + b"\0"))
        return responses(stream)


@contextlib.contextmanager
def fixture():
    database = "yasp_test_" + uuid.uuid4().hex
    created = False
    schema = "public"
    try:
        result = psql(f'CREATE DATABASE "{database}" TEMPLATE template0', maintenance=True)
        assert result.returncode == 0, "test login must be able to create a disposable database: " + result.stderr
        created = True
        TARGET["database"] = database
        result = psql(backend=True, file=ROOT / "tests/fixtures/read_forwarding.sql")
        assert result.returncode == 0, result.stderr
        # These types and functions are private to this test's disposable database.
        result = psql(
            f"CREATE DOMAIN {schema}.email_value AS text; "
            f"CREATE FUNCTION {schema}.attempt_write() RETURNS integer LANGUAGE sql AS "
            f"'UPDATE {schema}.users SET full_name = full_name WHERE id = 1 RETURNING id'; "
            f"CREATE FUNCTION {schema}.change_client_encoding() RETURNS text LANGUAGE plpgsql AS $$ "
            "BEGIN PERFORM set_config('client_encoding', 'LATIN1', false); RETURN 'é'; END; $$; "
            f"CREATE FUNCTION {schema}.change_datestyle() RETURNS date LANGUAGE plpgsql AS $$ "
            "BEGIN PERFORM set_config('DateStyle', 'SQL, DMY', false); RETURN '2025-02-24'::date; END; $$",
            backend=True,
        )
        assert result.returncode == 0, result.stderr
        yield schema
    finally:
        TARGET["database"] = MAINTENANCE_TARGET["database"]
        if created:
            result = psql(f'DROP DATABASE "{database}" WITH (FORCE)', maintenance=True)
            assert result.returncode == 0, result.stderr


def forwarding_checks(schema):
    queries = [
        f"SELECT (SELECT count(*) FROM {schema}.users), (SELECT count(*) FROM {schema}.products), "
        f"(SELECT count(*) FROM {schema}.orders), (SELECT count(*) FROM {schema}.support_tickets)",
        f"SELECT u.id, u.email, count(o.id), sum(p.price * o.qty) FROM {schema}.users u "
        f"JOIN {schema}.orders o ON o.buyer_email = u.email JOIN {schema}.products p ON p.id = o.product_id "
        "GROUP BY u.id, u.email ORDER BY u.id",
        f"WITH contacts AS (SELECT email, full_name FROM {schema}.users) "
        f"SELECT c.full_name, t.requester_email, t.subject FROM contacts c JOIN {schema}.support_tickets t "
        f"ON t.requester_email = c.email JOIN {schema}.orders o ON o.id = t.order_id AND o.buyer_email = c.email ORDER BY t.id",
        f"SELECT email FROM {schema}.users WHERE id < 3 UNION SELECT requester_email FROM {schema}.support_tickets WHERE id > 8 ORDER BY 1",
        f"SELECT id, email FROM {schema}.users WHERE false",
        "SELECT NULL::text, 12345678901234567890.12345678901234567890::numeric, "
        "'2025-02-24 10:15:00.123456+00'::timestamptz, 'infinity'::timestamp, '-infinity'::date, "
        "'{\"email\":\"a@example.test\", \"null\":null}'::json, "
        "ARRAY['one',NULL,'comma,value']::text[], decode('00ff205c', 'hex'), 'NaN'::numeric",
        f"SELECT email::{schema}.email_value AS contact, email AS duplicate, phone AS duplicate FROM {schema}.users WHERE id = 1",
        "SELECT 'DELETE FROM users' AS harmless /* UPDATE is a comment */",
        'SELECT $$DELETE FROM users; UPDATE users SET email = NULL$$ AS "UPDATE"',
        "SELECT i, repeat('row', 8) FROM generate_series(1, 10000) AS series(i)",
        "SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'server_version'",
        "SHOW server_version",
    ]
    for query in queries:
        direct, forwarded = psql(query, backend=True), psql(query)
        assert direct.returncode == forwarded.returncode == 0, (query, direct.stderr, forwarded.stderr)
        assert forwarded.stdout == direct.stdout, (query, direct.stdout, forwarded.stdout)
        # Compare full native RowDescription and text DataRow payloads, including
        # table/attribute identifiers, type OIDs, modifiers, NULLs, and aliases.
        assert wire_query(query) == wire_query(query, backend=True), query
    assert psql(queries[0]).stdout.strip() == "10|5|20|10"
    result = psql("SHOW ALL")
    assert result.returncode == 0 and "server_version|" in result.stdout, result.stderr

    for query in [
        f"DELETE FROM {schema}.users", f"UPDATE {schema}.users SET full_name = 'changed'",
        f"WITH changed AS (DELETE FROM {schema}.orders RETURNING *) SELECT * FROM changed",
        f"SELECT * INTO {schema}.unwanted_table FROM {schema}.users", f"SELECT * FROM {schema}.users FOR UPDATE",
        f"SELECT * FROM (SELECT * FROM {schema}.users FOR SHARE) locked",
        "SELECT 1; SELECT 2", "BEGIN", "SET search_path = public", "COPY t TO STDOUT",
    ]:
        result = psql(query, "SELECT 1")
        assert "0A000" in result.stderr and result.stdout.strip() == "1", (query, result)
    for query, code in [
        (f"SELECT * FROM {schema}.missing_table", "42P01"),
        ("SELECT 1 / 0", "22012"),
        ("SELECT * FROM pg_catalog.pg_authid", "42501"),
        (f"SELECT {schema}.attempt_write()", "25006"),
    ]:
        result = psql(query, "SELECT 1")
        assert code in result.stderr and result.stdout.strip() == "1", (query, result)
    assert psql(queries[0]).stdout.strip() == "10|5|20|10"


def backend_recovery_checks(schema):
    with authenticated_socket() as stream:
        for query, code in [
            ("SELECT " + "1+" * 100000 + "1", "54000"),
            ("SELECT set_config('client_encoding', 'LATIN1', false), 'é'::text", "0A000"),
            ("SELECT set_config('DateStyle', 'SQL, DMY', false), '2025-02-24'::date, "
             "set_config('DateStyle', 'ISO, MDY', false)", "0A000"),
            ("SELECT set_config(lower('client_encoding'), 'LATIN1', false)", "0A000"),
            ("SELECT set_config('statement_timeout', '0', false)", "0A000"),
            (r'''SELECT U&"set\005fconfig"('DateStyle', 'SQL, DMY', false)''', "0A000"),
        ]:
            stream.sendall(packet(b"Q", query.encode() + b"\0"))
            result = responses(stream)
            assert [kind for kind, _ in result] == [b"E", b"Z"], result
            assert ("C" + code + "\0").encode() in result[0][1], result
            # No result bytes from a rejected encoding change may leak, and the
            # same frontend must recover with correct UTF-8 text afterwards.
            stream.sendall(packet(b"Q", "SELECT 'é'::text\0".encode()))
            result = responses(stream)
            assert [kind for kind, _ in result] == [b"T", b"D", b"C", b"Z"], result
            assert result[1][1] == b"\0\1\0\0\0\2\xc3\xa9", result

        # Function bodies execute in PostgreSQL, outside frontend AST inspection.
        for name in ["change_client_encoding", "change_datestyle"]:
            stream.sendall(packet(b"Q", f"SELECT {schema}.{name}()\0".encode()))
            result = responses(stream)
            assert b"C" not in [kind for kind, _ in result] and result[-1] == (b"Z", b"I"), result
            errors = [payload for kind, payload in result if kind == b"E"]
            assert len(errors) == 1 and b"C0A000\0" in errors[0], result
            if name == "change_client_encoding":
                assert b"D" not in [kind for kind, _ in result], "invalid backend encoding reached the frontend"
            else:
                for kind, payload in result:
                    if kind == b"D":
                        payload[6:].decode("utf-8")
            stream.sendall(packet(b"Q", "SELECT 'é'::text\0".encode()))
            result = responses(stream)
            assert [kind for kind, _ in result] == [b"T", b"D", b"C", b"Z"], result
            assert result[1][1] == b"\0\1\0\0\0\2\xc3\xa9", result

    pids = {psql("SELECT pg_backend_pid()").stdout.strip() for _ in range(4)}
    assert len(pids) == 1, "sequential queries did not reuse a pooled connection"
    original = psql("SHOW application_name").stdout
    result = psql("SELECT set_config('application_name', 'yasp_test_leak', false)", "SHOW application_name")
    assert result.returncode == 0 and result.stdout == "yasp_test_leak\n" + original, result
    result = psql("SELECT pg_backend_pid(), pg_advisory_lock(912345678901)")
    assert result.returncode == 0, result.stderr
    pid = int(result.stdout.split("|")[0])
    result = psql(f"SELECT count(*) FROM pg_catalog.pg_locks WHERE pid = {pid} AND locktype = 'advisory'", backend=True)
    assert result.stdout.strip() == "0", "session advisory lock survived pool cleanup"

    for query in ["SELECT pg_terminate_backend(pg_backend_pid())", "SELECT repeat('x', 8388608)"]:
        result = psql(query, "SELECT 1")
        assert result.stderr and result.stdout.strip() == "1", result
        assert len(result.stderr) < 8192, "backend failure exposed a large payload"
        health()
    with authenticated_socket() as stream:
        stream.sendall(packet(b"Q", b"SELECT pg_backend_pid()\0"))
        result = responses(stream)
        assert [kind for kind, _ in result] == [b"T", b"D", b"C", b"Z"], result
        pid = int(result[1][1][6:])
        stream.sendall(packet(b"Q", b"SELECT pg_sleep(4)\0"))
        deadline = time.monotonic() + 2
        while True:
            active = psql(f"SELECT count(*) FROM pg_stat_activity WHERE pid = {pid} "
                          "AND state = 'active' AND query = 'SELECT pg_sleep(4)'", backend=True)
            assert active.returncode == 0, active.stderr
            if active.stdout.strip() == "1":
                break
            assert time.monotonic() < deadline, "query did not start before frontend disconnect"
            time.sleep(0.03)
        # Shut down before libpq's destructor can enqueue a Terminate message.
        stream.shutdown(socket.SHUT_RDWR)
    deadline = time.monotonic() + 3
    while True:
        remaining = psql(f"SELECT count(*) FROM pg_stat_activity WHERE pid = {pid}", backend=True)
        assert remaining.returncode == 0, remaining.stderr
        if remaining.stdout.strip() == "0":
            break
        assert time.monotonic() < deadline, "abandoned query retained its backend connection"
        time.sleep(0.03)
    health()

    result = psql("SELECT pg_sleep(61)", "SELECT 1", timeout=75)
    assert "57014" in result.stderr and result.stdout.strip() == "1", result
    health()


def pool_checks():
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as clients:
        running = [clients.submit(psql, "SELECT pg_sleep(12)") for _ in range(8)]
        deadline = time.monotonic() + 3
        while True:
            active = psql("SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE usename = current_user "
                          "AND datname = current_database() AND state = 'active' AND query = 'SELECT pg_sleep(12)'", backend=True)
            assert active.returncode == 0, active.stderr
            if active.stdout.strip() == "8":
                break
            assert time.monotonic() < deadline, "eight queries did not occupy the pool"
            time.sleep(0.03)
        started = time.monotonic()
        exhausted = psql("SELECT 1", timeout=15)
        assert exhausted.returncode != 0 and 9 <= time.monotonic() - started < 12, exhausted
        for query in running:
            assert query.result().returncode == 0
        reused = list(clients.map(lambda _: psql("SELECT pg_backend_pid(), pg_sleep(0.03)"), range(24)))
        assert all(query.returncode == 0 for query in reused), reused
        pids = {int(query.stdout.split("|")[0]) for query in reused}
        assert len(pids) <= 8, pids
    health()


def wire_checks():
    with authenticated_socket() as stream:
        stream.sendall(packet(b"Q", b"SELECT 1;\0"))
        rows = responses(stream)
        assert [kind for kind, _ in rows] == [b"T", b"D", b"C", b"Z"], rows
        assert rows[0][1] == struct.pack("!H", 1) + b"?column?\0" + struct.pack("!IhIhih", 0, 0, 23, 4, -1, 0)
        assert rows[1][1] == b"\0\1\0\0\0\x011"
        assert rows[2][1] == b"SELECT 1\0" and rows[3][1] == b"I"
        stream.sendall(packet(b"Q", b" \t\0"))
        assert responses(stream) == [(b"I", b""), (b"Z", b"I")]

        # An error discards pipelined Bind/Query messages until Sync. No query
        # result or preparation acknowledgement may appear before recovery.
        stream.sendall(packet(b"P", b"probe\0SELECT 1\0\0\0")
                       + packet(b"B", b"\0probe\0\0\0\0\0\0\0")
                       + packet(b"Q", b"SELECT 1\0") + packet(b"S"))
        result = responses(stream)
        assert [kind for kind, _ in result] == [b"E", b"Z"], result
        assert b"C0A000\0" in result[0][1] and result[1][1] == b"I"
        stream.sendall(packet(b"Q", b"SELECT 1\0"))
        assert [kind for kind, _ in responses(stream)] == [b"T", b"D", b"C", b"Z"]
        stream.sendall(packet(b"X"))
        closed(stream)

    # Length-only attacks must fail without sending or allocating their payload.
    for header in [struct.pack("!I", 8 * 1024 * 1024 + 1), struct.pack("!I", 3)]:
        with socket.create_connection(ADDRESS, timeout=3) as stream:
            stream.sendall(header)
            closed(stream)
        health()
    for header in [b"Q" + struct.pack("!I", 8 * 1024 * 1024), b"Q" + struct.pack("!I", 3)]:
        with authenticated_socket() as stream:
            stream.sendall(header)
            closed(stream)
        health()
    with socket.create_connection(ADDRESS, timeout=3) as stream:
        # Malformed body and fragmented header; one bad client cannot stop serving.
        for byte in struct.pack("!II", 9, 196608) + b"x":
            stream.sendall(bytes([byte]))
        # Fatal startup errors must close without advertising ReadyForQuery.
        assert read_exact(stream, 1) == b"E"
        length = struct.unpack("!I", read_exact(stream, 4))[0]
        assert 4 <= length < 8192
        assert b"SFATAL\0" in read_exact(stream, length - 4)
        closed(stream)
    health()
    with socket.create_connection(ADDRESS, timeout=3) as stream:
        for code in [80877104, 80877103]:
            stream.sendall(struct.pack("!II", 8, code))
            assert stream.recv(1) == b"N"


def session_checks(process):
    held = []
    try:
        for _ in range(32):
            stream = socket.create_connection(ADDRESS, timeout=3)
            held.append(stream)
            stream.sendall(struct.pack("!II", 8, 80877103))
            assert stream.recv(1) == b"N"
        with socket.create_connection(ADDRESS, timeout=3) as excess:
            closed(excess)
        held.pop().close()
        wait_ready(process)
        for stream in held:
            stream.close()
        held.clear()
        wait_ready(process)

        idle = socket.create_connection(ADDRESS, timeout=13)
        held.append(idle)
        idle.sendall(struct.pack("!II", 8, 80877103))
        assert idle.recv(1) == b"N"
        started = time.monotonic()
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=13)
        assert process.returncode == 0 and 9.5 <= time.monotonic() - started < 13
        closed(idle)
        with socket.socket() as probe:
            assert probe.connect_ex(ADDRESS) != 0, "listener remains open after shutdown"
    finally:
        for stream in held:
            stream.close()


def env_file(values):
    lines = []
    for key, value in values.items():
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$").replace("\n", "\\n").replace("\r", "\\r")
        lines.append(f'{key}="{escaped}"\n')
    return "".join(lines)


def startup_checks(directory):
    path = Path(directory) / "config/local.yml"
    path.parent.mkdir()
    env_path = Path(directory) / ".env"
    config_text = json.dumps(CONFIG)
    def variant(change):
        config = json.loads(config_text)
        change(config)
        return json.dumps(config)

    target_text = json.dumps(TARGET)
    target_key, login_key = json.dumps(TARGET_NAME), json.dumps(LOGIN_NAME)
    login_text = json.dumps(LOGIN)
    duplicate_targets = '{"version": 1, "targets": {' + target_key + ': ' + target_text + ', ' + target_key + ': ' + target_text + '}}'
    duplicate_login = target_text.replace(json.dumps(TARGET["logins"]), '{' + login_key + ': ' + login_text + ', ' + login_key + ': ' + login_text + '}')
    duplicate_logins = '{"version": 1, "targets": {' + target_key + ': ' + duplicate_login + '}}'
    secret_values = {**FRONTEND_ENV, **{key: "SECRET_MARKER_" + str(index) for index, key in enumerate(PASSWORDS)}}
    secret_file = env_file(secret_values)
    for content, secrets in [
        (None, secret_file),
        ('targets: {primary: {host: "SECRET_MARKER\n', secret_file),
        (" " * 65537, secret_file),
        (variant(lambda config: config.pop("version")), secret_file),
        (variant(lambda config: config.update(version=2)), secret_file),
        (variant(lambda config: config.update(targets={})), secret_file),
        (variant(lambda config: config["targets"][TARGET_NAME].pop("port")), secret_file),
        (variant(lambda config: config["targets"][TARGET_NAME].update(port="SECRET_MARKER")), secret_file),
        (variant(lambda config: config["targets"][TARGET_NAME].update(password="SECRET_MARKER")), secret_file),
        (variant(lambda config: config["targets"][TARGET_NAME].update(engine="SECRET_MARKER")), secret_file),
        (variant(lambda config: config["targets"][TARGET_NAME].update(tls_mode="require")), secret_file),
        (variant(lambda config: config["targets"][TARGET_NAME].update(logins={})), secret_file),
        (variant(lambda config: config["targets"][TARGET_NAME]["logins"][LOGIN_NAME].pop("password_env")), secret_file),
        (variant(lambda config: config["targets"].update({TARGET_NAME + "_extra": TARGET})), secret_file),
        (variant(lambda config: config["targets"][TARGET_NAME]["logins"].update({LOGIN_NAME + "_extra": LOGIN})), secret_file),
        (variant(lambda config: config.update(source={"password": "SECRET_MARKER"})), secret_file),
        (config_text[:-1] + ', "version": 1}', secret_file),
        (duplicate_targets, secret_file),
        (duplicate_logins, secret_file),
        (config_text, env_file(FRONTEND_ENV)),
        (config_text, env_file({key: value for key, value in secret_values.items() if key != LOGIN["password_env"]})),
        (config_text, env_file({key: value for key, value in secret_values.items() if key != "YASP_GATEWAY_USERNAME"})),
        (config_text, env_file({key: value for key, value in secret_values.items() if key != "YASP_GATEWAY_DATABASE"})),
        (config_text, 'SECRET_MARKER="unterminated\n'),
    ]:
        if content is None:
            path.unlink(missing_ok=True)
        else:
            path.write_text(content)
        env_path.write_text(secrets)
        result = subprocess.run([BINARY], cwd=directory, env=ENV, capture_output=True, timeout=3)
        assert result.returncode != 0, "invalid startup settings were accepted"
        assert b"SECRET_MARKER" not in result.stdout + result.stderr, "invalid input was exposed in diagnostics"
    env_path.unlink()


def custom_config_checks(directory):
    config_path = Path(directory) / "config/local.yml"
    custom = json.loads(json.dumps(CONFIG))
    custom_target = custom["targets"].pop(TARGET_NAME)
    custom["targets"][TARGET_NAME + "_custom"] = custom_target
    env_values = {**FRONTEND_ENV, **PASSWORDS}
    custom_login = custom_target["logins"][LOGIN_NAME]
    if custom_login["password_env"] == "YASP_GATEWAY_PASSWORD":
        independent_key = "YASP_TEST_TARGET_" + uuid.uuid4().hex + "_PASSWORD"
        env_values[independent_key] = PASSWORDS[LOGIN["password_env"]]
        custom_login["password_env"] = independent_key
    (Path(directory) / ".env").write_text(env_file(env_values))
    custom_path = Path(directory) / "custom-targets.yml"
    custom_path.write_text(json.dumps(custom))
    config_path.unlink()
    # The explicit YAML path selects the target; frontend identity stays in env.
    overrides = {"YASP_GATEWAY_USERNAME": GATEWAY["username"] + "_override",
                 "YASP_GATEWAY_DATABASE": GATEWAY["database"] + "_override",
                 "YASP_GATEWAY_PASSWORD": "override_secret"}
    with server(directory, config_file=custom_path, **overrides) as process:
        wait_ready(process, user=overrides["YASP_GATEWAY_USERNAME"], password="override_secret", database=overrides["YASP_GATEWAY_DATABASE"])
        assert psql("SELECT 1").returncode != 0
        process.send_signal(signal.SIGINT)
        process.wait(timeout=13)
        assert process.returncode == 0


def main():
    with socket.socket() as probe:
        assert probe.connect_ex(ADDRESS) != 0, "port 6432 is occupied; stop the existing gateway before testing"
    with tempfile.TemporaryDirectory(prefix="yasp-gateway-") as directory:
        startup_checks(directory)
        assert all(PASSWORDS.values()), "supply the password environment variables named by tests/config.yml"
        with fixture() as schema:
            config_path = Path(directory) / "config/local.yml"
            config_path.write_text(json.dumps(CONFIG))
            # No .env is present: exported password variables must be sufficient.
            with server(directory, **(FRONTEND_ENV | PASSWORDS)) as process:
                wait_ready(process)
                health(ssl="prefer")
                for options in [{"password": "wrong"}, {"password": None}, {"user": "unknown"},
                                {"database": "unknown"}, {"ssl": "require"}]:
                    assert psql("SELECT 1", **options).returncode != 0, options
                assert psql("SELECT 2").stdout.strip() == "2"
                assert psql("  sElEcT\t1 ;  ").stdout.strip() == "1"
                forwarding_checks(schema)
                wire_checks()
                backend_recovery_checks(schema)
                pool_checks()
                session_checks(process)

            custom_config_checks(directory)
        with socket.socket() as reusable:
            reusable.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            reusable.bind(ADDRESS)
    print("Gateway checks passed in a disposable database: read/SHOW forwarding, native metadata and values, read-only enforcement, pool cleanup/bounds, timeout/failure recovery, authentication, frontend bounds, shutdown, and YAML/secret validation.")


if __name__ == "__main__":
    main()
