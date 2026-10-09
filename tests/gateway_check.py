"""Run the host with real psql/libpq and exercise its observable wire contract."""

import concurrent.futures
import contextlib
import ctypes
import ctypes.util
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

BINARY = str(Path(sys.argv[1]).resolve())
ADDRESS = ("127.0.0.1", 6432)
ENV_FILE = "YASP_GATEWAY_USERNAME=metabase\nYASP_GATEWAY_PASSWORD=yasp_dev_only\nYASP_GATEWAY_DATABASE=yasp\n"
ENV = {k: v for k, v in os.environ.items() if not k.startswith(("PG", "YASP_GATEWAY_"))}
ENV["RUST_LOG"] = "info"
LIBPQ = ctypes.CDLL(ctypes.util.find_library("pq"))
for name, args, result in [
    ("PQconnectdb", [ctypes.c_char_p], ctypes.c_void_p),
    ("PQstatus", [ctypes.c_void_p], ctypes.c_int),
    ("PQsocket", [ctypes.c_void_p], ctypes.c_int),
    ("PQfinish", [ctypes.c_void_p], None),
]:
    function = getattr(LIBPQ, name)
    function.argtypes, function.restype = args, result


def psql(*queries, password="yasp_dev_only", user="metabase", database="yasp", ssl="disable"):
    environment = ENV | {"PGPASSFILE": "/dev/null", "PSQL_HISTORY": "/dev/null"}
    if password is not None:
        environment["PGPASSWORD"] = password
    command = ["psql", "-X", "-w", "-At", "-v", "VERBOSITY=verbose",
               f"host=127.0.0.1 port=6432 user={user} dbname={database} sslmode={ssl} connect_timeout=2"]
    for query in queries:
        command.extend(["-c", query])
    return subprocess.run(command, env=environment, text=True, capture_output=True, timeout=5)


def health(**kwargs):
    result = psql("SELECT 1;", **kwargs)
    assert result.returncode == 0 and result.stdout.strip() == "1", result.stderr


def wait_ready(process, **kwargs):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        assert process.poll() is None, "gateway exited before becoming ready"
        result = psql("SELECT 1", **kwargs)
        if result.returncode == 0:
            assert result.stdout.strip() == "1"
            return
        time.sleep(0.03)
    raise AssertionError("gateway never became ready")


@contextlib.contextmanager
def server(directory, **overrides):
    with open(Path(directory) / "server.log", "w+") as log:
        process = subprocess.Popen([BINARY], cwd=directory, env=ENV | overrides, stdout=log, stderr=log)
        try:
            yield process
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=7)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise AssertionError("gateway failed to shut down")
            log.seek(0)
            output = log.read()
            assert "yasp_dev_only" not in output and "SECRET_MARKER" not in output, output
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
def authenticated_socket():
    # Let the installed PostgreSQL client library perform SCRAM. Borrow its
    # authenticated socket only to send deliberately unusual protocol sequences.
    connection = LIBPQ.PQconnectdb(
        b"host=127.0.0.1 port=6432 user=metabase dbname=yasp password=yasp_dev_only sslmode=disable connect_timeout=2"
    )
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

        idle = socket.create_connection(ADDRESS, timeout=8)
        held.append(idle)
        idle.sendall(struct.pack("!II", 8, 80877103))
        assert idle.recv(1) == b"N"
        started = time.monotonic()
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=7)
        assert process.returncode == 0 and 4.5 <= time.monotonic() - started < 7
        closed(idle)
        with socket.socket() as probe:
            assert probe.connect_ex(ADDRESS) != 0, "listener remains open after shutdown"
    finally:
        for stream in held:
            stream.close()


def main():
    with socket.socket() as probe:
        assert probe.connect_ex(ADDRESS) != 0, "port 6432 is occupied; stop the existing gateway before testing"
    with tempfile.TemporaryDirectory(prefix="yasp-gateway-") as directory:
        path = Path(directory) / ".env"
        for content in [None, 'SECRET_MARKER="unterminated\n', ENV_FILE.replace("yasp_dev_only", ""),
                        "YASP_GATEWAY_USERNAME=metabase\n"]:
            if content is None:
                path.unlink(missing_ok=True)
            else:
                path.write_text(content)
            result = subprocess.run([BINARY], cwd=directory, env=ENV, capture_output=True, timeout=3)
            assert result.returncode != 0
            assert b"SECRET_MARKER" not in result.stderr and b"yasp_dev_only" not in result.stderr

        path.write_text(ENV_FILE)
        with server(directory) as process:
            wait_ready(process)
            health(ssl="prefer")
            for options in [{"password": "wrong"}, {"password": None}, {"user": "unknown"},
                            {"database": "unknown"}, {"ssl": "require"}]:
                assert psql("SELECT 1", **options).returncode != 0, options
            for query in ["SELECT 2", "SELECT 1; SELECT 1", "SELECT 1;;", "BEGIN", "COPY t TO STDOUT"]:
                result = psql(query, "SELECT 1")
                assert "0A000" in result.stderr and result.stdout.strip() == "1", result
            assert psql("  sElEcT\t1 ;  ").stdout.strip() == "1"
            wire_checks()
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as clients:
                list(clients.map(lambda _: health(), range(16)))
            session_checks(process)

        with server(directory, YASP_GATEWAY_USERNAME="override", YASP_GATEWAY_PASSWORD="override_secret",
                    YASP_GATEWAY_DATABASE="override_db") as process:
            wait_ready(process, user="override", password="override_secret", database="override_db")
            assert psql("SELECT 1").returncode != 0
            process.send_signal(signal.SIGINT)
            process.wait(timeout=7)
            assert process.returncode == 0
        with socket.socket() as reusable:
            reusable.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            reusable.bind(ADDRESS)
    print("Gateway checks passed: psql authentication, query/recovery, wire metadata, frame/session bounds, shutdown, and .env validation.")


if __name__ == "__main__":
    main()
