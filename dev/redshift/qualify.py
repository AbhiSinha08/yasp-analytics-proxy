"""Seed and qualify the unmodified Oblako server through Amazon's connector."""

import argparse
import decimal
import json
import os
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, "/yasp/driver")
import redshift_connector


def connect(database=None, protocol=0, user=None, password=None):
    connection = redshift_connector.connect(
        host=os.environ.get("YASP_REDSHIFT_HOST", "localhost"),
        port=int(os.environ.get("YASP_REDSHIFT_PORT", "5439")),
        user=user or os.environ.get("YASP_REDSHIFT_USER", os.environ.get("POSTGRES_USER", "yasp_admin")),
        password=password or os.environ.get("YASP_REDSHIFT_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "yasp_local_password")),
        database=database or os.environ.get("YASP_REDSHIFT_DATABASE", os.environ.get("POSTGRES_DB", "yasp_demo")),
        ssl=True,
        sslmode="verify-full",
        client_protocol_version=protocol,
        timeout=10,
    )
    connection.autocommit = True
    return connection


def identifier(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,100}", value):
        raise ValueError("database name must be a lowercase SQL identifier")
    return '"' + value + '"'


def seed(database=None):
    with connect(database) as connection:
        with connection.cursor() as cursor:
            # This application fixture has no semicolons within string literals.
            for statement in Path(__file__).with_name("seed.sql").read_text().split(";\n"):
                if statement.strip():
                    cursor.execute(statement.rstrip(";\n"))
            for name, password in [
                ("yasp_reader", os.environ.get("YASP_REDSHIFT_READER_PASSWORD", "yasp_reader_password")),
                ("yasp_denied", os.environ.get("YASP_REDSHIFT_DENIED_PASSWORD", "yasp_denied_password")),
            ]:
                cursor.execute("SELECT 1 FROM pg_user WHERE usename = %s", (name,))
                if cursor.fetchone() is None:
                    cursor.execute("CREATE USER " + identifier(name) + " PASSWORD '" + password.replace("'", "''") + "'")
            cursor.execute("GRANT USAGE ON SCHEMA public, analytics TO yasp_reader")
            cursor.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public, analytics TO yasp_reader")
            # Real Redshift exposes these discovery views to ordinary users.
            cursor.execute("GRANT SELECT ON pg_catalog.svv_tables, pg_catalog.svv_columns TO PUBLIC")


def initialize_database(database):
    """Run Oblako's bundled database initialization, without changing its code."""
    admin = os.environ.get("POSTGRES_USER", "yasp_admin")
    # template1 has the catalog/date views, but not the bundled SUPER objects.
    # Install those objects into this new database as the upstream init does.
    for script in ["08_super.sql", "99_system_catalog.sql"]:
        subprocess.run([
            "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", admin,
            "-p", os.environ.get("OBLAKO_PG_PORT", "5433"), "-d", database,
            "-f", "/docker-entrypoint-initdb.d/" + script,
        ], check=True, stdout=subprocess.DEVNULL)


def create_database(database):
    quoted = identifier(database)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE " + quoted)
    try:
        initialize_database(database)
        seed(database)
    except BaseException:
        drop_database(database)
        raise


def drop_database(database):
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DROP DATABASE " + identifier(database))


CASES = [
    ("scalar_decimal", "SELECT amount FROM analytics.orders WHERE id = 10", [[decimal.Decimal("123.456789012")]]),
    ("metadata", "SELECT column_name, data_type FROM svv_columns WHERE table_schema = 'public' AND table_name = 'users' ORDER BY ordinal_position", [["id", "integer"], ["full_name", "character varying"], ["email", "character varying"], ["created_at", "timestamp without time zone"]]),
    ("dateadd", "SELECT DATEADD(day, 1, '2026-01-02'::timestamp)::date::varchar", [["2026-01-03"]]),
    ("join_aggregate", "SELECT u.full_name, count(o.id) FROM public.users u JOIN analytics.orders o ON o.user_id = u.id GROUP BY u.full_name ORDER BY u.full_name", [["Alice", 1], ["Bob", 1]]),
    ("super_scalar", "SELECT JSON_SERIALIZE(JSON_PARSE('12345678901234567890.123456789'))", [["12345678901234567890.123456789"]]),
    ("super_json_typeof", "SELECT JSON_TYPEOF(JSON_PARSE('[1,2]'))", [["array"]]),
    ("super_navigation", "SELECT payload.customer.name::varchar FROM analytics.events WHERE id = 1", [["Alice"]]),
    ("super_array_index", "SELECT payload.items[0].sku::varchar FROM analytics.events WHERE id = 1", [["a"]]),
    ("super_missing", "SELECT payload.absent FROM analytics.events WHERE id = 1", [[None]]),
    ("super_json_null", "SELECT JSON_SERIALIZE(NULL::SUPER)", [["null"]]),
    ("partiql_array_iteration", "SELECT item.sku::varchar FROM analytics.events e, e.payload.items item ORDER BY item.sku::varchar", [["a"], ["b"]]),
    ("partiql_array_at", "SELECT idx, item.sku::varchar FROM analytics.events e, e.payload.items AS item AT idx ORDER BY idx", [[0, "a"], [1, "b"]]),
    ("partiql_object_iteration", "SELECT attr, val::varchar FROM analytics.events e, UNPIVOT e.payload.customer AS val AT attr ORDER BY attr", [["name", "Alice"]]),
]


def qualify(database=None):
    observations = []
    with connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
        for name, sql, expected in CASES:
            observation = {"case": name, "sql": sql, "expected": expected}
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    actual = [list(row) for row in cursor.fetchall()]
                    observation |= {"actual": actual, "type_oids": [field[1] for field in cursor.description], "passed": actual == expected}
            except Exception as error:
                observation |= {"passed": False, "error": str(error)}
            observations.append(observation)
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT payload FROM analytics.events WHERE id = 1")
                actual = cursor.fetchone()[0]
                oid = cursor.description[0][1]
                observations.append({"case": "native_super_oid", "expected_oid": 4000, "actual_oid": oid, "actual": actual, "passed": oid == 4000})
        except Exception as error:
            observations.append({"case": "native_super_oid", "passed": False, "error": str(error)})
    try:
        with connect(database, protocol=None) as negotiated:
            with negotiated.cursor() as cursor:
                cursor.execute("SELECT 1")
                actual = cursor.fetchall()
        observations.append({"case": "default_protocol", "passed": [list(row) for row in actual] == [[1]]})
    except Exception as error:
        observations.append({"case": "default_protocol", "passed": False, "error": str(error)})
    for name, user, password, allowed in [
        ("reader_grants", "yasp_reader", os.environ.get("YASP_REDSHIFT_READER_PASSWORD", "yasp_reader_password"), True),
        ("denied_grants", "yasp_denied", os.environ.get("YASP_REDSHIFT_DENIED_PASSWORD", "yasp_denied_password"), False),
    ]:
        try:
            with connect(database, user=user, password=password) as restricted:
                with restricted.cursor() as cursor:
                    cursor.execute("SELECT count(*) FROM public.users")
                    actual = cursor.fetchone()[0]
            observations.append({"case": name, "passed": allowed and actual == 2})
        except Exception as error:
            observations.append({"case": name, "passed": not allowed and "42501" in str(error), "error": str(error)})
    try:
        with connect(database) as read_only:
            with read_only.cursor() as cursor:
                cursor.execute("BEGIN READ ONLY")
                try:
                    cursor.execute("UPDATE public.users SET full_name = full_name WHERE id = 1")
                    observations.append({"case": "read_only_transaction", "passed": False})
                except Exception as error:
                    observations.append({"case": "read_only_transaction", "passed": "25006" in str(error)})
                finally:
                    cursor.execute("ROLLBACK")
    except Exception as error:
        observations.append({"case": "read_only_transaction", "passed": False, "error": str(error)})
    baseline = {"scalar_decimal", "metadata", "dateadd", "join_aggregate", "super_scalar",
                "super_json_typeof", "default_protocol", "reader_grants", "denied_grants", "read_only_transaction"}
    passed = all(item["passed"] for item in observations)
    baseline_passed = all(item["passed"] for item in observations if item["case"] in baseline)
    return {"connector": redshift_connector.__version__, "server": version, "passed": passed,
            "baseline_passed": baseline_passed,
            "baseline_cases": sorted(baseline),
            "validation_gaps": [item["case"] for item in observations if not item["passed"]],
            "observations": observations}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["seed", "check", "create-database", "drop-database"])
    parser.add_argument("database", nargs="?")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--baseline", action="store_true", help="gate the supported baseline while still reporting every native conformance gap")
    arguments = parser.parse_args()
    if arguments.operation in {"create-database", "drop-database"} and not arguments.database:
        parser.error("database name is required for create-database and drop-database")
    if arguments.operation == "seed":
        seed(arguments.database)
        return 0
    if arguments.operation == "create-database":
        create_database(arguments.database)
        return 0
    if arguments.operation == "drop-database":
        drop_database(arguments.database)
        return 0
    report = qualify(arguments.database)
    output = json.dumps(report, indent=2, default=str)
    if arguments.report:
        arguments.report.parent.mkdir(parents=True, exist_ok=True)
        arguments.report.write_text(output + "\n")
    print(output)
    return 0 if report["baseline_passed" if arguments.baseline else "passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
