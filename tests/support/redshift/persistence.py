"""Verify persisted Oblako data, definitions, and SUPER navigation without reseeding."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import uuid


def docker(*arguments, **options):
    result = subprocess.run(["docker", *arguments], capture_output=True, text=True, timeout=40, **options)
    if result.returncode:
        raise RuntimeError("docker " + " ".join(arguments[:2]) + ": " + result.stderr)
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true", help="gate durable data/metadata while still reporting native navigation")
    arguments = parser.parse_args()
    name = "yasp-redshift-persistence-" + uuid.uuid4().hex
    volume = name + "-data"
    label = "yasp.compatibility=" + os.environ["YASP_JDBC_RUN"]
    tls = Path(os.environ["YASP_TEST_TLS_DIR"]).resolve()
    docker("volume", "create", "--label", label, volume)
    observations = []
    try:
        def query(sql):
            code = "import sys,json;sys.path.insert(0,'/yasp');from qualify import connect;c=connect();q=c.cursor();q.execute(sys.stdin.read());print(json.dumps(q.fetchall(),default=str));c.close()"
            return json.loads(docker("exec", "-i", name, "python3", "-c", code, input=sql))

        def start():
            docker("run", "-d", "--name", name, "--label", label,
                   "-v", volume + ":/var/lib/postgresql/data",
                   "-v", str(tls) + ":/yasp/tls:ro",
                   "-e", "POSTGRES_USER=yasp_admin", "-e", "POSTGRES_PASSWORD=yasp_local_password",
                   "-e", "POSTGRES_DB=yasp_demo", "-e", "POSTGRES_HOST_AUTH_METHOD=md5",
                   "-e", "OBLAKO_SSL_CERT=/yasp/tls/server.crt", "-e", "OBLAKO_SSL_KEY=/yasp/tls/server.key",
                   "-e", "SSL_CERT_FILE=/yasp/tls/ca.crt", os.environ["YASP_REDSHIFT_IMAGE"])
            deadline = time.monotonic() + 90
            while True:
                try:
                    if query("SELECT 1") == [[1]]:
                        return
                except RuntimeError:
                    pass
                if time.monotonic() > deadline:
                    raise RuntimeError("persistent Redshift fixture did not start: " + docker("logs", name))
                time.sleep(0.2)

        start()
        docker("exec", name, "python3", "/yasp/qualify.py", "seed")
        queries = {
            "data": "SELECT id, full_name, email FROM public.users ORDER BY id",
            "metadata": "SELECT table_schema,table_name,column_name,data_type FROM svv_columns WHERE table_schema IN ('public','analytics') ORDER BY 1,2,3",
            "super_data": "SELECT payload FROM analytics.events ORDER BY id",
            "super_navigation": "SELECT payload.customer.name::varchar FROM analytics.events WHERE id=1",
        }
        before = {key: query(sql) for key, sql in queries.items()}
        docker("stop", "--time", "10", name)
        docker("rm", "-v", name)
        start()
        for key, sql in queries.items():
            observation = {"case": key, "expected": before[key]}
            try:
                actual = query(sql)
                observation |= {"actual": actual, "passed": actual == before[key]}
            except RuntimeError as error:
                observation |= {"passed": False, "error": str(error)}
            observations.append(observation)
        baseline_passed = all(item["passed"] for item in observations if item["case"] != "super_navigation")
        report = {"passed": all(item["passed"] for item in observations), "baseline_passed": baseline_passed,
                  "baseline_cases": ["data", "metadata", "super_data"], "observations": observations}
        destination = Path(__file__).resolve().parents[3] / "target/compatibility/redshift-persistence.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
        return 0 if report["baseline_passed" if arguments.baseline else "passed"] else 1
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True, timeout=30)
        docker("volume", "rm", volume)


if __name__ == "__main__":
    raise SystemExit(main())
