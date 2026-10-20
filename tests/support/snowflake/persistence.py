"""Persistent fixture acceptance through Snowflake HTTP, across container recreation."""
import json
import os
import subprocess
import time
import uuid


def docker(*args, **kwargs):
    result = subprocess.run(['docker', *args], text=True, capture_output=True, timeout=30, **kwargs)
    if result.returncode:
        raise RuntimeError('docker ' + ' '.join(args[:2]) + ': ' + result.stderr)
    return result.stdout


def main():
    name = 'yasp-persistence-' + uuid.uuid4().hex
    volume = name + '-data'
    docker('volume', 'create', '--label', 'yasp.compatibility=' + os.environ['YASP_JDBC_RUN'], volume)
    try:
        def start():
            docker('run', '-d', '--name', name, '--label', 'yasp.compatibility=' + os.environ['YASP_JDBC_RUN'],
                   '-v', volume + ':/data', os.environ['YASP_SNOWFLAKE_IMAGE'])
            deadline = time.monotonic() + 45
            while True:
                ready = subprocess.run(['docker', 'exec', name, 'python', 'sql.py', '--health'], capture_output=True, timeout=10)
                if ready.returncode == 0:
                    break
                assert time.monotonic() < deadline, docker('logs', name)
                time.sleep(0.1)

        def query(sql):
            code = 'import json; from sql import query; print(json.dumps(query(' + repr(sql) + ')))'
            return json.loads(docker('exec', name, 'python', '-c', code))

        def definitions():
            queries = [
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA IN ('PUBLIC','ANALYTICS') ORDER BY 1,2,3",
                'SHOW PRIMARY KEYS IN SCHEMA "YASP_DEMO"."ANALYTICS"',
                'SHOW IMPORTED KEYS IN SCHEMA "YASP_DEMO"."ANALYTICS"',
                "SELECT TABLE_NAME, COMMENT, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ('PUBLIC','ANALYTICS') ORDER BY 1",
                "SELECT TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='ANALYTICS' ORDER BY 1",
            ]
            results = [sorted(query(sql), key=str) for sql in queries]
            # DuckDB canonicalizes quoted builtin cast types when checkpointing.
            # Compare this seed view's definition after that harmless formatting.
            for row in results[4]:
                row[1] = row[1].replace('AS "DATE"', 'AS DATE').replace('AS "DOUBLE"', 'AS DOUBLE')
            return results

        start()
        before = definitions()
        assert any(r[1:3] == ['USERS', 'ID'] and r[6] == 'Customer identifier' for r in before[0]), before[0]
        assert any(r[1:3] == ['USERS', 'FULL_NAME'] and r[3] is None for r in before[0]), before[0]
        assert sorted((row[4], row[5]) for row in before[1]) == [('ID', '2'), ('TENANT_ID', '1')]
        assert sorted((row[4], row[8], row[9]) for row in before[2] if row[7] == 'TYPE_REFERENCES') == [('ID', 'TYPE_ID', '2'), ('TENANT_ID', 'TENANT_ID', '1')]
        assert any(r == ['USERS', 'Demo customers', 'BASE TABLE'] for r in before[3])
        assert any(r == ['DAILY_SALES', None, 'VIEW'] for r in before[3])
        assert len(before[4]) == 1 and 'ORDERS' in before[4][0][1]
        query("INSERT INTO PUBLIC.USERS (ID,FULL_NAME) VALUES (99,'Persisted user')")
        docker('stop', '--time', '10', name)
        docker('rm', name)
        start()
        assert query("SELECT FULL_NAME FROM PUBLIC.USERS WHERE ID=99") == [['Persisted user']]
        after = definitions()
        for index, (expected, actual) in enumerate(zip(before, after)):
            assert actual == expected, f'metadata query {index} changed across restart: {expected!r} -> {actual!r}' 
    finally:
        subprocess.run(['docker', 'rm', '-f', '-v', name], capture_output=True, timeout=30)
        docker('volume', 'rm', volume)


if __name__ == '__main__':
    main()
