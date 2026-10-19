"""Execute Snowflake SQL over HTTP; use stdin or --file, never open DuckDB files."""
import argparse
import os
from pathlib import Path
import sys
import snowflake.connector


def connect():
    return snowflake.connector.connect(user='fake', password='snow', account='fakesnow',
        host='127.0.0.1', port=64616, protocol='http', database='YASP_DEMO', schema='PUBLIC',
        session_parameters={'CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED': False}, login_timeout=3, network_timeout=3)


def seed(connection):
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS YASP_DEMO.PUBLIC._YASP_SEED (VERSION INTEGER PRIMARY KEY)')
    cursor.execute('SELECT VERSION FROM YASP_DEMO.PUBLIC._YASP_SEED')
    versions = cursor.fetchall()
    if versions:
        if versions != [(1,)]:
            raise RuntimeError('unsupported seed version; reset the developer volume explicitly')
        return
    for _ in connection.execute_stream((Path(__file__).parent / 'seed.sql').open()):
        pass
    cursor.execute('INSERT INTO YASP_DEMO.PUBLIC._YASP_SEED VALUES (1)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--file', type=Path)
    parser.add_argument('--health', action='store_true')
    args = parser.parse_args()
    with connect() as connection:
        if args.health:
            assert connection.cursor().execute('SELECT VERSION FROM _YASP_SEED').fetchall() == [(1,)]
            assert connection.cursor().execute('SELECT COUNT(*) FROM USERS').fetchone()[0] >= 3
        else:
            source = args.file.open() if args.file else sys.stdin
            for cursor in connection.execute_stream(source):
                if cursor.description:
                    print([column.name for column in cursor.description])
                    for row in cursor:
                        print(row)
