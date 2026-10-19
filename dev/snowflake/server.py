"""Pinned local Snowflake HTTP server with persistent, shared storage."""
import argparse
from pathlib import Path
import signal
import threading
from fakesnow_cancel import server
from sql import seed
import snowflake.connector


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--test', action='store_true', help='enable owned test counters and chunk fixtures')
    args = parser.parse_args()
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    Path('/data').mkdir(exist_ok=True)
    with server(host='0.0.0.0', port=64616, db_path='/data', chunk_mode='dynamic' if args.test else None) as (settings, _):
        settings.update(host='127.0.0.1', database='YASP_DEMO', schema='PUBLIC')
        with snowflake.connector.connect(**settings) as connection:
            seed(connection)
            if args.test:
                for sql in ['CREATE DATABASE YASP_RUNTIME', 'CREATE SCHEMA YASP_RUNTIME.PUBLIC',
                            'CREATE SCHEMA YASP_RUNTIME."MixedSchema"',
                            'CREATE TABLE YASP_RUNTIME.PUBLIC.METADATA_TYPES (AMOUNT NUMBER(38,9) NOT NULL, "MixedCase" VARCHAR(30), CREATED TIMESTAMP_NTZ(6))']:
                    connection.cursor().execute(sql)
        stopped.wait()


if __name__ == '__main__':
    main()
