"""Captured SQLAlchemy 1.4 reflection used by psycopg2 BI clients."""
import json
import os
import time
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.pool import NullPool

profile = os.environ['YASP_JDBC_PROFILE']
url = URL.create(('redshift' if profile == 'redshift' else 'postgresql') + '+psycopg2',
    username=os.environ['YASP_JDBC_USER'], password=os.environ['YASP_JDBC_PASSWORD'],
    host=os.environ['YASP_JDBC_HOST'], port=int(os.environ['YASP_JDBC_PORT']), database=os.environ['YASP_JDBC_DATABASE'])
engine = create_engine(url, poolclass=NullPool, isolation_level='AUTOCOMMIT', connect_args={'sslmode':'verify-ca','sslrootcert':'/tmp/yasp-fixture-ca.crt','connect_timeout':5})
queries = []
@event.listens_for(engine, 'before_cursor_execute')
def capture(connection, cursor, statement, parameters, context, executemany):
    queries.append(cursor.mogrify(statement, parameters).decode())

observations = []
try:
    deadline = time.monotonic() + 10
    while True:
        try:
            inspector = inspect(engine)
            break
        except OperationalError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.2)
    # Superset requests this before executing chart/SQL Lab queries.
    with engine.connect() as connection:
        dbapi = connection.connection.connection
        pid = connection.exec_driver_sql('SELECT pg_backend_pid()').scalar()
        qualified = connection.exec_driver_sql('SELECT pg_catalog.pg_backend_pid()').scalar()
        observations.append({'operation':'session_pid', 'passed':type(pid) is int and pid == qualified == dbapi.get_backend_pid(), 'result':pid})
        connection.exec_driver_sql('SELECT ID FROM PUBLIC.USERS LIMIT 1').fetchall()
    for name, operation in [
        ('schemas', lambda: inspector.get_schema_names()),
        ('tables', lambda: inspector.get_table_names(schema='PUBLIC')),
        ('views', lambda: inspector.get_view_names(schema='ANALYTICS')),
        ('columns', lambda: inspector.get_columns('USERS',schema='PUBLIC')),
        ('primary_keys', lambda: inspector.get_pk_constraint('USERS',schema='PUBLIC')),
        ('foreign_keys', lambda: inspector.get_foreign_keys('ORDERS',schema='PUBLIC')),
        ('composite_keys', lambda: inspector.get_foreign_keys('TYPE_REFERENCES',schema='ANALYTICS')),
        ('indexes', lambda: inspector.get_indexes('USERS',schema='PUBLIC')),
        ('uniques', lambda: inspector.get_unique_constraints('USERS',schema='PUBLIC')),
        ('comments', lambda: inspector.get_table_comment('USERS',schema='PUBLIC')),
    ]:
        try:
            observations.append({'operation':name, 'passed':True, 'result':operation()})
        except Exception as error:
            observations.append({'operation':name, 'passed':False, 'error':str(error)})
except Exception as error:
    observations.append({'operation':'connect','passed':False,'error':str(error)})
finally:
    engine.dispose()
print('RESULT ' + json.dumps({'observations':observations, 'queries':queries},default=str))
