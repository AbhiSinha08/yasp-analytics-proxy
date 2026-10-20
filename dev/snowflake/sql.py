"""Load SQL through the public SQL API. No DuckDB file access or Python packages."""
import argparse
import json
import os
from pathlib import Path
import sys
import urllib.request

ENDPOINT = os.environ.get('YASP_SNOWFLAKE_ENDPOINT', '127.0.0.1:64616')

def query(sql, schema='PUBLIC'):
    request = urllib.request.Request('http://' + ENDPOINT + '/api/v2/statements',
        json.dumps({'statement':sql, 'database':'YASP_DEMO', 'schema':schema}).encode(),
        {'Content-Type':'application/json'})
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.load(response)
    if result.get('code') != '090001':
        raise RuntimeError(result.get('message', repr(result)))
    return result.get('data', [])


def statements(source):
    # Split outside SQL strings and identifiers; preserve semicolons in values.
    buffer, quote = [], None
    index = 0
    while index < len(source):
        char = source[index]
        buffer.append(char)
        if quote:
            if char == quote:
                if index+1 < len(source) and source[index+1] == quote:
                    buffer.append(source[index+1]); index += 1
                else:
                    quote = None
        elif char in "'\"":
            quote = char
        elif char == ';':
            yield ''.join(buffer[:-1]).strip()
            buffer = []
        index += 1
    if quote:
        raise ValueError('unterminated SQL quote')
    if ''.join(buffer).strip():
        yield ''.join(buffer).strip()


def seed():
    # Bootstrap without a schema context until PUBLIC exists.
    query('CREATE SCHEMA IF NOT EXISTS PUBLIC', schema='')
    query('CREATE TABLE IF NOT EXISTS PUBLIC._YASP_SEED (VERSION INTEGER PRIMARY KEY)')
    versions = query('SELECT VERSION FROM PUBLIC._YASP_SEED')
    if versions:
        if versions != [['2']]:
            raise RuntimeError('unsupported seed version; reset this emulator volume explicitly')
        return
    for sql in statements(Path(__file__).with_name('seed.sql').read_text()):
        query(sql)
    query('INSERT INTO PUBLIC._YASP_SEED VALUES (2)')

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--file',type=Path)
    parser.add_argument('--health',action='store_true')
    args=parser.parse_args()
    if args.health:
        assert query('SELECT VERSION FROM PUBLIC._YASP_SEED') == [['2']]
        assert int(query('SELECT COUNT(*) FROM PUBLIC.USERS')[0][0]) >= 3
    else:
        for sql in statements(args.file.read_text() if args.file else sys.stdin.read()):
            print(json.dumps(query(sql)))
