"""Pinned Fakesnow metadata corrections using actual DuckDB definitions.

DuckDB supplies generated constraint names. Its FOREIGN KEY references are
schema-local. This fixture reports those facts rather than synthesizing keys.
"""
from contextlib import contextmanager


def show_keys(expression, current_database, *, kind):
    from sqlglot import exp, parse_one
    expected = 'IMPORTED' if kind == 'FOREIGN' else kind
    if not isinstance(expression, exp.Show) or expression.name.upper() != expected + ' KEYS':
        return expression
    scope = expression.args.get('scope')
    scope_kind = expression.args.get('scope_kind')
    conditions = ["c.table_name NOT LIKE '_fs_%'"]
    literal = lambda s: "'" + s.replace("'", "''") + "'"
    if current_database and scope_kind != 'ACCOUNT':
        conditions.append('c.database_name = ' + literal(current_database))
    if scope_kind == 'SCHEMA' and scope:
        conditions.append('c.schema_name = ' + literal(scope.name))
        if scope.db:
            conditions.append('c.database_name = ' + literal(scope.db))
    elif scope_kind == 'TABLE' and scope:
        conditions.append('c.table_name = ' + literal(scope.name))
        if scope.db:
            conditions.append('c.schema_name = ' + literal(scope.db))
        if scope.catalog:
            conditions.append('c.database_name = ' + literal(scope.catalog))
    where = ' AND '.join(conditions)
    if kind == 'FOREIGN':
        sql = f"""SELECT to_timestamp(0)::timestamptz AS created_on,
        p.database_name AS pk_database_name, p.schema_name AS pk_schema_name,
        p.table_name AS pk_table_name, c.referenced_column_names[pos] AS pk_column_name,
        c.database_name AS fk_database_name, c.schema_name AS fk_schema_name,
        c.table_name AS fk_table_name, c.constraint_column_names[pos] AS fk_column_name,
        pos AS key_sequence, 'NO ACTION' AS update_rule, 'NO ACTION' AS delete_rule,
        c.constraint_name AS fk_name, p.constraint_name AS pk_name,
        'NOT DEFERRABLE' AS deferrability, false AS rely, NULL::VARCHAR AS comment
        FROM duckdb_constraints() c
        JOIN duckdb_constraints() p ON p.database_name = c.database_name
        AND p.schema_name = c.schema_name AND p.table_name = c.referenced_table
        AND p.constraint_column_names = c.referenced_column_names
        AND p.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        CROSS JOIN LATERAL range(1, len(c.constraint_column_names) + 1) positions(pos)
        WHERE c.constraint_type = 'FOREIGN KEY' AND {where}"""
    else:
        constraint = 'PRIMARY KEY' if kind == 'PRIMARY' else 'UNIQUE'
        sql = f"""SELECT to_timestamp(0)::timestamptz AS created_on,
        c.database_name AS database_name, c.schema_name AS schema_name, c.table_name AS table_name,
        c.constraint_column_names[pos] AS column_name, pos AS key_sequence,
        c.constraint_name AS constraint_name, false AS rely, NULL::VARCHAR AS comment
        FROM duckdb_constraints() c
        CROSS JOIN LATERAL range(1, len(c.constraint_column_names) + 1) positions(pos)
        WHERE c.constraint_type = '{constraint}' AND {where}"""
    return parse_one(sql, dialect='duckdb')


@contextmanager
def metadata_fix():
    from string import Template
    from fakesnow import info_schema
    import fakesnow.transforms as transforms
    from fakesnow.cursor import FakeSnowflakeCursor
    from sqlglot import exp
    original_execute = FakeSnowflakeCursor._execute
    original_comments = transforms.extract_comment_on_columns
    original_tables = info_schema.SQL_CREATE_INFORMATION_SCHEMA_TABLES_VIEW
    # Snowflake INFORMATION_SCHEMA.TABLES includes views; the pinned fixture
    # only enumerates duckdb_tables and would silently omit them from discovery.
    info_schema.SQL_CREATE_INFORMATION_SCHEMA_TABLES_VIEW = Template(
        original_tables.template
        .replace('create view if not exists', 'create or replace view')
        .replace("'BASE TABLE' AS \"TABLE_TYPE\"", 'tables.relation_type AS "TABLE_TYPE"')
        .replace('from duckdb_tables tables', '''from (
            select database_name, schema_name, table_name, estimated_size,
                   'BASE TABLE' as relation_type from duckdb_tables()
            union all
            select database_name, schema_name, view_name, NULL::BIGINT,
                   'VIEW' as relation_type from duckdb_views() where not internal
        ) tables''')
    )

    def comments(expression):
        table = expression.this.copy() if isinstance(expression, exp.Alter) else None
        result = original_comments(expression)
        if table is not None and result.args.get("col_comments"):
            result.args["yasp_column_table"] = table
        return result

    def execute(cursor, transformed, params=None):
        if table := transformed.args.get("yasp_column_table"):
            quote = lambda value: '"' + value.replace('"', '""') + '"'
            target = '.'.join(quote(s) for s in [table.catalog or cursor._conn.database, table.db or cursor._conn.schema, table.name])
            for name, comment in transformed.args["col_comments"]:
                literal = "'" + comment.replace("'", "''") + "'"
                cursor._duck_conn.execute('COMMENT ON COLUMN ' + target + '.' + quote(name) + ' IS ' + literal)
        if isinstance(transformed, exp.Create) and transformed.args.get("kind") == "TABLE" and transformed.find(exp.Reference):
            target = transformed.this
            if isinstance(target, exp.Schema):
                target = target.this
            database = target.catalog or cursor._conn.database
            schema = target.db or cursor._conn.schema
            quote = lambda value: '"' + value.replace('"', '""') + '"'
            # DuckDB resolves unqualified FK parents against its current database.
            # Fakesnow normally stores only the logical session coordinates.
            cursor._duck_conn.execute('USE ' + quote(database) + '.' + quote(schema))
            for reference in transformed.find_all(exp.Reference):
                table = reference.this.this if isinstance(reference.this, exp.Schema) else reference.this
                if isinstance(table, exp.Table) and table.catalog == database:
                    table.set('catalog', None)
        return original_execute(cursor, transformed, params)

    original = transforms.show_keys
    transforms.show_keys = show_keys
    FakeSnowflakeCursor._execute = execute
    transforms.extract_comment_on_columns = comments
    try:
        yield
    finally:
        transforms.show_keys = original
        FakeSnowflakeCursor._execute = original_execute
        transforms.extract_comment_on_columns = original_comments
        info_schema.SQL_CREATE_INFORMATION_SCHEMA_TABLES_VIEW = original_tables
