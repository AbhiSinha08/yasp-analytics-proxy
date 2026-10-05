"""No-op illustration of proposed signatures; not loaded by YASP."""


def before_query(context, sql, parameters):
    """Leave SQL and parameter semantics unchanged."""
    return None


def after_result(context, columns, rows):
    """Return the bounded batch without changing its schema."""
    return rows


def on_connection(context, event):
    """Observe a lifecycle event without side effects."""
    return None
