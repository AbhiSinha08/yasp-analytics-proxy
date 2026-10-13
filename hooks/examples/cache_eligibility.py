"""Bypass live monitoring queries using case-insensitive SQL fragments."""


def cache_eligibility(request, params):
    # Substrings also match comments/literals; this only bypasses caching.
    sql = request["sql"].lower()
    return not any(
        fragment.lower() in sql for fragment in params.get("excluded_sql_fragments", [])
    )
