"""Application example; client SQL remarks are local-demo hints, not identity."""

import re


def derive_context(request, params=None):
    params = params or {}
    if "datasets" not in request:
        return None
    # Test fixture only. SQL comments are client-controlled and do not prove identity.
    match = re.search(
        r"/\* yasp-test user=([\w-]+) workload=([\w-]+) report=([\w-]+) \*/",
        request["sql"],
    )
    if match is not None:
        user_id, workload, report_kind = match.groups()
    else:
        if not request["sql"].lstrip().startswith(("--", "/*")):
            return None
        import yasp_extractors

        metadata = (
            yasp_extractors.metabase_rs(request["sql"])
            or yasp_extractors.metabase_pg(request["sql"])
            or yasp_extractors.redash(request["sql"])
        )
        if metadata is None:
            return None
        user_id = str(metadata["user_id"])
        workload = params.get("workload")
        report_kind = params.get("report_kind")
    user = request["datasets"].get("users", user_id)
    if not isinstance(user, dict):
        return None
    locations = request["datasets"].read("email_locations")
    return {
        "user_id": user_id,
        "department": user.get("department"),
        "workload": workload,
        "report_kind": report_kind,
        # Context helps the app locate fields; no values are masked or rewritten.
        "email_columns": locations.get("columns", []),
    }


if __name__ == "__main__":
    class Datasets:
        def get(self, name, key):
            return {"department": "finance"} if (name, key) == ("users", "alice") else None

        def read(self, name):
            return {"columns": ["crm.contacts.email"]} if name == "email_locations" else {}

    request = {
        "sql": "/* yasp-test user=alice workload=reporting report=dashboard */ SELECT 1",
        "datasets": Datasets(),
    }
    assert derive_context(request)["department"] == "finance"
    assert derive_context({"sql": "SELECT 1", "datasets": Datasets()}) is None
