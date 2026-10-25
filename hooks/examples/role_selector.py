"""Example application policy; map values and context keys are application-owned."""


def select_backend(request, params):
    """Show policy-based routing with a secondary route as the demo fallback."""
    fallback = {"target": "primary", "backend_login": "analyst"}
    # return None  # Return None to deny backend selection.

    context = request.get("query_context", {})
    workload = context.get("workload")
    user_id = context.get("user_id")
    department = context.get("department")
    report_kind = context.get("report_kind")
    if not all(
        isinstance(value, str) and value
        for value in (workload, user_id, department, report_kind)
    ):
        return fallback

    # The lookup checks that the supplied context maps to a loaded application row.
    datasets = request.get("datasets")
    if datasets is None:
        return fallback
    user = datasets.get_many("users", [user_id]).get(user_id)
    if not isinstance(user, dict) or user.get("department") != department:
        return fallback

    # Read the complete field-location dataset for routing context only; no masking.
    _email_columns = datasets.read("email_locations").get("columns", [])

    target = params["target_by_workload"].get(workload)
    login = params["login_by_department"].get(department, {}).get(target)
    if not isinstance(target, str) or not target or not isinstance(login, str) or not login:
        return fallback

    missing = object()
    compute_type = params["type_by_report"].get(report_kind, missing)
    if compute_type is missing or (compute_type is not None and (not isinstance(compute_type, str) or not compute_type)):
        return fallback

    decision = {"target": target, "backend_login": login}
    if compute_type is not None:
        decision["compute_type"] = compute_type
    return decision


if __name__ == "__main__":
    class Datasets:
        def get_many(self, name, keys):
            return {"alice": {"department": "finance"}} if name == "users" and "alice" in keys else {}

        def read(self, name):
            return {"columns": ["crm.contacts.email"]} if name == "email_locations" else {}

    params = {
        "target_by_workload": {"reporting": "olap"},
        "login_by_department": {"finance": {"olap": "restricted"}},
        "type_by_report": {"dashboard": "small"},
    }
    request = {
        "query_context": {
            "workload": "reporting",
            "user_id": "alice",
            "department": "finance",
            "report_kind": "dashboard",
        },
        "datasets": Datasets(),
    }
    assert select_backend(request, params) == {
        "target": "olap", "backend_login": "restricted", "compute_type": "small"
    }
    wrong_department = {**request["query_context"], "department": "sales"}
    assert select_backend({"query_context": wrong_department, "datasets": Datasets()}, params) == {
        "target": "primary", "backend_login": "analyst"
    }
    unknown_user = {**request["query_context"], "user_id": "eve"}
    assert select_backend({"query_context": unknown_user, "datasets": Datasets()}, params) == {
        "target": "primary", "backend_login": "analyst"
    }
    assert select_backend({"query_context": {}}, params) == {
        "target": "primary", "backend_login": "analyst"
    }
