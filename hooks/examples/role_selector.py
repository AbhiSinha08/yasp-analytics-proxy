"""Example application policy; map values and context keys are application-owned."""


def select_backend(request, params):
    """Return a configured route or deny when required classifications are absent."""
    context = request.get("context", {})
    workload = context.get("workload")
    user_id = context.get("user_id")
    department = context.get("department")
    report_kind = context.get("report_kind")
    if not all(
        isinstance(value, str) and value
        for value in (workload, user_id, department, report_kind)
    ):
        return None

    # The lookup checks that the supplied context maps to a loaded application row.
    datasets = request.get("datasets")
    if datasets is None:
        return None
    user = datasets.get_many("users", [user_id]).get(user_id)
    if not isinstance(user, dict) or user.get("department") != department:
        return None

    # Read the complete field-location dataset for routing context only; no masking.
    _email_columns = datasets.read("email_locations").get("columns", [])

    target = params["target_by_workload"].get(workload)
    login = params["login_by_department"].get(department, {}).get(target)
    if not isinstance(target, str) or not target or not isinstance(login, str) or not login:
        return None

    missing = object()
    compute_type = params["type_by_report"].get(report_kind, missing)
    if compute_type is missing or (compute_type is not None and (not isinstance(compute_type, str) or not compute_type)):
        return None

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
        "context": {
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
    wrong_department = {**request["context"], "department": "sales"}
    assert select_backend({"context": wrong_department, "datasets": Datasets()}, params) is None
    unknown_user = {**request["context"], "user_id": "eve"}
    assert select_backend({"context": unknown_user, "datasets": Datasets()}, params) is None
    assert select_backend({"context": {}}, params) is None
