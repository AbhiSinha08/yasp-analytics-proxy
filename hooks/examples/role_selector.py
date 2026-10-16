"""Example application policy; map values and context keys are application-owned."""


def select_backend(request, params):
    """Return a configured route or deny when required classifications are absent."""
    context = request.get("context", {})
    workload = context.get("workload")
    user_id = context.get("user_id")
    report_kind = context.get("report_kind")
    if not all(isinstance(value, str) and value for value in (workload, user_id, report_kind)):
        return None

    target = params["target_by_workload"].get(workload)
    login = params["login_by_user"].get(user_id, {}).get(target)
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
    params = {
        "target_by_workload": {"reporting": "olap"},
        "login_by_user": {"alice": {"olap": "restricted"}},
        "type_by_report": {"dashboard": "small"},
    }
    request = {"context": {"workload": "reporting", "user_id": "alice", "report_kind": "dashboard"}}
    assert select_backend(request, params) == {
        "target": "olap", "backend_login": "restricted", "compute_type": "small"
    }
    assert select_backend({"context": {}}, params) is None
