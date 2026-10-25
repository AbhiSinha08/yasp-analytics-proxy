"""Application account routing and schema-preserving email presentation masks."""


def _account(request):
    user_id = request.get("query_context", {}).get("user_id")
    if isinstance(user_id, bool) or not isinstance(user_id, (int, str)):
        return None
    user = request["datasets"].get("users", str(user_id))
    return user.get("account") if isinstance(user, dict) else None


def select_backend(request, params):
    account = _account(request)
    if not isinstance(account, str) or not account:
        return None
    login = params["login_by_account"].get(account)
    if not isinstance(login, str) or not login:
        return None
    return {"target": params["target"], "backend_login": login}


def _can_read_email(request):
    account = _account(request)
    if not isinstance(account, str) or not account:
        raise ValueError("missing account")
    access = request["datasets"].get("pii_access", account)
    if not isinstance(access, dict):
        raise ValueError("missing account permissions")
    return access.get("email") is True


def _mask(batch, column):
    values = batch.text_column(column)
    batch.replace_text_column(column, [None if value is None else "XXXX" for value in values])


def mask_email_from_ast(request, batch, params):
    if _can_read_email(request):
        return
    ast = request["data_contexts"]["ast"]
    if ast is None or ast.get("unknown_outputs"):
        raise ValueError("email lineage unavailable")
    tags = request["datasets"].read("email_columns")
    columns = {column["ordinal"]: column for column in ast["columns"]}
    for output in request["columns"]:
        lineage = columns[output["ordinal"]]
        if any("unknown" in source for source in lineage["sources"]):
            raise ValueError("email lineage unresolved")
        if any(
            tags.get(source.get("column"), {}).get("tag") == "email"
            for source in lineage["sources"]
        ):
            _mask(batch, output["ordinal"])


def mask_email_from_model(request, batch, params):
    if _can_read_email(request):
        return
    decisions = request["data_contexts"]["decision_model"]
    if decisions is None:
        raise ValueError("email classification unavailable")
    columns = {entry["column"]["ordinal"]: entry for entry in decisions["columns"]}
    for output in request["columns"]:
        outcome = columns[output["ordinal"]]["outcome"]
        if outcome["status"] != "available":
            raise ValueError("email classification unavailable")
        answer = outcome["answers"]["email"]
        if answer["value"] >= 0.5:
            _mask(batch, output["ordinal"])
        elif answer["confidence"] < 0.5:
            raise ValueError("email classification uncertain")
