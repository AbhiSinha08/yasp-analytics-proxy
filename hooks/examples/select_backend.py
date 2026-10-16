"""Select a configured target/login and optional compute type."""


def select_backend(request, params):
    return {
        "target": params["target"],
        "backend_login": params["backend_login"],
        **({"compute_type": params["compute_type"]} if "compute_type" in params else {}),
    }
