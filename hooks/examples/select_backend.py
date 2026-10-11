"""Select the configured target/login supplied by the consuming application."""


def select_backend(request, params):
    return {
        "target": params["target"],
        "backend_login": params["backend_login"],
    }
