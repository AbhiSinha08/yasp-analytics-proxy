"""Select the configured target/login supplied by the consuming application."""


def select_backend(request, options):
    return {
        "target": options["target"],
        "backend_login": options["backend_login"],
    }
