"""Role-selection callback scaffold; not loaded by YASP."""


def select_backend(context, policy):
    """Select a configured target/login and policy-dependent cache scope."""
    raise NotImplementedError("Configure a role-selection script before serving queries.")
