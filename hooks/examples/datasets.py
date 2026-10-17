"""Small JSON loader used by the shared-datasets configuration example."""

import json
from pathlib import Path


def load_json(request, params):
    """Load one JSON object from this file's directory or an explicit path."""
    path = Path(params["file"])
    if not path.is_absolute():
        path = Path(__file__).parent / path
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError("a dataset file must contain a JSON object")
    return value


if __name__ == "__main__":
    assert load_json({}, {"file": "users.json"})["alice"]["department"] == "finance"
