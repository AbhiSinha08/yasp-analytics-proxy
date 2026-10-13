"""Optional BI context example; the extractors are placeholders today."""

import yasp_extractors


def derive_context(request, params=None):
    params = params or {}
    extractor = getattr(yasp_extractors, params.get("source", "metabase"), None)
    if extractor is None:
        return None

    try:
        metadata = extractor(request["sql"])
    except NotImplementedError:
        return None

    if metadata is None:
        return None

    # These are application-owned example settings, not extractor fields.
    identity_key = params.get("identity_key")
    identities = params.get("identities", {})
    if identity_key and identity_key in metadata:
        identity = identities.get(str(metadata[identity_key]))
        if identity is not None:
            return {**metadata, "identity": identity}
    return metadata
