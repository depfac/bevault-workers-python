"""Shared helpers for store configuration."""


def _mapping_key_for_name(mapping, name: str):
    """Return the actual dict key in ``mapping`` that matches ``name`` case-insensitively."""
    folded = name.casefold()
    for k in mapping:
        if isinstance(k, str) and k.casefold() == folded:
            return k
    return None


def get_first(mapping, *keys):
    """Return the value for the first of ``keys`` that is present in ``mapping``.

    Lookup is **case-insensitive** for string keys (e.g. ``Host`` and ``host`` match).
    ``keys`` are tried in order; use multiple names only when they denote different
    fields (e.g. ``user`` vs ``username``), not for casing variants.
    """
    for name in keys:
        if isinstance(name, str):
            resolved = _mapping_key_for_name(mapping, name)
            if resolved is not None:
                return mapping[resolved]
        elif name in mapping:
            return mapping[name]
    return None
