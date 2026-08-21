from __future__ import annotations

from datetime import datetime
from importlib import import_module


def get_deadline() -> datetime | None:
    """Return the current Function invocation deadline, if available."""
    try:
        runtime = import_module("vercel_runtime")
    except ModuleNotFoundError as exc:
        # Only the runtime itself being absent means "not on Vercel". A
        # missing transitive dependency is a broken install and must surface.
        if exc.name != "vercel_runtime":
            raise
        return None

    accessor = getattr(runtime, "get_deadline", None)
    if not callable(accessor):
        # Older runtime without deadline support.
        return None

    value = accessor()
    return value if isinstance(value, datetime) else None
