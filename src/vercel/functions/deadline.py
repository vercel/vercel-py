from __future__ import annotations

from datetime import datetime
from importlib import import_module


def get_deadline() -> datetime | None:
    """Return the current Function invocation deadline, if available."""
    try:
        runtime = import_module("vercel_runtime")
        runtime_get_deadline = getattr(runtime, "get_deadline", None)
        if not callable(runtime_get_deadline):
            return None
        value = runtime_get_deadline()
    except Exception:
        return None

    return value if isinstance(value, datetime) else None
