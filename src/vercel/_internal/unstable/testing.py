"""Internal testing hooks for unstable default-session state."""

from __future__ import annotations

from vercel._internal.unstable import default as _default_state


def reset_default_session() -> None:
    """Reset default-session state for tests.

    Clears the process-global fallback session and options, and resets
    the context-local binding to unbound.
    """
    _default_state._fallback_options = None
    _default_state._fallback_session = None
    _default_state._default_session_ctx.set(None)


__all__ = ["reset_default_session"]
