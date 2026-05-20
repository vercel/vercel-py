"""Default-session state for the unstable facade."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token

from vercel._internal.unstable.errors import SessionLifecycleError
from vercel._internal.unstable.session import Session, SessionOptions

# Process-global fallback state
_fallback_options: SessionOptions | None = None
_fallback_session: Session | None = None

# Context-local session binding
_default_session_ctx: ContextVar[Session | None] = ContextVar(
    "vercel_unstable_default_session", default=None
)


class DefaultSessionReconfigurationError(SessionLifecycleError):
    """Raised when setup_default_session is called after the fallback has initialized."""


def setup_default_session(*, options: SessionOptions | None = None) -> None:
    """Configure the process-global default session.

    Can only be called before the fallback session has been lazily initialized.
    Raises DefaultSessionReconfigurationError if called after initialization.
    """
    global _fallback_session, _fallback_options

    if _fallback_session is not None:
        raise DefaultSessionReconfigurationError(
            "setup_default_session cannot be called after the default session has been initialized"
        )

    _fallback_options = options


def get_default_session() -> Session:
    """Return the effective default session.

    Prefers the context-local bound session, otherwise lazily creates or returns
    the process-global fallback session.
    """
    ctx_session = _default_session_ctx.get()
    if ctx_session is not None:
        return ctx_session

    global _fallback_session

    if _fallback_session is None:
        _fallback_session = Session(options=_fallback_options)

    return _fallback_session


@asynccontextmanager
async def use_session(session: Session) -> AsyncIterator[None]:
    """Bind a session for the current async context.

    The binding is task-local, nests predictably, and restores the previous
    binding on exit. Does not initialize or close the bound session.
    """
    token: Token[Session | None] = _default_session_ctx.set(session)
    try:
        yield
    finally:
        _default_session_ctx.reset(token)
