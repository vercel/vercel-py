"""Default-session placeholders for the unstable facade."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from vercel._internal.unstable.session import Session, SessionOptions


def setup_default_session(*, options: SessionOptions | None = None) -> None:
    """Configure the process-global default session.

    Runtime behavior is implemented in a later slice.
    """
    _ = options
    raise NotImplementedError("default-session setup will be implemented in Slice 07")


def get_default_session() -> Session:
    """Return the effective default session."""
    raise NotImplementedError("default-session resolution will be implemented in Slice 07")


@asynccontextmanager
async def use_session(session: Session) -> AsyncIterator[None]:
    """Bind a session for the current async context."""
    _ = session
    raise NotImplementedError("context-local session binding will be implemented in Slice 07")
    yield
