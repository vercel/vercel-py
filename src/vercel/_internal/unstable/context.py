"""Context-local explicit SDK session state."""

from contextvars import ContextVar, Token

from vercel._internal.unstable.errors import VercelSessionError
from vercel._internal.unstable.session import SdkSession, SyncSdkSession

_active_session: ContextVar[SdkSession | SyncSdkSession | None] = ContextVar(
    "vercel_unstable_active_session",
    default=None,
)


def get_active_session() -> SdkSession:
    session = _active_session.get()
    if session is None:
        return SdkSession.default()
    if isinstance(session, SyncSdkSession):
        raise VercelSessionError("Async unstable APIs cannot be used in a sync SDK session")
    return session


def get_active_sync_session() -> SyncSdkSession:
    session = _active_session.get()
    if session is None:
        return SyncSdkSession.default()
    if isinstance(session, SdkSession):
        raise VercelSessionError("Sync unstable APIs cannot be used in an async SDK session")
    return session


def bind_active_session(
    session: SdkSession | SyncSdkSession,
) -> Token[SdkSession | SyncSdkSession | None]:
    return _active_session.set(session)


def reset_active_session(token: Token[SdkSession | SyncSdkSession | None]) -> None:
    _active_session.reset(token)
