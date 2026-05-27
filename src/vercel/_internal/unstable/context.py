"""Context-local SDK session state."""

from contextvars import ContextVar, Token

from vercel._internal.unstable.session import SdkSession

_active_session: ContextVar[SdkSession | None] = ContextVar(
    "vercel_unstable_active_session",
    default=None,
)


def get_active_session() -> SdkSession:
    session = _active_session.get()
    if session is None:
        session = SdkSession.default()
        _active_session.set(session)
    return session


def bind_active_session(session: SdkSession) -> Token[SdkSession | None]:
    return _active_session.set(session)


def reset_active_session(token: Token[SdkSession | None]) -> None:
    _active_session.reset(token)
