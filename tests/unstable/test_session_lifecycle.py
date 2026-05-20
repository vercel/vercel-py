from __future__ import annotations

import pytest

from vercel._internal.unstable.errors import SessionClosedError, SessionLifecycleError
from vercel.unstable import Session, SyncSession, VercelError


async def test_async_session_context_manager_closes_after_body_error() -> None:
    session = Session()

    with pytest.raises(RuntimeError, match="body failed"):
        async with session as active:
            assert active is session
            raise RuntimeError("body failed")

    with pytest.raises(SessionClosedError):
        await session.initialize()


def test_sync_session_context_manager_closes_after_body_error() -> None:
    session = SyncSession()

    with pytest.raises(RuntimeError, match="body failed"):
        with session as active:
            assert active is session
            raise RuntimeError("body failed")

    with pytest.raises(SessionClosedError):
        session.initialize()


async def test_async_session_close_is_idempotent_and_prevents_future_initialize() -> None:
    session = Session()

    await session.aclose()
    await session.aclose()

    with pytest.raises(SessionClosedError):
        await session.initialize()


def test_sync_session_close_is_idempotent_and_prevents_future_initialize() -> None:
    session = SyncSession()

    session.close()
    session.close()

    with pytest.raises(SessionClosedError):
        session.initialize()


def test_closed_session_error_is_typed_under_vercel_error() -> None:
    error = SessionClosedError("closed")

    assert isinstance(error, SessionLifecycleError)
    assert isinstance(error, VercelError)
