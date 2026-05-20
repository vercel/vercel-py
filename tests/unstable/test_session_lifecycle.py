from __future__ import annotations

import asyncio

import pytest
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from vercel._internal.unstable.errors import SessionClosedError, SessionLifecycleError
from vercel.unstable import Session, SessionOptions, SyncSession, VercelError


def test_session_construction_stores_options_without_initializing() -> None:
    options = SessionOptions(client_pool_size=5)

    session = Session(options=options)
    sync_session = SyncSession(options=options)

    assert session.options is options
    assert sync_session.options is options
    assert session._initialized is False
    assert sync_session._initialized is False
    assert session._closed is False
    assert sync_session._closed is False


class AsyncSessionLifecycleMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.session = Session()
        self.expected_initialized = False
        self.expected_closed = False

    @rule()
    def initialize(self) -> None:
        async def run() -> None:
            if self.expected_closed:
                with pytest.raises(SessionClosedError):
                    await self.session.initialize()
            else:
                await self.session.initialize()
                self.expected_initialized = True

        asyncio.run(run())

    @rule()
    def close(self) -> None:
        asyncio.run(self.session.aclose())
        self.expected_closed = True

    @invariant()
    def lifecycle_state_matches_model(self) -> None:
        assert self.session._initialized is self.expected_initialized
        assert self.session._closed is self.expected_closed


class SyncSessionLifecycleMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.session = SyncSession()
        self.expected_initialized = False
        self.expected_closed = False

    @rule()
    def initialize(self) -> None:
        if self.expected_closed:
            with pytest.raises(SessionClosedError):
                self.session.initialize()
        else:
            self.session.initialize()
            self.expected_initialized = True

    @rule()
    def close(self) -> None:
        self.session.close()
        self.expected_closed = True

    @invariant()
    def lifecycle_state_matches_model(self) -> None:
        assert self.session._initialized is self.expected_initialized
        assert self.session._closed is self.expected_closed


TestAsyncSessionLifecycle = AsyncSessionLifecycleMachine.TestCase
TestSyncSessionLifecycle = SyncSessionLifecycleMachine.TestCase


async def test_async_session_context_manager_closes_after_body_error() -> None:
    session = Session()

    with pytest.raises(RuntimeError, match="body failed"):
        async with session as active:
            assert active is session
            assert session._initialized is True
            session._ensure_open()
            raise RuntimeError("body failed")

    assert session._closed is True


def test_sync_session_context_manager_closes_after_body_error() -> None:
    session = SyncSession()

    with pytest.raises(RuntimeError, match="body failed"):
        with session as active:
            assert active is session
            assert session._initialized is True
            session._ensure_open()
            raise RuntimeError("body failed")

    assert session._closed is True


def test_closed_session_error_is_typed_under_vercel_error() -> None:
    error = SessionClosedError("closed")

    assert isinstance(error, SessionLifecycleError)
    assert isinstance(error, VercelError)
