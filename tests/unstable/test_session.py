from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

from vercel import unstable as vercel
from vercel._internal.unstable.errors import (
    VercelServiceOptionsError,
    VercelSessionClosedError,
    VercelSessionError,
)
from vercel._internal.unstable.options import ServiceOptions
from vercel._internal.unstable.sandbox.options import DEFAULT_SANDBOX_API_BASE_URL
from vercel._internal.unstable.session import (
    SdkSession,
    SyncSdkSession,
    get_active_session,
    get_active_sync_session,
)
from vercel.unstable import sandbox
from vercel.unstable.sandbox import SandboxServiceOptions, sync as sync_sandbox


@dataclass(frozen=True, slots=True)
class OtherServiceOptions(ServiceOptions):
    value: str


class CountingClient(httpx.Client):
    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        super().close()


class CountingAsyncClient(httpx.AsyncClient):
    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    async def aclose(self) -> None:
        self.close_calls += 1
        await super().aclose()


def test_runtime_dependency_direction_and_removed_layers() -> None:
    internal = Path(__file__).parents[2] / "src/vercel/_internal/unstable"
    sandbox_internal = internal / "sandbox"

    deleted_modules = (
        "context.py",
        "sandbox/client.py",
        "sandbox/handles.py",
        "sandbox/operations.py",
    )
    for deleted in deleted_modules:
        assert not (internal / deleted).exists()

    for runtime in ("async_runtime.py", "sync_runtime.py"):
        source = (sandbox_internal / runtime).read_text()
        assert "vercel._internal.unstable.session" not in source
        assert "vercel.unstable" not in source


def test_sync_session_options_inherit_replace_reject_duplicates_and_close() -> None:
    parent = get_active_sync_session()
    sandbox_outer = SandboxServiceOptions(base_url="https://outer.example.com")
    sandbox_inner = SandboxServiceOptions()
    other_outer = OtherServiceOptions(value="outer")

    def factory() -> httpx.Client:
        return httpx.Client()

    with vercel.session(service_options=[sandbox_outer, other_outer], httpx_client_factory=factory):
        outer_session = get_active_sync_session()
        assert outer_session is not parent
        assert outer_session.get_service_option(SandboxServiceOptions) is sandbox_outer
        assert outer_session.get_service_option(OtherServiceOptions) is other_outer
        assert outer_session.get_setting("httpx_client_factory") is factory

        with vercel.session(service_options=[sandbox_inner]):
            inner_session = get_active_sync_session()
            inner_sandbox_options = inner_session.get_service_option(SandboxServiceOptions)
            assert inner_sandbox_options is sandbox_inner
            assert inner_sandbox_options.base_url == DEFAULT_SANDBOX_API_BASE_URL
            assert inner_session.get_service_option(OtherServiceOptions) is other_outer
            assert inner_session.get_setting("httpx_client_factory") is factory

        assert get_active_sync_session() is outer_session
        assert inner_session.is_closed

    assert get_active_sync_session() is parent
    assert outer_session.is_closed
    with pytest.raises(VercelSessionClosedError):
        outer_session.sandbox_service()

    with pytest.raises(VercelServiceOptionsError):
        with vercel.session(
            service_options=[
                SandboxServiceOptions(base_url="https://one.example.com"),
                SandboxServiceOptions(base_url="https://two.example.com"),
            ],
        ):
            pass


@pytest.mark.asyncio
async def test_async_sessions_inherit_factory_and_explicit_none_resets_it() -> None:
    calls = 0

    def factory() -> httpx.AsyncClient:
        nonlocal calls
        calls += 1
        return httpx.AsyncClient()

    async with vercel.session(httpx_client_factory=factory):
        get_active_session().sandbox_service()
        async with vercel.session():
            inherited = get_active_session()
            assert inherited.get_setting("httpx_client_factory") is factory
            inherited.sandbox_service()
        async with vercel.session(httpx_client_factory=None):
            reset = get_active_session()
            assert reset.get_setting("httpx_client_factory") is None
            reset.sandbox_service()

    assert calls == 2
    assert inherited.is_closed
    assert reset.is_closed
    with pytest.raises(VercelSessionClosedError):
        inherited.sandbox_service()


def test_default_sessions_are_independent() -> None:
    assert isinstance(get_active_session(), SdkSession)
    assert isinstance(get_active_sync_session(), SyncSdkSession)
    assert get_active_session() is SdkSession.default()
    assert get_active_sync_session() is SyncSdkSession.default()


@pytest.mark.asyncio
async def test_explicit_scopes_reject_opposite_mode_use_and_nesting() -> None:
    async with vercel.session():
        with pytest.raises(VercelSessionError):
            sync_sandbox.get_snapshot(snapshot_id="snap_123")
        with pytest.raises(VercelSessionError):
            with vercel.session():
                pass

    with vercel.session():
        with pytest.raises(VercelSessionError):
            await sandbox.get_snapshot(snapshot_id="snap_123")
        with pytest.raises(VercelSessionError):
            async with vercel.session():
                pass


def test_sync_factory_is_lazy_shared_and_closed_once() -> None:
    clients: list[CountingClient] = []

    def factory() -> httpx.Client:
        client = CountingClient()
        clients.append(client)
        return client

    with vercel.session(httpx_client_factory=factory):
        session = get_active_sync_session()
        assert clients == []
        assert session.sandbox_service() is session.sandbox_service()
        assert len(clients) == 1

    assert clients[0].close_calls == 1


@pytest.mark.asyncio
async def test_async_factory_is_lazy_shared_and_closed_once() -> None:
    clients: list[CountingAsyncClient] = []

    def factory() -> httpx.AsyncClient:
        client = CountingAsyncClient()
        clients.append(client)
        return client

    async with vercel.session(httpx_client_factory=factory):
        session = get_active_session()
        assert clients == []
        service = session.sandbox_service()
        assert service is session.sandbox_service()
        assert not hasattr(service, "aclose")
        assert not hasattr(service.api_client, "aclose")
        assert len(clients) == 1

    assert clients[0].close_calls == 1


@pytest.mark.asyncio
async def test_async_session_rejects_and_closes_sync_factory_client() -> None:
    client = CountingClient()

    async with vercel.session(httpx_client_factory=lambda: client):
        with pytest.raises(VercelSessionError):
            get_active_session().sandbox_service()

    assert client.close_calls == 1


def test_sync_session_rejects_and_closes_async_factory_client() -> None:
    client = CountingAsyncClient()

    with vercel.session(httpx_client_factory=lambda: client):
        with pytest.raises(VercelSessionError):
            get_active_sync_session().sandbox_service()

    assert client.close_calls == 1
