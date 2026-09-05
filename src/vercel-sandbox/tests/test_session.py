from dataclasses import dataclass

import httpx2 as httpx
import pytest

from vercel import sandbox
from vercel._internal.core.options import ServiceOptions
from vercel._internal.core.session import get_active_session, get_active_sync_session
from vercel.api import session
from vercel.errors import (
    VercelServiceOptionsError,
    VercelSessionClosedError,
    VercelSessionError,
)
from vercel.sandbox import sync as sync_sandbox
from vercel.sandbox._internal.service import get_sandbox_service, get_sync_sandbox_service


@dataclass(frozen=True, slots=True)
class OtherServiceOptions(ServiceOptions):
    value: str


def test_sandbox_option_modes_are_mutually_exclusive() -> None:
    async_options = sandbox.SandboxServiceOptions()
    sync_options = sync_sandbox.SandboxServiceOptions()

    with pytest.raises(VercelServiceOptionsError, match="one object per logical service"):
        with session(service_options=[async_options, sync_options]):
            pass


@pytest.mark.asyncio
async def test_sandbox_option_mode_is_validated_when_service_is_accessed() -> None:
    async_options = sandbox.SandboxServiceOptions()
    sync_options = sync_sandbox.SandboxServiceOptions()

    with session(service_options=[async_options]):
        active = get_active_sync_session()
        with pytest.raises(
            VercelServiceOptionsError,
            match=(
                "SandboxServiceOptions cannot configure this session mode; "
                "use SyncSandboxServiceOptions"
            ),
        ):
            get_sync_sandbox_service(active)

    async with session(service_options=[sync_options]):
        active_async = get_active_session()
        with pytest.raises(
            VercelServiceOptionsError,
            match=(
                "SyncSandboxServiceOptions cannot configure this session mode; "
                "use SandboxServiceOptions"
            ),
        ):
            get_sandbox_service(active_async)


@pytest.mark.asyncio
async def test_nested_scope_replaces_sandbox_option_mode() -> None:
    sync_options = sync_sandbox.SandboxServiceOptions()
    async_options = sandbox.SandboxServiceOptions()

    async with session(service_options=[sync_options]):
        async with session(service_options=[async_options]):
            assert (
                get_active_session().get_service_option(sandbox.SandboxServiceOptions)
                is async_options
            )


def test_sandbox_options_inherit_and_service_is_cached_for_session() -> None:
    sandbox_outer = sync_sandbox.SandboxServiceOptions(base_url="https://outer.example.com")
    other_outer = OtherServiceOptions(value="outer")

    def factory() -> httpx.Client:
        return httpx.Client()

    with session(service_options=[sandbox_outer, other_outer], httpx_client_factory=factory):
        outer_session = get_active_sync_session()
        assert outer_session.get_service_option(sync_sandbox.SandboxServiceOptions) is sandbox_outer
        assert outer_session.get_service_option(OtherServiceOptions) is other_outer
        service = get_sync_sandbox_service(outer_session)
        assert get_sync_sandbox_service(outer_session) is service

        with session():
            inner_session = get_active_sync_session()
            assert (
                inner_session.get_service_option(sync_sandbox.SandboxServiceOptions)
                is sandbox_outer
            )
            assert inner_session.get_service_option(OtherServiceOptions) is other_outer
            assert get_sync_sandbox_service(inner_session) is not service

        assert inner_session.is_closed

    assert outer_session.is_closed
    with pytest.raises(VercelSessionClosedError):
        get_sync_sandbox_service(outer_session)


@pytest.mark.asyncio
async def test_public_sandbox_calls_obey_session_runtime_mode() -> None:
    async with session():
        with pytest.raises(VercelSessionError):
            sync_sandbox.get_snapshot(snapshot_id="snap_123")
        with pytest.raises(VercelSessionError):
            with session():
                pass

    with session():
        with pytest.raises(VercelSessionError):
            await sandbox.get_snapshot(snapshot_id="snap_123")
        with pytest.raises(VercelSessionError):
            async with session():
                pass
