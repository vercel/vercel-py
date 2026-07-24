from dataclasses import dataclass

import httpx
import pytest

from vercel import sandbox
from vercel.api import session
from vercel.errors import (
    VercelSessionClosedError,
    VercelSessionError,
)
from vercel.internal.core.options import ServiceOptions
from vercel.internal.core.session import get_active_sync_session
from vercel.sandbox import SandboxServiceOptions, sync as sync_sandbox
from vercel.sandbox._internal.service import get_sandbox_service


@dataclass(frozen=True, slots=True)
class OtherServiceOptions(ServiceOptions):
    value: str


def test_sandbox_options_inherit_and_service_is_cached_for_session() -> None:
    sandbox_outer = SandboxServiceOptions(base_url="https://outer.example.com")
    other_outer = OtherServiceOptions(value="outer")

    def factory() -> httpx.Client:
        return httpx.Client()

    with session(service_options=[sandbox_outer, other_outer], httpx_client_factory=factory):
        outer_session = get_active_sync_session()
        assert outer_session.get_service_option(SandboxServiceOptions) is sandbox_outer
        assert outer_session.get_service_option(OtherServiceOptions) is other_outer
        service = get_sandbox_service(outer_session)
        assert get_sandbox_service(outer_session) is service

        with session():
            inner_session = get_active_sync_session()
            assert inner_session.get_service_option(SandboxServiceOptions) is sandbox_outer
            assert inner_session.get_service_option(OtherServiceOptions) is other_outer
            assert get_sandbox_service(inner_session) is not service

        assert inner_session.is_closed

    assert outer_session.is_closed
    with pytest.raises(VercelSessionClosedError):
        get_sandbox_service(outer_session)


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
