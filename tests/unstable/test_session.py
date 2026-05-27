from dataclasses import dataclass

import pytest

from vercel import unstable as vercel
from vercel._internal.unstable.context import get_active_session
from vercel._internal.unstable.errors import (
    VercelServiceOptionsError,
    VercelSessionClosedError,
)
from vercel._internal.unstable.options import ServiceOptions
from vercel._internal.unstable.sandbox.options import DEFAULT_SANDBOX_API_BASE_URL
from vercel.unstable.sandbox import SandboxServiceOptions


@dataclass(frozen=True, slots=True)
class OtherServiceOptions(ServiceOptions):
    value: str


def test_sync_session_context_restores_parent_and_invalidates_scoped_session() -> None:
    parent = get_active_session()

    with vercel.session():
        scoped = get_active_session()
        assert scoped is not parent
        assert scoped.is_alive

    assert get_active_session() is parent
    assert not scoped.is_alive


async def test_async_session_context_restores_parent_and_invalidates_scoped_session() -> None:
    parent = get_active_session()

    async with vercel.session():
        scoped = get_active_session()
        assert scoped is not parent
        assert scoped.is_alive

    assert get_active_session() is parent
    assert not scoped.is_alive


def test_nested_sessions_inherit_and_replace_options_by_concrete_type() -> None:
    sandbox_outer = SandboxServiceOptions(base_url="https://outer.example.com")
    sandbox_inner = SandboxServiceOptions()
    other_outer = OtherServiceOptions(value="outer")

    with vercel.session(
        service_options=[sandbox_outer, other_outer],
        httpx_client_factory="outer-factory",
    ):
        outer_session = get_active_session()
        assert outer_session.get_service_option(SandboxServiceOptions) is sandbox_outer
        assert outer_session.get_service_option(OtherServiceOptions) is other_outer
        assert outer_session.get_setting("httpx_client_factory") == "outer-factory"

        with vercel.session(
            service_options=[sandbox_inner],
            httpx_client_factory="inner-factory",
        ):
            inner_session = get_active_session()
            inner_sandbox_options = inner_session.get_service_option(SandboxServiceOptions)
            assert inner_sandbox_options is sandbox_inner
            assert inner_sandbox_options.base_url == DEFAULT_SANDBOX_API_BASE_URL
            assert inner_session.get_service_option(OtherServiceOptions) is other_outer
            assert inner_session.get_setting("httpx_client_factory") == "inner-factory"

        assert get_active_session() is outer_session


def test_service_options_reject_duplicate_concrete_types() -> None:
    with pytest.raises(VercelServiceOptionsError):
        with vercel.session(
            service_options=[
                SandboxServiceOptions(base_url="https://one.example.com"),
                SandboxServiceOptions(base_url="https://two.example.com"),
            ],
        ):
            pass


def test_service_options_reject_non_service_options() -> None:
    with pytest.raises(VercelServiceOptionsError):
        with vercel.session(service_options=[object()]):  # type: ignore[list-item]
            pass


def test_sandbox_service_uses_effective_options_and_session_token() -> None:
    options = SandboxServiceOptions(base_url="https://sandbox.example.com")

    with vercel.session(service_options=[options]):
        sdk_session = get_active_session()
        service = sdk_session.sandbox_service()

        assert service is sdk_session.sandbox_service()
        assert service.options is options
        assert service.api_client.base_url == options.base_url
        assert service.api_client.credentials_factory is options.credentials_factory
        assert service.alive_token is sdk_session.alive_token
        assert service.alive_token.is_alive

    assert not service.alive_token.is_alive
    with pytest.raises(VercelSessionClosedError):
        sdk_session.sandbox_service()
