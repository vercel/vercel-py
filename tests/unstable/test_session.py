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


def test_session_options_inherit_replace_reject_duplicates_and_invalidate() -> None:
    parent = get_active_session()
    sandbox_outer = SandboxServiceOptions(base_url="https://outer.example.com")
    sandbox_inner = SandboxServiceOptions()
    other_outer = OtherServiceOptions(value="outer")

    with vercel.session(
        service_options=[sandbox_outer, other_outer],
        httpx_client_factory="outer-factory",
    ):
        outer_session = get_active_session()
        assert outer_session is not parent
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
        assert not inner_session.is_alive

    assert get_active_session() is parent
    assert not outer_session.is_alive
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
