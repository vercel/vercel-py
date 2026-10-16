"""Shared fixtures for standalone Sandbox tests."""

from collections.abc import Generator

import pytest
from fake_sandbox import FakeSandboxApi
from sandbox_modes import Mode
from sandbox_scenarios import AsyncDriver, SyncDriver, _ScenarioDriver

pytest_plugins = ["vendor.respx.plugin"]


@pytest.fixture
def mock_env_clear(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Prevent tests from resolving credentials from the developer environment."""
    for name in (
        "VERCEL_TOKEN",
        "VERCEL_TEAM_ID",
        "VERCEL_PROJECT_ID",
        "VERCEL_OIDC_TOKEN",
        "VERCEL_OIDC_TOKEN_HEADER",
        "VERCEL_REGION",
    ):
        monkeypatch.delenv(name, raising=False)

    from vercel.oidc.token import _clear_cached_oidc_token

    _clear_cached_oidc_token()
    yield
    _clear_cached_oidc_token()


@pytest.fixture
def fake_api(mock_env_clear: None) -> FakeSandboxApi:
    """An in-memory Sandbox API the SDK can be pointed at."""
    return FakeSandboxApi()


@pytest.fixture(params=["async", "sync"])
def driver(request: pytest.FixtureRequest, fake_api: FakeSandboxApi) -> _ScenarioDriver:
    """Drive the async or the sync public API against ``fake_api``."""
    if request.param == "async":
        return AsyncDriver(**fake_api.session_kwargs(sync=False))
    return SyncDriver(**fake_api.session_kwargs(sync=True))


@pytest.fixture(params=["async", "sync"])
def mode(request: pytest.FixtureRequest, fake_api: FakeSandboxApi) -> Mode:
    """Run the test once through the async and once through the sync API."""
    return Mode(request.param, fake_api)
