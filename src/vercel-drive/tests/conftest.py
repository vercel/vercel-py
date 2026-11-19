"""Shared fixtures for Drive tests."""

import inspect
from collections.abc import AsyncIterator, Generator
from contextlib import asynccontextmanager
from types import ModuleType
from typing import Any

import pytest

from vercel import drive
from vercel._internal.core.session import SdkSession, SyncSdkSession
from vercel.api import session
from vercel.drive import DriveCredentials, RegionInput, sync as drive_sync

pytest_plugins = ["vendor.respx.plugin"]

TEST_BASE_URL = "https://drive.test"
DRIVES_URL = f"{TEST_BASE_URL}/v1/drives"
SCOPE = {"teamId": "team_123", "projectId": "prj_123"}


def drive_json(**overrides: Any) -> dict[str, Any]:
    """A `{"drive": ...}` response body, as the Drives API returns it."""
    fields = {
        "id": "drive_123",
        "name": "cache",
        "projectId": "prj_123",
        "region": "iad1",
        "maxSizeBytes": 1024**3,
        "workloadType": "sandbox",
        "currentSessionId": "sbx_123",
        "currentSandboxName": "preview",
        "createdAt": 1,
        "updatedAt": 2,
        **overrides,
    }
    return {"drive": {key: value for key, value in fields.items() if value is not None}}


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
def delays(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record retry waits instead of sleeping."""
    recorded: list[float] = []

    async def sleep(self: object, seconds: float) -> None:
        recorded.append(seconds)

    monkeypatch.setattr(SdkSession, "sleep", sleep)
    monkeypatch.setattr(SyncSdkSession, "sleep", sleep)
    return recorded


class DriveApi:
    """Run one scenario against `vercel.drive` or `vercel.drive.sync`.

    Every call is awaitable, so a test reads the same in both modes.
    """

    def __init__(self, *, sync: bool) -> None:
        self.sync = sync
        self.module: ModuleType = drive_sync if sync else drive
        self.handle_type: type = drive_sync.SyncDrive if sync else drive.Drive

    def options(self, *, region: RegionInput = None) -> Any:
        credentials = DriveCredentials(token="token", team_id="team_123", project_id="prj_123")
        if self.sync:
            return drive_sync.DriveServiceOptions(
                base_url=TEST_BASE_URL, credentials_factory=lambda: credentials, region=region
            )

        async def credentials_factory() -> DriveCredentials:
            return credentials

        return drive.DriveServiceOptions(
            base_url=TEST_BASE_URL, credentials_factory=credentials_factory, region=region
        )

    @asynccontextmanager
    async def session(self, *, options: Any = None) -> AsyncIterator[None]:
        service_options = [self.options() if options is None else options]
        if self.sync:
            with session(service_options=service_options):
                yield
        else:
            async with session(service_options=service_options):
                yield

    async def call(self, operation: Any, /, **kwargs: Any) -> Any:
        """Call a module function by name, or a bound handle method."""
        if isinstance(operation, str):
            operation = getattr(self.module, operation)
        result = operation(**kwargs)
        return await result if inspect.isawaitable(result) else result

    async def query(self, **kwargs: Any) -> list[Any]:
        drives = self.module.query_drives(**kwargs)
        if self.sync:
            return list(drives)
        return [item async for item in drives]


@pytest.fixture(params=["async", "sync"])
def api(request: pytest.FixtureRequest, mock_env_clear: None) -> DriveApi:
    return DriveApi(sync=request.param == "sync")
