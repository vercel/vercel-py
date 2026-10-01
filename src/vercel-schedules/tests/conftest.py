"""Shared fixtures for Schedules tests."""

from collections.abc import Generator
from typing import Any

import pytest

from vercel.schedules import SchedulesServiceOptions

pytest_plugins = ["vendor.respx.plugin"]

TEST_BASE_URL = "https://schedules.test"
TEST_TOKEN = "oidc-token"

SCHEDULE_JSON: dict[str, Any] = {
    "scheduleId": "sch_123",
    "ownerId": "team_123",
    "projectId": "prj_123",
    "trackId": "production",
    "namespace": "jobs",
    "name": "cleanup",
    "timezone": "UTC",
    "expression": {"type": "cron", "cron": "0 * * * *"},
    "target": {"type": "queue", "topic": "scheduled-cleanup"},
    "state": "active",
    "source": "dynamic",
    "createdAt": 1_700_000_000_000,
    "updatedAt": 1_700_000_000_000,
}

DISPATCH_HEADERS: dict[str, str] = {
    "ce-specversion": "1.0",
    "ce-type": "com.vercel.schedule.v1beta",
    "ce-source": "/schedule/default/every-ten",
    "ce-id": "01JABCDEF0123456789012345:1788350400000",
    "ce-time": "2026-09-02T12:00:00.000Z",
    "ce-vssscheduleid": "01JABCDEF0123456789012345",
    "ce-vssschedulename": "every-ten",
    "ce-vssnamespace": "default",
    "ce-vssschedulesource": "static",
}


@pytest.fixture
def mock_env_clear(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Prevent tests from resolving credentials from the developer environment."""
    for name in (
        "VERCEL_TOKEN",
        "VERCEL_TEAM_ID",
        "VERCEL_PROJECT_ID",
        "VERCEL_OIDC_TOKEN",
        "VERCEL_OIDC_TOKEN_HEADER",
        "VERCEL_SCHEDULE_BASE_URL",
    ):
        monkeypatch.delenv(name, raising=False)

    from vercel.oidc.token import _clear_cached_oidc_token

    _clear_cached_oidc_token()
    yield
    _clear_cached_oidc_token()


def session_options(
    *,
    base_url: str = TEST_BASE_URL,
    token: str = TEST_TOKEN,
) -> list[SchedulesServiceOptions]:
    """Service options wired to a test base URL and a fixed token."""
    return [SchedulesServiceOptions(base_url=base_url, token=token)]
