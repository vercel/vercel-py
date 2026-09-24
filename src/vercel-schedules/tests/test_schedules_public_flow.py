"""Public surface over a mocked network: sessions, credentials, and pagination.

Argument validation and body rendering are covered in `test_schedules_service`;
here each operation is exercised end to end once, plus the session and
credential behaviour that only shows up with a real transport in the loop.
"""

import asyncio
import json
from datetime import timedelta
from typing import Any

import httpx2 as httpx
import pytest
from conftest import SCHEDULE_JSON, TEST_BASE_URL, session_options

import vendor.respx as respx
from vercel.api import session
from vercel.errors import VercelError
from vercel.schedules import (
    DEFAULT_SCHEDULES_BASE_URL,
    Schedule,
    SchedulesCredentialsError,
    SchedulesServiceOptions,
    SchedulesValidationError,
    create_schedule,
    delete_schedule,
    disable_schedule,
    enable_schedule,
    get_schedule,
    invoke_schedule,
    list_schedules,
    sync as schedules_sync,
    update_schedule,
)

SCHEDULES_URL = f"{TEST_BASE_URL}/v1/schedules"
CLEANUP_URL = f"{SCHEDULES_URL}/cleanup"


def sent_body(route: respx.Route) -> dict[str, Any]:
    return json.loads(route.calls.last.request.content)


def page(*ids: str, cursor: str | None) -> httpx.Response:
    return httpx.Response(
        200,
        json={"data": [{**SCHEDULE_JSON, "scheduleId": i} for i in ids], "cursor": cursor},
    )


def ok(**overrides: Any) -> httpx.Response:
    return httpx.Response(200, json={**SCHEDULE_JSON, **overrides})


def mock_name_routes() -> dict[str, respx.Route]:
    return {
        "get": respx.get(CLEANUP_URL).mock(return_value=ok()),
        "patch": respx.patch(CLEANUP_URL).mock(return_value=ok()),
        "enable": respx.post(f"{CLEANUP_URL}/enable").mock(return_value=ok()),
        "disable": respx.post(f"{CLEANUP_URL}/disable").mock(return_value=ok(state="inactive")),
        "invoke": respx.post(f"{CLEANUP_URL}/invoke").mock(return_value=httpx.Response(202)),
        "delete": respx.delete(CLEANUP_URL).mock(return_value=httpx.Response(204)),
    }


# --- options ---------------------------------------------------------------


def test_base_url_defaults_to_public_service_unless_env_overrides(
    mock_env_clear: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert SchedulesServiceOptions().base_url == DEFAULT_SCHEDULES_BASE_URL

    monkeypatch.setenv("VERCEL_SCHEDULE_BASE_URL", "https://staging.test/")
    assert SchedulesServiceOptions().base_url == "https://staging.test/"


def test_token_and_credentials_factory_are_exclusive() -> None:
    async def factory() -> str:
        return "x"

    with pytest.raises(SchedulesValidationError):
        SchedulesServiceOptions(token="t", credentials_factory=factory)


# --- async surface ---------------------------------------------------------


@respx.mock
async def test_create_sends_body_with_bearer_token(mock_env_clear: None) -> None:
    route = respx.post(SCHEDULES_URL).mock(return_value=ok())

    async with session(service_options=session_options()):
        schedule = await create_schedule(
            "cleanup",
            topic="scheduled-cleanup",
            cron="0 * * * *",
            namespace="jobs",
            jitter=timedelta(minutes=2),
        )

    assert isinstance(schedule, Schedule)
    assert schedule.name == "cleanup"
    assert route.calls.last.request.headers["authorization"] == "Bearer oidc-token"
    assert sent_body(route) == {
        "name": "cleanup",
        "namespace": "jobs",
        "expression": {"type": "cron", "cron": "0 * * * *"},
        "jitter": 2,
        "target": {"type": "queue", "topic": "scheduled-cleanup"},
    }


@respx.mock
async def test_name_based_operations_pass_name_and_namespace(mock_env_clear: None) -> None:
    routes = mock_name_routes()

    async with session(service_options=session_options()):
        assert (await get_schedule("cleanup", namespace="jobs")).name == "cleanup"
        await update_schedule("cleanup", namespace="jobs", jitter=timedelta(minutes=5))
        assert (await enable_schedule("cleanup", namespace="jobs")).is_active
        assert not (await disable_schedule("cleanup", namespace="jobs")).is_active
        await invoke_schedule("cleanup", namespace="jobs")
        await delete_schedule("cleanup", namespace="jobs")

    for route in routes.values():
        assert route.call_count == 1
        assert route.calls.last.request.url.params["namespace"] == "jobs"
    assert sent_body(routes["patch"]) == {"jitter": 5}


@respx.mock
async def test_list_follows_cursors(mock_env_clear: None) -> None:
    route = respx.get(SCHEDULES_URL).mock(
        side_effect=[page("a", "b", cursor="c1"), page("c", cursor=None)]
    )

    async with session(service_options=session_options()):
        ids = [s.schedule_id async for s in list_schedules(namespace="jobs", page_size=2)]

    assert ids == ["a", "b", "c"]
    assert [str(call.request.url) for call in route.calls] == [
        f"{SCHEDULES_URL}?namespace=jobs&limit=2",
        f"{SCHEDULES_URL}?namespace=jobs&cursor=c1&limit=2",
    ]


@respx.mock
async def test_list_stops_on_an_empty_page_even_with_a_cursor(mock_env_clear: None) -> None:
    route = respx.get(SCHEDULES_URL).mock(return_value=page(cursor="loop"))

    async with session(service_options=session_options()):
        assert [s async for s in list_schedules()] == []

    assert route.call_count == 1


@respx.mock
async def test_list_rejects_non_positive_page_size(mock_env_clear: None) -> None:
    async with session(service_options=session_options()):
        with pytest.raises(SchedulesValidationError, match="positive integer"):
            async for _ in list_schedules(page_size=0):
                pass


@respx.mock
async def test_validation_fails_before_any_request(mock_env_clear: None) -> None:
    create = respx.post(SCHEDULES_URL).mock(return_value=ok())
    routes = mock_name_routes()

    async with session(service_options=session_options()):
        with pytest.raises(SchedulesValidationError, match="exactly one of cron or at"):
            await create_schedule("tick", topic="t")
        with pytest.raises(SchedulesValidationError, match="at least one field"):
            await update_schedule("cleanup")
        with pytest.raises(SchedulesValidationError, match="name"):
            await get_schedule("")
        with pytest.raises(SchedulesValidationError, match="namespace"):
            await delete_schedule("cleanup", namespace="")

    assert not create.called
    assert not any(route.called for route in routes.values())


# --- sync surface ----------------------------------------------------------


@respx.mock
def test_sync_surface_drives_every_operation(mock_env_clear: None) -> None:
    create = respx.post(SCHEDULES_URL).mock(return_value=ok())
    listing = respx.get(SCHEDULES_URL).mock(
        side_effect=[page("a", cursor="c1"), page("b", cursor=None)]
    )
    routes = mock_name_routes()

    with session(service_options=session_options()):
        created = schedules_sync.create_schedule("cleanup", topic="t", cron="0 * * * *")
        ids = [s.schedule_id for s in schedules_sync.list_schedules()]
        assert schedules_sync.get_schedule("cleanup").name == "cleanup"
        schedules_sync.update_schedule("cleanup", jitter=timedelta(minutes=5))
        assert schedules_sync.enable_schedule("cleanup").is_active
        assert not schedules_sync.disable_schedule("cleanup").is_active
        schedules_sync.invoke_schedule("cleanup")
        schedules_sync.delete_schedule("cleanup")

    assert created.schedule_id == "sch_123"
    assert ids == ["a", "b"]
    assert sent_body(create)["expression"] == {"type": "cron", "cron": "0 * * * *"}
    assert listing.call_count == 2
    assert sent_body(routes["patch"]) == {"jitter": 5}
    for route in routes.values():
        assert route.call_count == 1
        assert "namespace" not in route.calls.last.request.url.params


# --- sessions and credentials ---------------------------------------------


@respx.mock
async def test_async_surface_rejects_a_sync_session(mock_env_clear: None) -> None:
    with pytest.raises(VercelError):
        with session(service_options=session_options()):
            await get_schedule("cleanup")


@respx.mock
def test_sync_surface_rejects_an_async_session(mock_env_clear: None) -> None:
    async def run() -> None:
        async with session(service_options=session_options()):
            schedules_sync.get_schedule("cleanup")

    with pytest.raises(VercelError):
        asyncio.run(run())


@respx.mock
async def test_credentials_factory_is_called_per_request(mock_env_clear: None) -> None:
    respx.get(CLEANUP_URL).mock(return_value=ok())
    tokens = iter(["tok-1", "tok-2"])

    async def factory() -> str:
        return next(tokens)

    options = SchedulesServiceOptions(base_url=TEST_BASE_URL, credentials_factory=factory)
    async with session(service_options=[options]):
        await get_schedule("cleanup")
        await get_schedule("cleanup")

    assert [c.request.headers["authorization"] for c in respx.calls] == [
        "Bearer tok-1",
        "Bearer tok-2",
    ]


@respx.mock
async def test_missing_credentials_surface_as_credentials_error(mock_env_clear: None) -> None:
    respx.get(CLEANUP_URL).mock(return_value=ok())

    async with session(service_options=[SchedulesServiceOptions(base_url=TEST_BASE_URL)]):
        with pytest.raises(SchedulesCredentialsError):
            await get_schedule("cleanup")


@respx.mock
def test_missing_credentials_surface_as_credentials_error_sync(mock_env_clear: None) -> None:
    respx.get(CLEANUP_URL).mock(return_value=ok())

    with session(service_options=[SchedulesServiceOptions(base_url=TEST_BASE_URL)]):
        with pytest.raises(SchedulesCredentialsError):
            schedules_sync.get_schedule("cleanup")


@respx.mock
def test_sync_session_rejects_a_suspending_credentials_factory(mock_env_clear: None) -> None:
    route = respx.get(CLEANUP_URL).mock(return_value=ok())

    async def factory() -> str:
        await asyncio.sleep(0)
        return "token"

    options = SchedulesServiceOptions(base_url=TEST_BASE_URL, credentials_factory=factory)
    with session(service_options=[options]):
        with pytest.raises(SchedulesCredentialsError, match="suspended in a sync session"):
            schedules_sync.get_schedule("cleanup")

    assert not route.called


@respx.mock
async def test_timeout_propagates(mock_env_clear: None) -> None:
    respx.get(CLEANUP_URL).mock(side_effect=httpx.TimeoutException("timed out"))

    async with session(service_options=session_options()):
        with pytest.raises(httpx.TimeoutException):
            await get_schedule("cleanup")
