"""Public surface over a mocked network: sync and async, validation, sessions."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx2 as httpx
import pytest
from conftest import SCHEDULE_JSON, TEST_BASE_URL, session_options

import vendor.respx as respx
from vercel.api import session
from vercel.errors import VercelError
from vercel.schedules import (
    DEFAULT_SCHEDULES_BASE_URL,
    MAX_JITTER,
    MIN_JITTER,
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
AT = datetime(2026, 10, 1, 9, 30, tzinfo=timezone.utc)
JITTER = timedelta(minutes=2)


def create_route() -> respx.Route:
    return respx.post(SCHEDULES_URL).mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))


def sent_body(route: respx.Route) -> dict[str, Any]:
    return json.loads(route.calls.last.request.content)


# --- options ---------------------------------------------------------------


def test_default_options_target_the_public_service(mock_env_clear: None) -> None:
    options = SchedulesServiceOptions()

    assert options.base_url == DEFAULT_SCHEDULES_BASE_URL == "https://vercel-schedules.com"
    assert options.token is None


def test_base_url_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_SCHEDULE_BASE_URL", "https://staging.test/")

    assert SchedulesServiceOptions().base_url == "https://staging.test/"


def test_token_and_credentials_factory_are_exclusive() -> None:
    async def factory() -> str:
        return "x"

    with pytest.raises(SchedulesValidationError):
        SchedulesServiceOptions(token="t", credentials_factory=factory)


def test_options_are_frozen() -> None:
    options = SchedulesServiceOptions()

    with pytest.raises(SchedulesValidationError, match="frozen"):
        options.base_url = "https://elsewhere.test"  # type: ignore[misc]


# --- create ----------------------------------------------------------------


@respx.mock
async def test_create_cron_schedule_async(mock_env_clear: None) -> None:
    route = create_route()

    async with session(service_options=session_options()):
        schedule = await create_schedule(
            "cleanup",
            topic="scheduled-cleanup",
            cron="0 * * * *",
            timezone="America/New_York",
            namespace="jobs",
            jitter=JITTER,
            payload={"maxAgeDays": 30},
        )

    assert isinstance(schedule, Schedule)
    assert schedule.name == "cleanup"
    assert schedule.timezone == "UTC"
    assert route.calls.last.request.headers["authorization"] == "Bearer oidc-token"
    assert sent_body(route) == {
        "name": "cleanup",
        "namespace": "jobs",
        "expression": {"type": "cron", "cron": "0 * * * *"},
        "timezone": "America/New_York",
        "jitter": 120,
        "target": {"type": "queue", "topic": "scheduled-cleanup"},
        "payload": {"maxAgeDays": 30},
    }


@respx.mock
def test_create_cron_schedule_sync_sends_the_same_body(mock_env_clear: None) -> None:
    route = create_route()

    with session(service_options=session_options()):
        schedule = schedules_sync.create_schedule(
            "cleanup",
            topic="scheduled-cleanup",
            cron="0 * * * *",
            timezone="America/New_York",
            namespace="jobs",
            jitter=JITTER,
            payload={"maxAgeDays": 30},
        )

    assert schedule.schedule_id == "sch_123"
    assert sent_body(route) == {
        "name": "cleanup",
        "namespace": "jobs",
        "expression": {"type": "cron", "cron": "0 * * * *"},
        "timezone": "America/New_York",
        "jitter": 120,
        "target": {"type": "queue", "topic": "scheduled-cleanup"},
        "payload": {"maxAgeDays": 30},
    }


@respx.mock
async def test_create_one_off_from_zoneinfo_datetime(mock_env_clear: None) -> None:
    route = create_route()
    local = datetime(2026, 10, 1, 9, 30, 15, 250_000, tzinfo=ZoneInfo("America/Los_Angeles"))

    async with session(service_options=session_options()):
        await create_schedule("report", topic="send-report", at=local)

    assert sent_body(route) == {
        "name": "report",
        "expression": {"type": "single", "at": "2026-10-01T09:30:15"},
        "timezone": "America/Los_Angeles",
        "target": {"type": "queue", "topic": "send-report"},
    }


@respx.mock
async def test_create_one_off_from_utc_datetime(mock_env_clear: None) -> None:
    route = create_route()

    async with session(service_options=session_options()):
        await create_schedule("report", topic="send-report", at=AT)

    body = sent_body(route)
    assert body["expression"] == {"type": "single", "at": "2026-10-01T09:30:00"}
    assert body["timezone"] == "UTC"


@respx.mock
async def test_create_one_off_from_naive_datetime_with_timezone(mock_env_clear: None) -> None:
    route = create_route()

    async with session(service_options=session_options()):
        await create_schedule(
            "report",
            topic="send-report",
            at=datetime(2026, 10, 1, 9, 30),
            timezone="Europe/Berlin",
        )

    body = sent_body(route)
    assert body["expression"] == {"type": "single", "at": "2026-10-01T09:30:00"}
    assert body["timezone"] == "Europe/Berlin"


@respx.mock
async def test_create_one_off_converts_aware_datetime_into_explicit_timezone(
    mock_env_clear: None,
) -> None:
    route = create_route()

    async with session(service_options=session_options()):
        await create_schedule("report", topic="send-report", at=AT, timezone="Asia/Tokyo")

    body = sent_body(route)
    assert body["expression"] == {"type": "single", "at": "2026-10-01T18:30:00"}
    assert body["timezone"] == "Asia/Tokyo"


@respx.mock
async def test_create_omits_optional_fields(mock_env_clear: None) -> None:
    route = create_route()

    async with session(service_options=session_options()):
        await create_schedule("tick", topic="t", cron="* * * * *")

    assert sent_body(route) == {
        "name": "tick",
        "expression": {"type": "cron", "cron": "* * * * *"},
        "target": {"type": "queue", "topic": "t"},
    }


@respx.mock
async def test_create_preserves_explicit_json_null_payload(mock_env_clear: None) -> None:
    route = create_route()

    async with session(service_options=session_options()):
        await create_schedule("tick", topic="t", cron="* * * * *", payload=None)

    assert sent_body(route)["payload"] is None


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({}, "exactly one of cron or at"),
        ({"cron": "* * * * *", "at": AT}, "exactly one of cron or at"),
        ({"cron": "  "}, "cron must be"),
        ({"cron": "* * * * *", "timezone": ""}, "timezone must be"),
        ({"at": AT.replace(tzinfo=None)}, "at is naive"),
        ({"at": AT.astimezone(timezone(timedelta(hours=-5)))}, "fixed offset"),
        ({"at": AT, "timezone": "Mars/Olympus_Mons"}, "not a known IANA timezone"),
        ({"cron": "* * * * *", "jitter": timedelta(seconds=-1)}, "between"),
        ({"cron": "* * * * *", "jitter": timedelta(seconds=59)}, "between"),
        ({"cron": "* * * * *", "jitter": timedelta(minutes=15, seconds=1)}, "between"),
        ({"cron": "* * * * *", "jitter": timedelta(minutes=1, milliseconds=500)}, "whole number"),
        ({"cron": "* * * * *", "jitter": 120}, "jitter must be a timedelta"),
    ],
)
@respx.mock
async def test_create_rejects_bad_arguments_before_any_request(
    mock_env_clear: None, kwargs: dict[str, Any], match: str
) -> None:
    route = create_route()

    async with session(service_options=session_options()):
        with pytest.raises(SchedulesValidationError, match=match):
            await create_schedule("tick", topic="t", **kwargs)
        with pytest.raises(ValueError, match=match):
            await create_schedule("tick", topic="t", **kwargs)

    assert not route.called


@pytest.mark.parametrize("jitter", [MIN_JITTER, timedelta(minutes=7), MAX_JITTER])
@respx.mock
async def test_create_accepts_jitter_bounds_inclusive(
    mock_env_clear: None, jitter: timedelta
) -> None:
    route = create_route()

    async with session(service_options=session_options()):
        await create_schedule("tick", topic="t", cron="* * * * *", jitter=jitter)

    assert sent_body(route)["jitter"] == int(jitter.total_seconds())


@respx.mock
async def test_create_rejects_empty_name_topic_and_namespace(mock_env_clear: None) -> None:
    async with session(service_options=session_options()):
        with pytest.raises(SchedulesValidationError, match="name"):
            await create_schedule("", topic="t", cron="* * * * *")
        with pytest.raises(SchedulesValidationError, match="topic"):
            await create_schedule("tick", topic="", cron="* * * * *")
        with pytest.raises(SchedulesValidationError, match="namespace"):
            await create_schedule("tick", topic="t", cron="* * * * *", namespace="")


# --- list ------------------------------------------------------------------


def page(*ids: str, cursor: str | None) -> httpx.Response:
    return httpx.Response(
        200,
        json={"data": [{**SCHEDULE_JSON, "scheduleId": i} for i in ids], "cursor": cursor},
    )


@respx.mock
async def test_list_follows_cursors_async(mock_env_clear: None) -> None:
    route = respx.get(SCHEDULES_URL).mock(
        side_effect=[page("a", "b", cursor="c1"), page("c", cursor=None)]
    )

    async with session(service_options=session_options()):
        ids = [s.schedule_id async for s in list_schedules(namespace="jobs", page_size=2)]

    assert ids == ["a", "b", "c"]
    urls = [str(call.request.url) for call in route.calls]
    assert urls == [
        f"{SCHEDULES_URL}?namespace=jobs&limit=2",
        f"{SCHEDULES_URL}?namespace=jobs&cursor=c1&limit=2",
    ]


@respx.mock
def test_list_follows_cursors_sync(mock_env_clear: None) -> None:
    respx.get(SCHEDULES_URL).mock(side_effect=[page("a", cursor="c1"), page("b", cursor=None)])

    with session(service_options=session_options()):
        ids = [s.schedule_id for s in schedules_sync.list_schedules()]

    assert ids == ["a", "b"]


@respx.mock
async def test_list_stops_on_an_empty_page_even_with_a_cursor(mock_env_clear: None) -> None:
    route = respx.get(SCHEDULES_URL).mock(return_value=page(cursor="loop"))

    async with session(service_options=session_options()):
        assert [s async for s in list_schedules()] == []

    assert route.call_count == 1


@respx.mock
async def test_list_rejects_non_positive_page_size(mock_env_clear: None) -> None:
    async with session(service_options=session_options()):
        with pytest.raises(SchedulesValidationError):
            async for _ in list_schedules(page_size=0):
                pass


@pytest.mark.parametrize("page_size", [True, 1.5, "10"])
@respx.mock
async def test_list_rejects_non_integer_page_size(mock_env_clear: None, page_size: Any) -> None:
    async with session(service_options=session_options()):
        with pytest.raises(SchedulesValidationError, match="positive integer"):
            async for _ in list_schedules(page_size=page_size):
                pass


# --- get / update / enable / disable / invoke / delete --------------------


@respx.mock
async def test_name_based_operations_async(mock_env_clear: None) -> None:
    get = respx.get(CLEANUP_URL).mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))
    respx.post(f"{CLEANUP_URL}/enable").mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))
    respx.post(f"{CLEANUP_URL}/disable").mock(
        return_value=httpx.Response(200, json={**SCHEDULE_JSON, "state": "inactive"})
    )
    invoke = respx.post(f"{CLEANUP_URL}/invoke").mock(return_value=httpx.Response(202))
    delete = respx.delete(CLEANUP_URL).mock(
        return_value=httpx.Response(200, json={"scheduleId": "sch_123"})
    )

    async with session(service_options=session_options()):
        assert (await get_schedule("cleanup", namespace="jobs")).name == "cleanup"
        assert (await enable_schedule("cleanup", namespace="jobs")).is_active
        assert not (await disable_schedule("cleanup", namespace="jobs")).is_active
        await invoke_schedule("cleanup", namespace="jobs")
        await delete_schedule("cleanup", namespace="jobs")

    for route in (get, invoke, delete):
        assert route.calls.last.request.url.params["namespace"] == "jobs"


@respx.mock
def test_name_based_operations_sync(mock_env_clear: None) -> None:
    respx.get(CLEANUP_URL).mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))
    respx.post(f"{CLEANUP_URL}/enable").mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))
    respx.post(f"{CLEANUP_URL}/disable").mock(
        return_value=httpx.Response(200, json={**SCHEDULE_JSON, "state": "inactive"})
    )
    respx.post(f"{CLEANUP_URL}/invoke").mock(return_value=httpx.Response(202))
    delete = respx.delete(CLEANUP_URL).mock(return_value=httpx.Response(204))

    with session(service_options=session_options()):
        assert schedules_sync.get_schedule("cleanup").name == "cleanup"
        assert schedules_sync.enable_schedule("cleanup").is_active
        assert not schedules_sync.disable_schedule("cleanup").is_active
        schedules_sync.invoke_schedule("cleanup")
        schedules_sync.delete_schedule("cleanup")

    assert "namespace" not in delete.calls.last.request.url.params


@respx.mock
async def test_update_sends_only_supplied_fields(mock_env_clear: None) -> None:
    route = respx.patch(CLEANUP_URL).mock(
        return_value=httpx.Response(
            200, json={**SCHEDULE_JSON, "expression": {"type": "cron", "cron": "0 2 * * *"}}
        )
    )

    async with session(service_options=session_options()):
        schedule = await update_schedule(
            "cleanup",
            namespace="jobs",
            cron="0 2 * * *",
            timezone="America/New_York",
            topic="daily-cleanup",
            jitter=None,
            payload={"job": "cleanup"},
        )

    assert schedule.expression.cron == "0 2 * * *"  # type: ignore[union-attr]
    assert route.calls.last.request.url.params["namespace"] == "jobs"
    assert sent_body(route) == {
        "expression": {"type": "cron", "cron": "0 2 * * *"},
        "timezone": "America/New_York",
        "target": {"type": "queue", "topic": "daily-cleanup"},
        "jitter": None,
        "payload": {"job": "cleanup"},
    }


@respx.mock
def test_update_single_field_sync(mock_env_clear: None) -> None:
    route = respx.patch(CLEANUP_URL).mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))

    with session(service_options=session_options()):
        schedules_sync.update_schedule("cleanup", jitter=timedelta(minutes=5))

    assert sent_body(route) == {"jitter": 300}


@respx.mock
async def test_update_one_off_derives_timezone(mock_env_clear: None) -> None:
    route = respx.patch(CLEANUP_URL).mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))

    async with session(service_options=session_options()):
        await update_schedule("cleanup", at=datetime(2026, 12, 1, 8, tzinfo=ZoneInfo("Asia/Tokyo")))

    assert sent_body(route) == {
        "expression": {"type": "single", "at": "2026-12-01T08:00:00"},
        "timezone": "Asia/Tokyo",
    }


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({}, "at least one field"),
        ({"cron": "* * * * *", "at": AT}, "at most one of cron or at"),
        ({"jitter": timedelta(seconds=30)}, "between"),
        ({"topic": ""}, "topic"),
    ],
)
@respx.mock
async def test_update_rejects_bad_arguments(
    mock_env_clear: None, kwargs: dict[str, Any], match: str
) -> None:
    route = respx.patch(CLEANUP_URL).mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))

    async with session(service_options=session_options()):
        with pytest.raises(SchedulesValidationError, match=match):
            await update_schedule("cleanup", **kwargs)

    assert not route.called


@respx.mock
async def test_empty_name_is_rejected(mock_env_clear: None) -> None:
    async with session(service_options=session_options()):
        with pytest.raises(SchedulesValidationError, match="name"):
            await get_schedule("")
        with pytest.raises(SchedulesValidationError, match="namespace"):
            await get_schedule("cleanup", namespace="")


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
    respx.get(CLEANUP_URL).mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))
    calls = 0

    async def factory() -> str:
        nonlocal calls
        calls += 1
        return f"tok-{calls}"

    options = SchedulesServiceOptions(base_url=TEST_BASE_URL, credentials_factory=factory)
    async with session(service_options=[options]):
        await get_schedule("cleanup")
        await get_schedule("cleanup")

    assert calls == 2
    assert respx.calls.last.request.headers["authorization"] == "Bearer tok-2"


@respx.mock
async def test_missing_credentials_surface_as_credentials_error(mock_env_clear: None) -> None:
    respx.get(CLEANUP_URL).mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))

    async with session(service_options=[SchedulesServiceOptions(base_url=TEST_BASE_URL)]):
        with pytest.raises(SchedulesCredentialsError):
            await get_schedule("cleanup")


@respx.mock
def test_missing_credentials_surface_as_credentials_error_sync(mock_env_clear: None) -> None:
    respx.get(CLEANUP_URL).mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))

    with session(service_options=[SchedulesServiceOptions(base_url=TEST_BASE_URL)]):
        with pytest.raises(SchedulesCredentialsError):
            schedules_sync.get_schedule("cleanup")


@respx.mock
def test_sync_session_rejects_a_suspending_credentials_factory(mock_env_clear: None) -> None:
    route = respx.get(CLEANUP_URL).mock(return_value=httpx.Response(200, json=SCHEDULE_JSON))

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
