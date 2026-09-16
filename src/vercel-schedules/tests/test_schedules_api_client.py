"""Api-client level tests over a fake transport.

Covers wire shapes, path encoding, error mapping, and malformed-response
handling without a network layer.
"""

import json
from datetime import datetime, timedelta, timezone

import httpx2 as httpx
import pytest
from conftest import SCHEDULE_JSON

from vercel._internal.core.http import (
    BaseTransport,
    JSONBody,
    ReadResponsePolicy,
    RequestBody,
    RequestTimeout,
)
from vercel._internal.core.http.transport import HeaderTypes, QueryParamTypes
from vercel.schedules import (
    CronExpression,
    FunctionTarget,
    OneOffExpression,
    QueueTarget,
    ScheduleNotFoundError,
    SchedulesApiError,
    ScheduleSource,
    SchedulesResponseError,
    ScheduleState,
)
from vercel.schedules._internal.api_client import USER_AGENT, SchedulesApiClient


class FakeTransport(BaseTransport):
    """Records requests and replays canned responses."""

    def __init__(self, *responses: httpx.Response) -> None:
        self.requests: list[httpx.Request] = []
        self._responses = list(responses)

    async def send(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: QueryParamTypes | None = None,
        body: RequestBody = None,
        headers: HeaderTypes | None = None,
        timeout: RequestTimeout = None,
        follow_redirects: bool | None = None,
        stream: bool = False,
        read_response: ReadResponsePolicy = ReadResponsePolicy.NEVER,
    ) -> httpx.Response:
        content = b""
        merged = httpx.Headers(headers)
        if isinstance(body, JSONBody):
            content = json.dumps(body.data).encode()
            merged["content-type"] = "application/json"
        if token is not None:
            merged["authorization"] = f"Bearer {token}"
        request = httpx.Request(method, path, params=params, headers=merged, content=content)
        self.requests.append(request)
        response = self._responses.pop(0) if self._responses else httpx.Response(200, json={})
        response.request = request
        return response


def make_client(*responses: httpx.Response) -> tuple[SchedulesApiClient, FakeTransport]:
    transport = FakeTransport(*responses)

    async def credentials() -> str:
        return "tok"

    client = SchedulesApiClient(
        base_url="https://schedules.test/",
        credentials_factory=credentials,
        transport=transport,
        timeout=timedelta(seconds=5),
    )
    return client, transport


BASE = "https://schedules.test/v1/schedules"


async def test_create_posts_json_body_with_auth_and_user_agent() -> None:
    client, transport = make_client(httpx.Response(200, json=SCHEDULE_JSON))

    body = {
        "name": "cleanup",
        "expression": {"type": "cron", "cron": "0 * * * *"},
        "target": {"type": "queue", "topic": "t"},
    }
    schedule = await client.create_schedule(body)

    assert schedule.schedule_id == "sch_123"
    assert schedule.name == "cleanup"
    request = transport.requests[0]
    assert request.method == "POST"
    assert str(request.url) == BASE
    assert request.headers["authorization"] == "Bearer tok"
    assert request.headers["user-agent"] == USER_AGENT
    assert request.headers["content-type"] == "application/json"
    assert json.loads(request.content) == body


async def test_create_requires_the_full_schedule_in_the_response() -> None:
    client, _ = make_client(httpx.Response(200, json={"scheduleId": "sch_1"}))

    with pytest.raises(SchedulesResponseError):
        await client.create_schedule({"name": "x"})


async def test_list_sends_only_supplied_query_params() -> None:
    client, transport = make_client(
        httpx.Response(200, json={"data": [SCHEDULE_JSON], "cursor": None}),
        httpx.Response(200, json={"data": [], "cursor": None}),
    )

    page = await client.list_schedules(namespace="jobs", cursor="abc", limit=10)
    assert [s.schedule_id for s in page.schedules] == ["sch_123"]
    assert page.next_cursor is None
    assert str(transport.requests[0].url) == f"{BASE}?namespace=jobs&cursor=abc&limit=10"

    await client.list_schedules(namespace=None, cursor=None, limit=None)
    assert str(transport.requests[1].url) == BASE


async def test_get_percent_encodes_the_name_and_maps_the_schedule() -> None:
    client, transport = make_client(
        httpx.Response(
            200,
            json={**SCHEDULE_JSON, "jitter": 120, "timezone": "America/New_York"},
        )
    )

    schedule = await client.get_schedule("clean/up 2", namespace="jobs")

    assert str(transport.requests[0].url) == f"{BASE}/clean%2Fup%202?namespace=jobs"
    assert schedule.schedule_id == "sch_123"
    assert schedule.expression == CronExpression(cron="0 * * * *")
    assert schedule.timezone == "America/New_York"
    assert isinstance(schedule.target, QueueTarget)
    assert schedule.target.topic == "scheduled-cleanup"
    assert schedule.state is ScheduleState.ACTIVE
    assert schedule.is_active
    assert schedule.source is ScheduleSource.DYNAMIC
    assert schedule.jitter == timedelta(minutes=2)
    assert schedule.created_at == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)


async def test_get_without_namespace_sends_no_query() -> None:
    client, transport = make_client(httpx.Response(200, json=SCHEDULE_JSON))

    await client.get_schedule("cleanup", namespace=None)

    assert str(transport.requests[0].url) == f"{BASE}/cleanup"


async def test_missing_timezone_defaults_to_utc() -> None:
    body = {k: v for k, v in SCHEDULE_JSON.items() if k != "timezone"}
    client, _ = make_client(httpx.Response(200, json=body))

    schedule = await client.get_schedule("cleanup", namespace=None)

    assert schedule.timezone == "UTC"


async def test_one_off_expression_keeps_the_wall_clock_time() -> None:
    client, _ = make_client(
        httpx.Response(
            200,
            json={
                **SCHEDULE_JSON,
                "timezone": "America/Los_Angeles",
                "expression": {"type": "single", "at": "2026-10-01T09:00:00"},
            },
        )
    )

    schedule = await client.get_schedule("cleanup", namespace=None)

    assert schedule.expression == OneOffExpression(at=datetime(2026, 10, 1, 9))
    assert schedule.timezone == "America/Los_Angeles"


async def test_function_target_is_parsed() -> None:
    client, _ = make_client(
        httpx.Response(
            200,
            json={**SCHEDULE_JSON, "target": {"type": "function", "function": "jobs/nightly"}},
        )
    )

    schedule = await client.get_schedule("cleanup", namespace=None)

    assert schedule.target == FunctionTarget(function="jobs/nightly")


async def test_unknown_target_is_a_malformed_response() -> None:
    client, _ = make_client(
        httpx.Response(
            200,
            json={**SCHEDULE_JSON, "target": {"type": "webhook", "topic": "not-a-queue"}},
        )
    )

    with pytest.raises(SchedulesResponseError):
        await client.get_schedule("cleanup", namespace=None)


async def test_update_patches_by_name_and_namespace() -> None:
    client, transport = make_client(httpx.Response(200, json=SCHEDULE_JSON))

    body = {"jitter": None, "timezone": "UTC"}
    schedule = await client.update_schedule("cleanup", namespace="jobs", body=body)

    request = transport.requests[0]
    assert schedule.schedule_id == "sch_123"
    assert request.method == "PATCH"
    assert str(request.url) == f"{BASE}/cleanup?namespace=jobs"
    assert json.loads(request.content) == body


async def test_delete_tolerates_204() -> None:
    client, transport = make_client(httpx.Response(204))

    await client.delete_schedule("cleanup", namespace="jobs")
    assert transport.requests[0].method == "DELETE"
    assert str(transport.requests[0].url) == f"{BASE}/cleanup?namespace=jobs"


@pytest.mark.parametrize("action", ["enable", "disable"])
async def test_enable_and_disable_post_without_a_body(action: str) -> None:
    client, transport = make_client(httpx.Response(200, json=SCHEDULE_JSON))

    schedule = await getattr(client, f"{action}_schedule")("cleanup", namespace=None)

    request = transport.requests[0]
    assert schedule.schedule_id == "sch_123"
    assert request.method == "POST"
    assert str(request.url) == f"{BASE}/cleanup/{action}"
    assert request.content == b""
    assert "content-type" not in request.headers


async def test_invoke_posts_without_a_body_and_ignores_the_response() -> None:
    client, transport = make_client(httpx.Response(202))

    await client.invoke_schedule("cleanup", namespace="jobs")

    request = transport.requests[0]
    assert request.method == "POST"
    assert str(request.url) == f"{BASE}/cleanup/invoke?namespace=jobs"
    assert request.content == b""


async def test_404_raises_schedule_not_found_which_is_a_lookup_error() -> None:
    client, _ = make_client(httpx.Response(404, json={"error": {"message": "no such schedule"}}))

    with pytest.raises(ScheduleNotFoundError) as info:
        await client.get_schedule("missing", namespace=None)

    assert isinstance(info.value, LookupError)
    assert info.value.status_code == 404
    assert "no such schedule" in str(info.value)
    assert "status=404" in str(info.value)


async def test_other_errors_carry_status_and_body_text() -> None:
    client, _ = make_client(httpx.Response(501, text="Not implemented"))

    with pytest.raises(SchedulesApiError) as info:
        await client.get_schedule("cleanup", namespace=None)

    assert type(info.value) is SchedulesApiError
    assert info.value.status_code == 501
    assert str(info.value).startswith("Not implemented")


async def test_non_json_success_is_a_response_error() -> None:
    client, _ = make_client(httpx.Response(200, text="<html>"))

    with pytest.raises(SchedulesResponseError):
        await client.get_schedule("cleanup", namespace=None)


async def test_malformed_success_is_a_response_error_with_data() -> None:
    client, _ = make_client(httpx.Response(200, json={"scheduleId": "sch_123"}))

    with pytest.raises(SchedulesResponseError) as info:
        await client.get_schedule("cleanup", namespace=None)

    assert info.value.data == {"scheduleId": "sch_123"}
