"""Public surface over a mocked network, run against the async and sync APIs."""

import json
from typing import Any
from urllib.parse import quote

import httpx2 as httpx
import pytest
from conftest import DRIVES_URL, SCOPE, DriveApi, drive_json

import vendor.respx as respx
from vercel import drives
from vercel.api import session
from vercel.drives import (
    CURRENT_REGION,
    DEFAULT_DRIVE_API_BASE_URL,
    DriveApiError,
    DriveCredentialsError,
    DriveMount,
    DriveQuery,
    DriveQueryByCreatedAt,
    DriveQueryByName,
    DriveQueryByUpdatedAt,
    DriveResponseError,
    DriveServiceOptions,
    RegionInput,
    sync as drives_sync,
)
from vercel.errors import VercelError

CACHE_URL = f"{DRIVES_URL}/cache"


def drives_page(*names: str, cursor: str | None) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "drives": [drive_json(name=name)["drive"] for name in names],
            "pagination": {"count": len(names), "next": cursor},
        },
    )


def api_error(status_code: int, code: str, message: str = "nope") -> httpx.Response:
    return httpx.Response(status_code, json={"error": {"code": code, "message": message}})


@respx.mock
@pytest.mark.parametrize(("status_code", "created"), [(201, True), (200, False)])
async def test_get_or_create_returns_the_drive_and_whether_it_was_created(
    api: DriveApi, status_code: int, created: bool
) -> None:
    route = respx.post(CACHE_URL).mock(
        return_value=httpx.Response(status_code, json=drive_json(region="sfo1"))
    )

    async with api.session():
        cache, was_created = await api.call(
            "get_or_create_drive", name="cache", region="sfo1", max_size_bytes=1024**3
        )

    assert was_created is created
    assert isinstance(cache, api.handle_type)
    assert (cache.id, cache.name, cache.project_id) == ("drive_123", "cache", "prj_123")
    assert (cache.region, cache.max_size_bytes) == ("sfo1", 1024**3)
    assert (cache.current_session_id, cache.current_sandbox_name) == ("sbx_123", "preview")
    assert (cache.created_at, cache.updated_at) == (1, 2)
    assert (cache.parent_drive_id, cache.root_drive_id) == (None, None)
    assert cache.snapshot() == DriveMount(cache, mode="snapshot")
    with pytest.raises(AttributeError):
        cache.name = "other"

    request = route.calls.last.request
    assert request.headers["authorization"] == "Bearer token"
    assert request.headers["user-agent"].startswith("vercel-drives/")
    assert dict(request.url.params) == {"teamId": "team_123"}
    assert json.loads(request.content) == {
        "projectId": "prj_123",
        "maxSizeBytes": 1024**3,
        "region": "sfo1",
    }


@respx.mock
@pytest.mark.parametrize(
    ("option", "argument", "environment", "sent"),
    [
        (None, None, "sfo1", None),
        (None, "iad1", "sfo1", "iad1"),
        (None, CURRENT_REGION, "sfo1", "sfo1"),
        (None, CURRENT_REGION, None, None),
        ("cdg1", None, "sfo1", "cdg1"),
        (CURRENT_REGION, None, "sfo1", "sfo1"),
        ("cdg1", CURRENT_REGION, None, None),
    ],
)
async def test_region_is_sent_only_when_requested(
    api: DriveApi,
    monkeypatch: pytest.MonkeyPatch,
    option: RegionInput,
    argument: RegionInput,
    environment: str | None,
    sent: str | None,
) -> None:
    if environment is not None:
        monkeypatch.setenv("VERCEL_REGION", environment)
    route = respx.post(CACHE_URL).mock(return_value=httpx.Response(200, json=drive_json()))

    async with api.session(options=api.options(region=option)):
        await api.call("get_or_create_drive", name="cache", region=argument)

    assert json.loads(route.calls.last.request.content).get("region") == sent


@respx.mock
@pytest.mark.parametrize("name_or_id", ["cache", "drive_123", "cache/with space"])
async def test_get_drive_looks_up_by_name_or_id(api: DriveApi, name_or_id: str) -> None:
    route = respx.get(f"{DRIVES_URL}/{quote(name_or_id, safe='')}").mock(
        return_value=httpx.Response(200, json=drive_json())
    )

    async with api.session():
        found = await api.call("get_drive", name_or_id=name_or_id)

    assert isinstance(found, api.handle_type)
    assert found.id == "drive_123"
    assert dict(route.calls.last.request.url.params) == SCOPE


@respx.mock
@pytest.mark.parametrize(
    ("query", "sorting"),
    [
        (None, {}),
        (DriveQueryByCreatedAt(sort_order="asc"), {"sortBy": "createdAt", "sortOrder": "asc"}),
        (DriveQueryByUpdatedAt(), {"sortBy": "updatedAt", "sortOrder": "desc"}),
        (
            DriveQueryByName(name_prefix="cache-", sort_order="asc"),
            {"sortBy": "name", "sortOrder": "asc", "namePrefix": "cache-"},
        ),
    ],
)
async def test_query_drives_follows_cursors(
    api: DriveApi, query: DriveQuery | None, sorting: dict[str, str]
) -> None:
    route = respx.get(DRIVES_URL).mock(
        side_effect=[
            drives_page("cache-1", "cache-2", cursor="cursor_2"),
            drives_page("cache-3", cursor=None),
        ]
    )

    async with api.session():
        drives = await api.query(query=query, page_size=2)

    assert [item.name for item in drives] == ["cache-1", "cache-2", "cache-3"]
    assert all(isinstance(item, api.handle_type) for item in drives)
    assert [dict(call.request.url.params) for call in route.calls] == [
        {**SCOPE, "limit": "2", **sorting},
        {**SCOPE, "limit": "2", "cursor": "cursor_2", **sorting},
    ]


@respx.mock
async def test_query_drives_starts_at_a_cursor_and_stops_on_an_empty_page(api: DriveApi) -> None:
    route = respx.get(DRIVES_URL).mock(return_value=drives_page(cursor="loop"))

    async with api.session():
        assert await api.query(cursor="start") == []

    assert [dict(call.request.url.params) for call in route.calls] == [{**SCOPE, "cursor": "start"}]


@respx.mock
async def test_fork_returns_an_independent_handle(api: DriveApi) -> None:
    respx.get(CACHE_URL).mock(return_value=httpx.Response(200, json=drive_json()))
    fork_route = respx.post(f"{CACHE_URL}/fork").mock(
        side_effect=[
            httpx.Response(
                201,
                json=drive_json(
                    id=f"drive_{name}",
                    name=name,
                    parentDriveId="drive_123",
                    rootDriveId="drive_root",
                    currentSessionId=None,
                    currentSandboxName=None,
                ),
            )
            for name in ("from-handle", "from-name")
        ]
    )

    async with api.session():
        source = await api.call("get_drive", name_or_id="cache")
        from_handle = await api.call(source.fork, name="from-handle")
        from_name = await api.call("fork_drive", source="cache", name="from-name")

    for forked, name in ((from_handle, "from-handle"), (from_name, "from-name")):
        assert isinstance(forked, api.handle_type)
        assert (forked.id, forked.name) == (f"drive_{name}", name)
        assert (forked.parent_drive_id, forked.root_drive_id) == ("drive_123", "drive_root")
        assert forked.current_session_id is None
    assert (source.id, source.parent_drive_id) == ("drive_123", None)
    assert [json.loads(call.request.content) for call in fork_route.calls] == [
        {"name": "from-handle"},
        {"name": "from-name"},
    ]
    assert all(dict(call.request.url.params) == SCOPE for call in fork_route.calls)


@respx.mock
async def test_delete_refreshes_the_handle_from_the_response(api: DriveApi) -> None:
    respx.get(CACHE_URL).mock(return_value=httpx.Response(200, json=drive_json()))
    delete_route = respx.delete(CACHE_URL).mock(
        return_value=httpx.Response(
            200,
            json=drive_json(
                id="drive_replacement",
                currentSessionId=None,
                currentSandboxName=None,
                updatedAt=3,
            ),
        )
    )

    async with api.session():
        cache = await api.call("get_drive", name_or_id="cache")
        assert await api.call(cache.delete) is cache
        deleted = await api.call("delete_drive", name="cache")

    assert (cache.id, cache.current_session_id, cache.updated_at) == ("drive_replacement", None, 3)
    assert isinstance(deleted, api.handle_type)
    assert deleted.id == "drive_replacement"
    assert [dict(call.request.url.params) for call in delete_route.calls] == [SCOPE, SCOPE]


@respx.mock
async def test_delete_rejects_a_response_for_another_drive(api: DriveApi) -> None:
    respx.get(CACHE_URL).mock(return_value=httpx.Response(200, json=drive_json()))
    respx.delete(CACHE_URL).mock(return_value=httpx.Response(200, json=drive_json(name="other")))

    async with api.session():
        cache = await api.call("get_drive", name_or_id="cache")
        with pytest.raises(DriveResponseError, match="different name or project"):
            await api.call(cache.delete)

    assert cache.name == "cache"


@respx.mock
@pytest.mark.parametrize(
    ("status_code", "code"),
    [(404, "not_found"), (409, "conflict"), (409, "drive_not_initialized"), (403, "forbidden")],
)
async def test_api_errors_carry_the_status_code_and_body(
    api: DriveApi, status_code: int, code: str
) -> None:
    route = respx.post(f"{CACHE_URL}/fork").mock(
        return_value=api_error(status_code, code, "The request failed.")
    )

    async with api.session():
        with pytest.raises(DriveApiError, match="The request failed.") as exc_info:
            await api.call("fork_drive", source="cache", name="cache-fork")

    error = exc_info.value
    assert (error.status_code, error.code) == (status_code, code)
    assert error.data == {"error": {"code": code, "message": "The request failed."}}
    assert error.response.status_code == status_code
    assert route.call_count == 1


@respx.mock
@pytest.mark.parametrize(
    "failure",
    [
        api_error(500, "internal_server_error"),
        api_error(429, "rate_limit_exceeded"),
        httpx.ConnectError("connection refused"),
    ],
)
async def test_transient_failures_are_retried(
    api: DriveApi, delays: list[float], failure: httpx.Response | Exception
) -> None:
    route = respx.get(CACHE_URL).mock(
        side_effect=[failure, failure, httpx.Response(200, json=drive_json())]
    )

    async with api.session():
        assert (await api.call("get_drive", name_or_id="cache")).name == "cache"

    assert route.call_count == 3
    assert len(delays) == 2


@respx.mock
async def test_retries_stop_after_two_and_raise_the_last_error(
    api: DriveApi, delays: list[float]
) -> None:
    route = respx.get(CACHE_URL).mock(return_value=api_error(503, "service_unavailable"))

    async with api.session():
        with pytest.raises(DriveApiError) as exc_info:
            await api.call("get_drive", name_or_id="cache")

    assert exc_info.value.status_code == 503
    assert route.call_count == 3


@respx.mock
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="not json"),
        httpx.Response(200, json={"drives": []}),
        httpx.Response(200, json={"drive": {"id": "drive_123"}}),
    ],
)
async def test_malformed_success_responses_are_rejected(
    api: DriveApi, response: httpx.Response
) -> None:
    respx.get(CACHE_URL).mock(return_value=response)

    async with api.session():
        with pytest.raises(DriveResponseError):
            await api.call("get_drive", name_or_id="cache")


@respx.mock
async def test_invalid_arguments_fail_before_any_request(api: DriveApi) -> None:
    async with api.session():
        for name in ("", " ", ".", ".."):
            with pytest.raises(ValueError, match="not a valid Drive name"):
                await api.call("get_drive", name_or_id=name)
            with pytest.raises(ValueError, match="not a valid Drive name"):
                await api.call("fork_drive", source="cache", name=name)
        for page_size, error in ((0, ValueError), (51, ValueError), ("5", TypeError)):
            with pytest.raises(error, match="page_size"):
                await api.query(page_size=page_size)

    assert not respx.calls


@respx.mock
async def test_default_credentials_come_from_the_environment(
    api: DriveApi, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_TOKEN", "env-token")
    monkeypatch.setenv("VERCEL_TEAM_ID", "team_env")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_env")
    route = respx.get(f"{DEFAULT_DRIVE_API_BASE_URL}/v1/drives/cache").mock(
        return_value=httpx.Response(200, json=drive_json())
    )

    async with api.session(options=api.module.DriveServiceOptions()):
        await api.call("get_drive", name_or_id="cache")

    request = route.calls.last.request
    assert request.headers["authorization"] == "Bearer env-token"
    assert dict(request.url.params) == {"teamId": "team_env", "projectId": "prj_env"}


@respx.mock
async def test_missing_credentials_raise_before_any_request(api: DriveApi) -> None:
    async with api.session(options=api.module.DriveServiceOptions()):
        with pytest.raises(DriveCredentialsError):
            await api.call("get_drive", name_or_id="cache")

    assert not respx.calls


def test_async_options_do_not_configure_a_sync_session(mock_env_clear: None) -> None:
    with session(service_options=[DriveServiceOptions()]):
        with pytest.raises(VercelError, match="use SyncDriveServiceOptions"):
            drives_sync.get_drive(name_or_id="cache")


@pytest.mark.parametrize("module", [drives, drives_sync])
def test_drive_mount_serializes_a_handle_or_name_as_the_drive_name(module: Any) -> None:
    class Handle:
        name = "cache"
        project_id = "prj_123"
        region = "iad1"

    assert module.DriveMount(Handle()).model_dump() == {"drive": "cache", "mode": "read-write"}
    assert module.DriveMount("cache", mode="snapshot").model_dump() == {
        "drive": "cache",
        "mode": "snapshot",
    }
    with pytest.raises(ValueError, match="snapshot.*read-write"):
        module.DriveMount("cache", mode="read-only")
