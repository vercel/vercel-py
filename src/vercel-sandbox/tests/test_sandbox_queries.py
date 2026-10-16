"""Iterating over sandboxes: filters, orderings, and pagination."""

from itertools import islice

import pytest
from fake_sandbox import FakeSandboxApi, wire
from pydantic import BaseModel, ValidationError
from sandbox_modes import Mode

from vercel import sandbox
from vercel.api import session
from vercel.errors import VercelSessionClosedError
from vercel.sandbox import (
    SandboxApiError,
    SandboxQuery,
    SandboxQueryByCreatedAt,
    SandboxQueryByCurrentSnapshotId,
    SandboxQueryByName,
    SandboxQueryByStatusUpdatedAt,
    TagFilter,
    sync as sandbox_sync,
)


def _seed_previews(fake_api: FakeSandboxApi) -> None:
    for name, env in [
        ("preview-1", "prod"),
        ("other", "prod"),
        ("preview-2", "prod"),
        ("preview-3", "dev"),
        ("preview-4", "prod"),
    ]:
        fake_api.seed(name, tags={"env": env})
        fake_api.clock.advance(1)


async def test_queries_filter_order_and_follow_cursors(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    _seed_previews(fake_api)
    query = mode.sandbox.SandboxQueryByName(
        sort_order="asc", name_prefix="preview", tag=mode.sandbox.TagFilter(key="env", value="prod")
    )

    async with mode.session():
        handles = await mode.collect(mode.sandbox.query_sandboxes(query=query, page_size=2))
        assert all(isinstance(handle, mode.handle_types("Sandbox")) for handle in handles)

    assert [handle.name for handle in handles] == ["preview-1", "preview-2", "preview-4"]
    first, second = (request.params.multi_items() for request in fake_api.requests)
    assert first == [
        ("teamId", "team_123"),
        ("project", "prj_123"),
        ("limit", "2"),
        ("sortBy", "name"),
        ("sortOrder", "asc"),
        ("namePrefix", "preview"),
        ("tags", "env:prod"),
    ]
    assert dict(second) == dict(first) | {"cursor": "2"}


async def test_a_cursor_resumes_a_query(mode: Mode, fake_api: FakeSandboxApi) -> None:
    _seed_previews(fake_api)
    query = mode.sandbox.SandboxQueryByCreatedAt(sort_order="asc")

    async with mode.session():
        resumed = await mode.collect(mode.sandbox.query_sandboxes(query=query, cursor="3"))

    assert [handle.name for handle in resumed] == ["preview-3", "preview-4"]
    assert fake_api.requests[0].params["cursor"] == "3"


async def test_an_unfiltered_query_sends_no_criteria(mode: Mode, fake_api: FakeSandboxApi) -> None:
    async with mode.session():
        assert await mode.collect(mode.sandbox.query_sandboxes()) == []

    assert dict(fake_api.requests[0].params) == {"teamId": "team_123", "project": "prj_123"}


@pytest.mark.parametrize(
    ("query", "expected", "names"),
    [
        (
            SandboxQueryByCreatedAt(tag=TagFilter(key="env", value="prod"), sort_order="asc"),
            {"sortBy": "createdAt", "sortOrder": "asc", "tags": "env:prod"},
            ["preview-1", "other", "preview-2", "preview-4"],
        ),
        (
            SandboxQueryByStatusUpdatedAt(sort_order="desc"),
            {"sortBy": "statusUpdatedAt", "sortOrder": "desc"},
            ["preview-4", "preview-3", "preview-2", "other", "preview-1"],
        ),
        (
            SandboxQueryByCurrentSnapshotId(sort_order="asc"),
            {"sortBy": "currentSnapshotId", "sortOrder": "asc"},
            None,
        ),
    ],
)
async def test_queries_encode_each_supported_ordering(
    fake_api: FakeSandboxApi,
    query: SandboxQuery,
    expected: dict[str, str],
    names: list[str] | None,
) -> None:
    _seed_previews(fake_api)

    async with fake_api.session():
        handles = [handle async for handle in sandbox.query_sandboxes(query=query)]

    assert dict(fake_api.requests[0].params) == {
        "teamId": "team_123",
        "project": "prj_123",
        **expected,
    }
    if names is not None:
        assert [handle.name for handle in handles] == names


@pytest.mark.parametrize(
    ("query_type", "kwargs"),
    [
        (SandboxQueryByCreatedAt, {"sort_order": "newest"}),
        (SandboxQueryByName, {"tags": [TagFilter(key="env", value="prod")]}),
        (SandboxQueryByStatusUpdatedAt, {"tag": TagFilter(key="env", value="prod")}),
        (SandboxQueryByCurrentSnapshotId, {"name_prefix": "preview"}),
    ],
)
def test_query_variants_reject_unsupported_combinations(
    query_type: type[BaseModel], kwargs: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        query_type(**kwargs)


async def test_queries_stop_fetching_when_the_consumer_stops(fake_api: FakeSandboxApi) -> None:
    _seed_previews(fake_api)

    async with fake_api.session():
        async for handle in sandbox.query_sandboxes(page_size=2):
            assert handle.name == "preview-4"
            break

    with fake_api.session(sync=True):
        names = [handle.name for handle in islice(sandbox_sync.query_sandboxes(page_size=2), 3)]

    assert names == ["preview-4", "preview-3", "preview-2"]
    assert [request.params.get("cursor") for request in fake_api.requests] == [None, None, "2"]


async def test_queries_validate_the_page_size(fake_api: FakeSandboxApi) -> None:
    async with fake_api.session():
        with pytest.raises(ValueError, match="page_size"):
            [handle async for handle in sandbox.query_sandboxes(page_size=51)]

    assert not fake_api.requests


def test_sync_queries_bind_the_session_they_were_created_in(fake_api: FakeSandboxApi) -> None:
    with fake_api.session(sync=True):
        handles = sandbox_sync.query_sandboxes()

    with pytest.raises(VercelSessionClosedError):
        next(handles)

    with session():
        orphaned = sandbox_sync.query_sandboxes()
    with pytest.raises(VercelSessionClosedError):
        next(orphaned)


async def test_api_errors_carry_status_code_and_body(fake_api: FakeSandboxApi) -> None:
    fake_api.fail("sandboxes.list", status=400, code="bad_request", message="unsupported filter")

    async with fake_api.session():
        with pytest.raises(SandboxApiError) as exc_info:
            [handle async for handle in sandbox.query_sandboxes()]

    assert exc_info.value.status_code == 400
    assert exc_info.value.code == "bad_request"
    assert exc_info.value.data == wire.error_payload("bad_request", "unsupported filter")
