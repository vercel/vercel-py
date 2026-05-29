import asyncio
import gc
import json
from collections.abc import AsyncGenerator, AsyncIterator, Generator
from itertools import islice
from typing import cast

import httpx
import pytest
import respx
from pydantic import BaseModel, ValidationError

from vercel import unstable as vercel
from vercel._internal.unstable.errors import VercelSessionClosedError
from vercel._internal.unstable.sandbox.options import SandboxCredentials
from vercel._internal.unstable.sandbox.state import SandboxRuntimeSessionState, SandboxState
from vercel._internal.unstable.session import get_active_session
from vercel.unstable import sandbox
from vercel.unstable.sandbox import (
    GitSource,
    SandboxApiError,
    SandboxCleanupError,
    SandboxCommandLogStream,
    SandboxQuery,
    SandboxQueryByCreatedAt,
    SandboxQueryByCurrentSnapshotId,
    SandboxQueryByName,
    SandboxQueryByStatusUpdatedAt,
    SandboxResources,
    SandboxResponseError,
    SandboxServiceOptions,
    SandboxSource,
    SandboxStatus,
    SandboxStreamError,
    SandboxTerminalStateError,
    SnapshotRetention,
    SnapshotSource,
    TagFilter,
    TarballSource,
    sync as sandbox_sync,
)


def _sandbox_response(
    *,
    name: str = "preview",
    session_id: str = "sbx_123",
    status: str = "running",
    session_status: str | None = None,
) -> dict[str, object]:
    return {
        "sandbox": {
            "name": name,
            "currentSessionId": session_id,
            "status": status,
            "persistent": True,
            "runtime": "python3.13",
            "createdAt": 1,
            "updatedAt": 2,
        },
        "session": {
            "id": session_id,
            "sourceSandboxName": name,
            "projectId": "prj_123",
            "status": session_status or status,
            "runtime": "python3.13",
            "cwd": "/vercel/sandbox",
            "memory": 2048,
            "vcpus": 1,
            "timeout": 300000,
            "requestedAt": 1,
        },
        "routes": [
            {
                "url": "https://preview.sandbox.test",
                "subdomain": "preview",
                "port": 3000,
                "system": False,
            }
        ],
    }


def _command_response(
    *,
    command_id: str = "cmd_123",
    session_id: str = "sbx_123",
    exit_code: int | None = None,
) -> dict[str, object]:
    return {
        "command": {
            "id": command_id,
            "name": "python",
            "args": ["--version"],
            "cwd": "/vercel/sandbox",
            "sessionId": session_id,
            "exitCode": exit_code,
            "startedAt": 1,
        }
    }


def _snapshot_response(
    *,
    snapshot_id: str = "snap_123",
    session_id: str = "sbx_123",
    status: str = "created",
) -> dict[str, object]:
    return {
        "snapshot": {
            "id": snapshot_id,
            "sourceSessionId": session_id,
            "region": "iad1",
            "status": status,
            "sizeBytes": 1024,
            "createdAt": 1,
            "updatedAt": 2,
        }
    }


def _session_options(*, base_url: str = "https://sandbox.test") -> list[SandboxServiceOptions]:
    async def credentials_factory() -> SandboxCredentials:
        return SandboxCredentials(
            token="token",
            team_id="team_123",
            project_id="prj_123",
        )

    return [
        SandboxServiceOptions(
            base_url=base_url,
            credentials_factory=credentials_factory,
        )
    ]


def _logs_response(*records: object) -> httpx.Response:
    return httpx.Response(
        200,
        text="\n".join(
            record if isinstance(record, str) else json.dumps(record) for record in records
        )
        + "\n",
    )


class _PendingLogStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.waiting = asyncio.Event()
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'{"stream": "stdout", "data": "partial\\n"}\n'
        self.waiting.set()
        await asyncio.Event().wait()

    async def aclose(self) -> None:
        self.closed = True


@respx.mock
async def test_public_create_sandbox_encodes_protocol_and_observed_state(
    mock_env_clear: None,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/v2/sandboxes"
        assert dict(request.url.params) == {"teamId": "team_123"}
        assert request.headers["authorization"] == "Bearer token"
        assert request.headers["user-agent"].startswith("vercel/unstable/sandbox/")
        assert "Python/" in request.headers["user-agent"]
        assert json.loads(request.content) == {
            "projectId": "prj_123",
            "name": "preview",
            "runtime": "python3.13",
            "source": {
                "type": "git",
                "url": "https://github.com/vercel/vercel-py",
                "revision": "main",
            },
            "timeout": 120000,
            "resources": {"vcpus": 2, "memory": 4096},
            "snapshotExpiration": 300000,
            "keepLastSnapshots": {
                "count": 3,
                "expiration": 600000,
                "deleteEvicted": False,
            },
            "tags": {"env": "test"},
        }
        response = _sandbox_response()
        payload = response["sandbox"]
        assert isinstance(payload, dict)
        payload["tags"] = {"env": "test"}
        return httpx.Response(200, json=response)

    route = respx.post("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(
            name="preview",
            runtime="python3.13",
            source=GitSource(
                url="https://github.com/vercel/vercel-py",
                revision="main",
            ),
            execution_time_limit=120_000,
            resources=SandboxResources(vcpus=2, memory=4096),
            snapshot_expiration=300_000,
            snapshot_retention=SnapshotRetention(
                count=3,
                expiration=600_000,
                delete_evicted=False,
            ),
            tags={"env": "test"},
        )

        with pytest.raises(AttributeError):
            handle.status = SandboxStatus.STOPPED  # type: ignore[misc]
        assert handle.tags is not None
        handle.tags["env"] = "mutated"

    assert route.called
    assert handle.status is SandboxStatus.RUNNING
    assert handle.tags == {"env": "test"}
    assert handle.current_session is not None
    assert handle.current_session.project_id == "prj_123"
    assert handle.routes[0].url == "https://preview.sandbox.test"
    assert not hasattr(handle, "model_dump")


@respx.mock
@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            GitSource(url="https://github.com/vercel/vercel-py"),
            {"type": "git", "url": "https://github.com/vercel/vercel-py"},
        ),
        (
            TarballSource(url="https://example.com/source.tar.gz"),
            {"type": "tarball", "url": "https://example.com/source.tar.gz"},
        ),
        (
            SnapshotSource(snapshot_id="snap_123"),
            {"type": "snapshot", "snapshotId": "snap_123"},
        ),
    ],
)
async def test_public_create_sandbox_serializes_source_variants(
    mock_env_clear: None,
    source: SandboxSource,
    expected: dict[str, str],
) -> None:
    route = respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    async with vercel.session(service_options=_session_options()):
        await sandbox.create_sandbox(name="preview", runtime="python3.13", source=source)

    assert json.loads(route.calls.last.request.content)["source"] == expected


@respx.mock
async def test_public_create_rejects_malformed_success_response(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(return_value=httpx.Response(200, json={}))

    async with vercel.session(service_options=_session_options()):
        with pytest.raises(SandboxResponseError):
            await sandbox.create_sandbox(name="preview", runtime="python3.13")


@respx.mock
async def test_public_create_rejects_terminal_initial_state(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )

    async with vercel.session(service_options=_session_options()):
        with pytest.raises(SandboxTerminalStateError) as exc_info:
            await sandbox.create_sandbox(name="preview", runtime="python3.13")

    assert exc_info.value.status is SandboxStatus.STOPPED
    assert isinstance(exc_info.value.sandbox, sandbox.Sandbox)
    assert exc_info.value.sandbox.name == "preview"


@respx.mock
def test_sync_create_terminal_error_contains_sync_handle(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )

    with vercel.session(service_options=_session_options()):
        with pytest.raises(SandboxTerminalStateError) as exc_info:
            sandbox_sync.create_sandbox(name="preview", runtime="python3.13")

    assert exc_info.value.status is SandboxStatus.STOPPED
    assert isinstance(exc_info.value.sandbox, sandbox_sync.SyncSandbox)
    assert exc_info.value.sandbox.name == "preview"


@respx.mock
async def test_service_returns_neutral_state_and_async_runtime_binds_handles(
    mock_env_clear: None,
) -> None:
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/snapshot").mock(
        return_value=httpx.Response(
            201,
            json={**_snapshot_response(), "session": _sandbox_response()["session"]},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json={"sandboxes": [_sandbox_response()["sandbox"]], "pagination": {"count": 1}},
        )
    )

    async with vercel.session(service_options=_session_options()):
        service = get_active_session().sandbox_service()
        state = await service.get_sandbox(name="preview")
        assert isinstance(state, SandboxState)
        assert isinstance(state.current_session, SandboxRuntimeSessionState)
        page_state = await service.query_sandboxes_page()
        assert isinstance(page_state.sandboxes[0], SandboxState)

        handle = await sandbox.get_sandbox(name="preview")
        assert isinstance(handle, sandbox.Sandbox)
        assert isinstance(handle.current_session, sandbox.SandboxRuntimeSession)
        assert isinstance(await handle.start_command("python"), sandbox.SandboxCommand)
        assert isinstance(await handle.snapshot(), sandbox.Snapshot)
        page = [item async for item in sandbox.query_sandboxes()]
        assert isinstance(page[0], sandbox.Sandbox)


@respx.mock
def test_sync_runtime_binds_only_sync_handles(mock_env_clear: None) -> None:
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/snapshot").mock(
        return_value=httpx.Response(
            201,
            json={**_snapshot_response(), "session": _sandbox_response()["session"]},
        )
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.get_sandbox(name="preview")
        assert isinstance(handle, sandbox_sync.SyncSandbox)
        assert isinstance(handle.current_session, sandbox_sync.SyncSandboxRuntimeSession)
        assert isinstance(handle.start_command("python"), sandbox_sync.SyncSandboxCommand)
        assert isinstance(handle.snapshot(), sandbox_sync.SyncSnapshot)


@respx.mock
async def test_session_closure_during_create_polling_is_rejected(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="pending", session_status="pending"),
        )
    )

    async with vercel.session(service_options=_session_options()):
        session = get_active_session()
        operation = asyncio.create_task(
            session.sandbox_service().create_sandbox(name="preview", runtime="python3.13")
        )
        await asyncio.sleep(0)
        await session.aclose()

        with pytest.raises(VercelSessionClosedError):
            await operation


@respx.mock
async def test_query_sandboxes_paginates_and_encodes_filters(mock_env_clear: None) -> None:
    first_page = {
        "sandboxes": [
            _sandbox_response(name="preview-1")["sandbox"],
            _sandbox_response(name="preview-2")["sandbox"],
        ],
        "pagination": {"count": 3, "next": "cursor_2", "prev": None},
    }
    second_page = {
        "sandboxes": [_sandbox_response(name="preview-3")["sandbox"]],
        "pagination": {"count": 3, "next": None, "prev": "cursor_1"},
    }
    requests: list[list[tuple[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params.multi_items()
        requests.append(params)
        if request.url.params.get("cursor") == "cursor_2":
            return httpx.Response(200, json=second_page)
        return httpx.Response(200, json=first_page)

    respx.get("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)

    async with vercel.session(service_options=_session_options()):
        handles = [
            handle
            async for handle in sandbox.query_sandboxes(
                query=SandboxQueryByName(
                    name_prefix="preview",
                    tag=TagFilter(key="env", value="prod"),
                ),
                page_size=2,
                cursor="cursor_1",
            )
        ]

    assert [handle.name for handle in handles] == ["preview-1", "preview-2", "preview-3"]
    assert requests == [
        [
            ("teamId", "team_123"),
            ("project", "prj_123"),
            ("limit", "2"),
            ("cursor", "cursor_1"),
            ("sortBy", "name"),
            ("sortOrder", "desc"),
            ("namePrefix", "preview"),
            ("tags", "env:prod"),
        ],
        [
            ("teamId", "team_123"),
            ("project", "prj_123"),
            ("limit", "2"),
            ("cursor", "cursor_2"),
            ("sortBy", "name"),
            ("sortOrder", "desc"),
            ("namePrefix", "preview"),
            ("tags", "env:prod"),
        ],
    ]


@respx.mock
async def test_query_sandboxes_without_query_omits_criteria(mock_env_clear: None) -> None:
    route = respx.get("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json={"sandboxes": [], "pagination": {"count": 0, "next": None, "prev": None}},
        )
    )

    async with vercel.session(service_options=_session_options()):
        assert [item async for item in sandbox.query_sandboxes()] == []

    assert dict(route.calls[0].request.url.params) == {
        "teamId": "team_123",
        "project": "prj_123",
    }


@respx.mock
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            SandboxQueryByCreatedAt(tag=TagFilter(key="env", value="prod"), sort_order="asc"),
            {"sortBy": "createdAt", "sortOrder": "asc", "tags": "env:prod"},
        ),
        (
            SandboxQueryByStatusUpdatedAt(sort_order="desc"),
            {"sortBy": "statusUpdatedAt", "sortOrder": "desc"},
        ),
        (
            SandboxQueryByCurrentSnapshotId(sort_order="asc"),
            {"sortBy": "currentSnapshotId", "sortOrder": "asc"},
        ),
    ],
)
async def test_query_sandboxes_encodes_supported_orderings(
    mock_env_clear: None,
    query: SandboxQuery,
    expected: dict[str, str],
) -> None:
    route = respx.get("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json={"sandboxes": [], "pagination": {"count": 0, "next": None, "prev": None}},
        )
    )

    async with vercel.session(service_options=_session_options()):
        assert [item async for item in sandbox.query_sandboxes(query=query)] == []

    params = dict(route.calls[0].request.url.params)
    assert params == {"teamId": "team_123", "project": "prj_123", **expected}


@pytest.mark.parametrize(
    ("query_type", "kwargs"),
    [
        (SandboxQueryByCreatedAt, {"sort_order": "newest"}),
        (SandboxQueryByName, {"tags": [TagFilter(key="env", value="prod")]}),
        (SandboxQueryByStatusUpdatedAt, {"tag": TagFilter(key="env", value="prod")}),
        (SandboxQueryByCurrentSnapshotId, {"name_prefix": "preview"}),
    ],
)
def test_sandbox_query_variants_reject_unsupported_combinations(
    query_type: type[BaseModel], kwargs: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        query_type(**kwargs)


@respx.mock
async def test_query_sandboxes_stops_when_consumer_breaks(mock_env_clear: None) -> None:
    route = respx.get("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json={
                "sandboxes": [
                    _sandbox_response(name="preview-1")["sandbox"],
                    _sandbox_response(name="preview-2")["sandbox"],
                ],
                "pagination": {"count": 2, "next": "cursor_2", "prev": None},
            },
        )
    )
    handles = []

    async with vercel.session(service_options=_session_options()):
        async for handle in sandbox.query_sandboxes(page_size=2):
            handles.append(handle)
            break

    assert [handle.name for handle in handles] == ["preview-1"]
    assert route.call_count == 1


async def test_query_sandboxes_rejects_invalid_page_size(mock_env_clear: None) -> None:
    async with vercel.session(service_options=_session_options()):
        with pytest.raises(ValueError, match="page_size"):
            [handle async for handle in sandbox.query_sandboxes(page_size=51)]


@respx.mock
async def test_public_api_error_propagates_status_code_code_and_data(mock_env_clear: None) -> None:
    data = {"error": {"code": "bad_request", "message": "unsupported filter"}}
    respx.get("https://sandbox.test/v2/sandboxes").mock(return_value=httpx.Response(400, json=data))

    async with vercel.session(service_options=_session_options()):
        with pytest.raises(SandboxApiError) as exc_info:
            [item async for item in sandbox.query_sandboxes()]

    assert exc_info.value.status_code == 400
    assert exc_info.value.code == "bad_request"
    assert exc_info.value.data == data


@respx.mock
async def test_create_sandbox_operation_invariants(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    async with vercel.session(service_options=_session_options()):
        operation = sandbox.create_sandbox(name="preview", runtime="python3.13")
        await operation
        with pytest.raises(RuntimeError, match="can only be used once"):
            await operation

    async with vercel.session():
        captured = sandbox.create_sandbox(name="preview", runtime="python3.13")

    with pytest.raises(VercelSessionClosedError):
        await captured

    with pytest.warns(RuntimeWarning, match="never awaited or entered"):
        unconsumed = sandbox.create_sandbox(name="preview", runtime="python3.13")
        del unconsumed
        gc.collect()


@respx.mock
async def test_closed_session_rejects_handles_and_lazy_logs(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/snapshot").mock(
        return_value=httpx.Response(
            201,
            json={**_snapshot_response(), "session": _sandbox_response()["session"]},
        )
    )

    async with vercel.session(service_options=_session_options()):
        service = get_active_session().sandbox_service()
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = await handle.session()
        command = await handle.start_command("sleep", ["30"])
        logs = command.logs()
        snapshot = await handle.snapshot()

    with pytest.raises(VercelSessionClosedError):
        await handle.start_command("true")
    with pytest.raises(VercelSessionClosedError):
        await runtime_session.start_command("true")
    with pytest.raises(VercelSessionClosedError):
        await command.refresh()
    with pytest.raises(VercelSessionClosedError):
        await anext(logs)
    with pytest.raises(VercelSessionClosedError):
        await snapshot.delete()
    with pytest.raises(VercelSessionClosedError):
        await service.get_sandbox(name="preview")


@respx.mock
async def test_async_command_logs_cache_order_refresh_and_closed_reads(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    responses = iter(
        [
            _logs_response(
                {"stream": "stdout", "data": "out-1\n"},
                {"stream": "stderr", "data": "err\n"},
                {"stream": "stdout", "data": "out-2\n"},
            ),
            _logs_response({"stream": "stdout", "data": "fresh\n"}),
        ]
    )
    route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_123/logs").mock(
        side_effect=lambda _request: next(responses)
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        command = await handle.start_command("python", ["--version"])
        uncached = await handle.start_command("python", ["--version"])

        logs = [(event.stream, event.data) async for event in command.logs()]
        assert logs[0][0] is SandboxCommandLogStream.STDOUT
        assert logs[1][0] is SandboxCommandLogStream.STDERR
        assert logs == [
            ("stdout", "out-1\n"),
            ("stderr", "err\n"),
            ("stdout", "out-2\n"),
        ]
        assert await command.output() == "out-1\nerr\nout-2\n"
        assert await command.stdout() == "out-1\nout-2\n"
        assert await command.stderr() == "err\n"
        assert route.call_count == 1

        refreshed = [(event.stream, event.data) async for event in command.logs(refresh=True)]
        assert refreshed == [("stdout", "fresh\n")]
        assert await command.output() == "fresh\n"
        assert route.call_count == 2

    assert await command.output() == "fresh\n"
    assert [(event.stream, event.data) async for event in command.logs()] == [("stdout", "fresh\n")]
    with pytest.raises(VercelSessionClosedError):
        await command.logs(refresh=True).__anext__()
    with pytest.raises(VercelSessionClosedError):
        await uncached.output()


@respx.mock
async def test_async_command_logs_skip_invalid_records_and_do_not_cache_failures(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    responses = iter(
        [
            _logs_response(
                "not-json",
                {"stream": "stdin", "data": "ignored"},
                {"stream": "stdout", "data": "before\n"},
                {
                    "stream": "error",
                    "data": {"code": "sandbox_stopped", "message": "session stopped"},
                },
            ),
            _logs_response(
                "still-not-json",
                {"stream": "unexpected", "data": "ignored"},
                {"stream": "stdout", "data": "retried\n"},
            ),
        ]
    )
    route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_123/logs").mock(
        side_effect=lambda _request: next(responses)
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        command = await handle.start_command("python", ["--version"])

        events = command.logs()
        first = await anext(events)
        assert first.stream is SandboxCommandLogStream.STDOUT
        assert (first.stream, first.data) == ("stdout", "before\n")
        with pytest.raises(SandboxStreamError, match="session stopped") as exc_info:
            await anext(events)
        assert exc_info.value.code == "sandbox_stopped"

        assert await command.output() == "retried\n"
        assert await command.output() == "retried\n"
        assert route.call_count == 2


@respx.mock
async def test_async_command_logs_close_and_refresh_generation_do_not_commit_stale_data(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    responses = iter(
        [
            _logs_response(
                {"stream": "stdout", "data": "partial\n"},
                {"stream": "stdout", "data": "discarded\n"},
            ),
            _logs_response(
                {"stream": "stdout", "data": "old-1\n"},
                {"stream": "stderr", "data": "old-2\n"},
            ),
            _logs_response({"stream": "stdout", "data": "fresh\n"}),
        ]
    )
    route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_123/logs").mock(
        side_effect=lambda _request: next(responses)
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        command = await handle.start_command("python", ["--version"])

        partial = cast(AsyncGenerator[sandbox.SandboxCommandLog, None], command.logs())
        assert (await anext(partial)).data == "partial\n"
        await partial.aclose()

        stale = command.logs()
        assert (await anext(stale)).data == "old-1\n"
        assert [(event.stream, event.data) async for event in command.logs(refresh=True)] == [
            ("stdout", "fresh\n")
        ]
        assert [(event.stream, event.data) async for event in stale] == [("stderr", "old-2\n")]
        assert await command.output() == "fresh\n"
        assert route.call_count == 3


@respx.mock
async def test_async_cancelled_command_logs_close_stream_without_caching(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    pending = _PendingLogStream()
    responses = iter(
        [
            httpx.Response(200, stream=pending),
            _logs_response({"stream": "stdout", "data": "retried\n"}),
        ]
    )
    route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_123/logs").mock(
        side_effect=lambda _request: next(responses)
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        command = await handle.start_command("python", ["--version"])

        output = asyncio.create_task(command.output())
        await pending.waiting.wait()
        output.cancel()
        with pytest.raises(asyncio.CancelledError):
            await output

        assert pending.closed
        assert await command.output() == "retried\n"
        assert route.call_count == 2


@respx.mock
async def test_async_context_cleanup_wraps_api_failure(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            500,
            json={"error": {"code": "sandbox_failed", "message": "delete failed"}},
        )
    )

    async with vercel.session(service_options=_session_options()):
        with pytest.raises(SandboxCleanupError) as exc_info:
            async with sandbox.create_sandbox(name="preview", runtime="python3.13"):
                pass

    assert exc_info.value.resource_type == "sandbox"
    assert exc_info.value.resource_id == "preview"
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "sandbox_failed"


@respx.mock
def test_sync_context_cleanup_wraps_api_failure(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            500,
            json={"error": {"code": "sandbox_failed", "message": "delete failed"}},
        )
    )

    with vercel.session(service_options=_session_options()):
        with pytest.raises(SandboxCleanupError) as exc_info:
            with sandbox_sync.create_sandbox(name="preview", runtime="python3.13"):
                pass

    assert exc_info.value.resource_type == "sandbox"
    assert exc_info.value.resource_id == "preview"
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "sandbox_failed"


@respx.mock
async def test_destroyed_async_handles_continue_issuing_requests(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(status="stopped", session_status="stopped")
        )
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        assert await handle.destroy() is handle
        assert handle.status is SandboxStatus.STOPPED
        assert (await handle.start_command("python", ["--version"])).id == "cmd_123"

    assert command_route.called


@respx.mock
async def test_stopped_runtime_session_continues_issuing_requests(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/stop").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(
                session_id="sbx_runtime", status="stopped", session_status="stopped"
            ),
        )
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(session_id="sbx_runtime"))
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = await handle.session()
        assert await runtime_session.stop() is runtime_session
        assert (await runtime_session.start_command("python", ["--version"])).id == "cmd_123"

    assert command_route.called


@respx.mock
def test_destroyed_sync_handles_continue_issuing_requests(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(status="stopped", session_status="stopped")
        )
    )
    session_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        assert handle.destroy() is handle
        assert handle.status is SandboxStatus.STOPPED
        assert handle.session().id == "sbx_runtime"

    assert session_route.called


@respx.mock
def test_stopped_sync_runtime_session_continues_issuing_requests(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/stop").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(
                session_id="sbx_runtime", status="stopped", session_status="stopped"
            ),
        )
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(session_id="sbx_runtime"))
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = handle.session()
        assert runtime_session.stop() is runtime_session
        assert runtime_session.start_command("python", ["--version"]).id == "cmd_123"

    assert command_route.called


@respx.mock
def test_sync_command_logs_cache_order_refresh_and_closed_reads(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    responses = iter(
        [
            _logs_response(
                {"stream": "stdout", "data": "out-1\n"},
                {"stream": "stderr", "data": "err\n"},
                {"stream": "stdout", "data": "out-2\n"},
            ),
            _logs_response({"stream": "stdout", "data": "fresh\n"}),
        ]
    )
    route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_123/logs").mock(
        side_effect=lambda _request: next(responses)
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        command = handle.start_command("python", ["--version"])
        uncached = handle.start_command("python", ["--version"])

        logs = [(event.stream, event.data) for event in command.logs()]
        assert logs[0][0] is SandboxCommandLogStream.STDOUT
        assert logs[1][0] is SandboxCommandLogStream.STDERR
        assert logs == [
            ("stdout", "out-1\n"),
            ("stderr", "err\n"),
            ("stdout", "out-2\n"),
        ]
        assert command.output() == "out-1\nerr\nout-2\n"
        assert command.stdout() == "out-1\nout-2\n"
        assert command.stderr() == "err\n"
        assert route.call_count == 1

        assert [(event.stream, event.data) for event in command.logs(refresh=True)] == [
            ("stdout", "fresh\n")
        ]
        assert command.output() == "fresh\n"
        assert route.call_count == 2

    assert command.output() == "fresh\n"
    with pytest.raises(VercelSessionClosedError):
        next(command.logs(refresh=True))
    with pytest.raises(VercelSessionClosedError):
        uncached.output()


@respx.mock
def test_sync_log_iterators_are_lazy_and_stream_errors_do_not_cache(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    responses = iter(
        [
            _logs_response(
                "{",
                {"stream": "error", "data": {"code": "terminated", "message": "terminated"}},
            ),
            _logs_response(
                "still-not-json",
                {"stream": "unexpected", "data": "ignored"},
                {"stream": "stdout", "data": "retried\n"},
            ),
        ]
    )
    route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_123/logs").mock(
        side_effect=lambda _request: next(responses)
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        command = handle.start_command("python", ["--version"])
        lazy_command_logs = handle.start_command("python", ["--version"]).logs()
        assert handle.current_session is not None
        logs = command.logs()
        raw_logs = handle.current_session.command_logs(command.id)
        assert route.call_count == 0

        with pytest.raises(SandboxStreamError, match="terminated") as exc_info:
            next(logs)
        assert exc_info.value.code == "terminated"
        assert command.output() == "retried\n"
        assert route.call_count == 2

    with pytest.raises(VercelSessionClosedError):
        next(lazy_command_logs)
    with pytest.raises(VercelSessionClosedError):
        next(raw_logs)
    assert route.call_count == 2


@respx.mock
def test_sync_command_logs_close_and_refresh_generation_do_not_commit_stale_data(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    responses = iter(
        [
            _logs_response(
                {"stream": "stdout", "data": "partial\n"},
                {"stream": "stderr", "data": "discarded\n"},
            ),
            _logs_response(
                {"stream": "stdout", "data": "old-1\n"},
                {"stream": "stderr", "data": "old-2\n"},
            ),
            _logs_response({"stream": "stdout", "data": "fresh\n"}),
        ]
    )
    route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_123/logs").mock(
        side_effect=lambda _request: next(responses)
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        command = handle.start_command("python", ["--version"])

        partial = cast(Generator[sandbox.SandboxCommandLog, None, None], command.logs())
        assert next(partial).data == "partial\n"
        partial.close()

        stale = command.logs()
        assert next(stale).data == "old-1\n"
        assert [(event.stream, event.data) for event in command.logs(refresh=True)] == [
            ("stdout", "fresh\n")
        ]
        assert [(event.stream, event.data) for event in stale] == [("stderr", "old-2\n")]
        assert command.output() == "fresh\n"
        assert route.call_count == 3


@respx.mock
async def test_mutating_handles_reject_mismatched_response_identity(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.patch("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="other"))
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_123").mock(
        return_value=httpx.Response(200, json=_command_response(command_id="other"))
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/snapshot").mock(
        return_value=httpx.Response(
            201,
            json={**_snapshot_response(), "session": _sandbox_response()["session"]},
        )
    )
    respx.delete("https://sandbox.test/v2/sandboxes/snapshots/snap_123").mock(
        return_value=httpx.Response(200, json=_snapshot_response(snapshot_id="other"))
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(SandboxResponseError):
            await handle.update(runtime="node22")

        command = await handle.start_command("python", ["--version"])
        with pytest.raises(SandboxResponseError):
            await command.refresh()

        snapshot = await handle.snapshot()
        with pytest.raises(SandboxResponseError):
            await snapshot.delete()


def test_sync_query_sandboxes_binds_session_at_iterator_creation(mock_env_clear: None) -> None:
    with vercel.session(service_options=_session_options()):
        handles = sandbox_sync.query_sandboxes()

    with pytest.raises(VercelSessionClosedError):
        next(handles)


@respx.mock
def test_sync_query_sandboxes_paginates_and_supports_early_consumers(
    mock_env_clear: None,
) -> None:
    first_page = {
        "sandboxes": [
            _sandbox_response(name="preview-1")["sandbox"],
            _sandbox_response(name="preview-2")["sandbox"],
        ],
        "pagination": {"count": 3, "next": "cursor_2", "prev": None},
    }
    second_page = {
        "sandboxes": [_sandbox_response(name="preview-3")["sandbox"]],
        "pagination": {"count": 3, "next": None, "prev": "cursor_1"},
    }
    requests: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        requests.append(params)
        if params.get("cursor") == "cursor_2":
            return httpx.Response(200, json=second_page)
        return httpx.Response(200, json=first_page)

    respx.get("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)

    with vercel.session(service_options=_session_options()):
        handles = list(
            islice(
                sandbox_sync.query_sandboxes(
                    query=sandbox_sync.SandboxQueryByName(
                        sort_order="asc",
                        name_prefix="preview",
                        tag=sandbox_sync.TagFilter(key="env", value="prod"),
                    ),
                    page_size=2,
                    cursor="cursor_1",
                ),
                3,
            )
        )

    assert [handle.name for handle in handles] == ["preview-1", "preview-2", "preview-3"]
    assert requests == [
        {
            "teamId": "team_123",
            "project": "prj_123",
            "limit": "2",
            "cursor": "cursor_1",
            "sortBy": "name",
            "sortOrder": "asc",
            "namePrefix": "preview",
            "tags": "env:prod",
        },
        {
            "teamId": "team_123",
            "project": "prj_123",
            "limit": "2",
            "cursor": "cursor_2",
            "sortBy": "name",
            "sortOrder": "asc",
            "namePrefix": "preview",
            "tags": "env:prod",
        },
    ]
