import gc
import json
import tarfile
from io import BytesIO
from itertools import islice

import httpx
import pytest
import respx

from vercel import unstable as vercel
from vercel._internal.unstable.context import get_active_session
from vercel._internal.unstable.errors import VercelSessionClosedError
from vercel._internal.unstable.sandbox.models import JSONValue
from vercel._internal.unstable.sandbox.options import SandboxCredentials
from vercel.unstable import sandbox
from vercel.unstable.sandbox import (
    GitSource,
    SandboxApiError,
    SandboxCleanupError,
    SandboxResources,
    SandboxServiceOptions,
    SandboxSource,
    SandboxStatus,
    Snapshot,
    SnapshotRetention,
    SnapshotSource,
    TagFilter,
    TarballSource,
    WriteFile,
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
    session_id: str = "sbx_runtime",
    command: str = "python",
    args: list[str] | None = None,
    exit_code: int | None = 0,
) -> dict[str, object]:
    return {
        "command": {
            "id": command_id,
            "name": command,
            "args": args or ["--version"],
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
            "expiresAt": 1_800_000_000_000,
            "createdAt": 1_700_000_000_000,
            "updatedAt": 1_700_000_000_001,
            "lastUsedAt": 1_700_000_000_002,
            "creationMethod": "manual",
            "parentId": None,
        }
    }


def _extract_tar_files(content: bytes) -> dict[str, tuple[tarfile.TarInfo, bytes]]:
    files: dict[str, tuple[tarfile.TarInfo, bytes]] = {}
    with tarfile.open(fileobj=BytesIO(content), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            extracted = tar.extractfile(member)
            if extracted is not None:
                files[member.name] = (member, extracted.read())
    return files


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


@respx.mock
async def test_public_create_sandbox_uses_session_options(mock_env_clear: None) -> None:
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
        }
        return httpx.Response(200, json=_sandbox_response())

    route = respx.post("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")

    assert route.called
    assert handle.name == "preview"
    assert handle.status is SandboxStatus.RUNNING
    assert handle.execution_time_limit == 300000
    assert handle.current_session is not None
    assert handle.current_session.project_id == "prj_123"
    assert handle.current_session.execution_time_limit == 300000
    assert handle.routes[0].url == "https://preview.sandbox.test"


@respx.mock
async def test_public_create_sandbox_serializes_typed_inputs(mock_env_clear: None) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
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
        return httpx.Response(200, json=_sandbox_response())

    route = respx.post("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)

    async with vercel.session(service_options=_session_options()):
        await sandbox.create_sandbox(
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

    assert route.called


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
        await sandbox.create_sandbox(
            name="preview",
            runtime="python3.13",
            source=source,
        )

    assert json.loads(route.calls.last.request.content)["source"] == expected


@respx.mock
async def test_query_sandboxes_serializes_tag_filters(mock_env_clear: None) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params.multi_items() == [
            ("teamId", "team_123"),
            ("project", "prj_123"),
            ("tags", "env:prod"),
            ("tags", "team:api"),
        ]
        return httpx.Response(200, json={"sandboxes": [_sandbox_response()["sandbox"]]})

    route = respx.get("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)

    async with vercel.session(service_options=_session_options()):
        handles = [
            handle
            async for handle in sandbox.query_sandboxes(
                tags=[
                    TagFilter(key="env", value="prod"),
                    TagFilter(key="team", value="api"),
                ]
            )
        ]

    assert route.called
    assert handles[0].name == "preview"


@respx.mock
async def test_query_sandboxes_stamps_session_project_id_on_handles(
    mock_env_clear: None,
) -> None:
    route = respx.get("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json={"sandboxes": [_sandbox_response()["sandbox"]]})
    )

    async with vercel.session(service_options=_session_options()):
        handles = [handle async for handle in sandbox.query_sandboxes()]

    assert route.called
    assert handles[0].project_id == "prj_123"


@respx.mock
async def test_query_sandboxes_preserves_backend_project_id(
    mock_env_clear: None,
) -> None:
    response_sandbox = _sandbox_response()["sandbox"]
    assert isinstance(response_sandbox, dict)
    listed_sandbox = dict(response_sandbox)
    listed_sandbox["projectId"] = "backend_project"
    respx.get("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json={"sandboxes": [listed_sandbox]})
    )

    async with vercel.session(service_options=_session_options()):
        handles = [handle async for handle in sandbox.query_sandboxes()]

    assert handles[0].project_id == "backend_project"


@respx.mock
async def test_query_sandboxes_stamps_explicit_project_id_on_handles(
    mock_env_clear: None,
) -> None:
    requests: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(dict(request.url.params))
        return httpx.Response(200, json={"sandboxes": [_sandbox_response()["sandbox"]]})

    respx.get("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)

    async with vercel.session(service_options=_session_options()):
        handles = [handle async for handle in sandbox.query_sandboxes(project_id="custom_project")]

    assert handles[0].project_id == "custom_project"
    assert requests == [
        {
            "teamId": "team_123",
            "project": "custom_project",
        }
    ]


@respx.mock
async def test_query_sandboxes_uses_page_size_and_iterates_pages(
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

    async with vercel.session(service_options=_session_options()):
        handles = [
            handle
            async for handle in sandbox.query_sandboxes(
                page_size=2,
                cursor="cursor_1",
                sort_by="createdAt",
                sort_order="desc",
                name_prefix="preview",
            )
        ]

    assert [handle.name for handle in handles] == ["preview-1", "preview-2", "preview-3"]
    assert requests == [
        {
            "teamId": "team_123",
            "project": "prj_123",
            "limit": "2",
            "cursor": "cursor_1",
            "sortBy": "createdAt",
            "sortOrder": "desc",
            "namePrefix": "preview",
        },
        {
            "teamId": "team_123",
            "project": "prj_123",
            "limit": "2",
            "cursor": "cursor_2",
            "sortBy": "createdAt",
            "sortOrder": "desc",
            "namePrefix": "preview",
        },
    ]


@respx.mock
async def test_query_sandboxes_caller_can_limit_results_by_breaking(
    mock_env_clear: None,
) -> None:
    page = {
        "sandboxes": [
            _sandbox_response(name="preview-1")["sandbox"],
            _sandbox_response(name="preview-2")["sandbox"],
        ],
        "pagination": {"count": 2, "next": "cursor_2", "prev": None},
    }
    requests: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(dict(request.url.params))
        return httpx.Response(200, json=page)

    respx.get("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)

    handles = []
    async with vercel.session(service_options=_session_options()):
        async for handle in sandbox.query_sandboxes(page_size=2):
            handles.append(handle)
            if len(handles) == 1:
                break

    assert [handle.name for handle in handles] == ["preview-1"]
    assert requests == [
        {
            "teamId": "team_123",
            "project": "prj_123",
            "limit": "2",
        }
    ]


async def test_query_sandboxes_rejects_invalid_page_size(mock_env_clear: None) -> None:
    async with vercel.session(service_options=_session_options()):
        with pytest.raises(ValueError, match="page_size"):
            [handle async for handle in sandbox.query_sandboxes(page_size=51)]


@respx.mock
async def test_query_sandboxes_surfaces_backend_multi_tag_error(mock_env_clear: None) -> None:
    route = respx.get("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(
            400,
            json={
                "error": {
                    "code": "bad_request",
                    "message": "multiple tag filters are not supported",
                }
            },
        )
    )

    async with vercel.session(service_options=_session_options()):
        with pytest.raises(SandboxApiError) as exc_info:
            [
                handle
                async for handle in sandbox.query_sandboxes(
                    tags=[
                        TagFilter(key="env", value="prod"),
                        TagFilter(key="team", value="api"),
                    ]
                )
            ]

    assert route.called
    assert exc_info.value.status_code == 400
    assert exc_info.value.code == "bad_request"


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
async def test_closed_session_rejects_handles_lazy_logs_and_captured_service(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(exit_code=None))
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/snapshot").mock(
        return_value=httpx.Response(
            201,
            json={
                **_snapshot_response(snapshot_id="snap_closed"),
                "session": _sandbox_response()["session"],
            },
        )
    )

    async with vercel.session(service_options=_session_options()):
        sdk_session = get_active_session()
        service = sdk_session.sandbox_service()
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = await handle.session()
        command = await handle.start_command("sleep", ["30"])
        pending_logs = command.logs()
        snapshot = await handle.snapshot()

    assert sdk_session.is_closed
    with pytest.raises(VercelSessionClosedError):
        await handle.start_command("true")
    with pytest.raises(VercelSessionClosedError):
        await runtime_session.start_command("true")
    with pytest.raises(VercelSessionClosedError):
        await command.refresh()
    with pytest.raises(VercelSessionClosedError):
        await anext(pending_logs)
    with pytest.raises(VercelSessionClosedError):
        await snapshot.delete()
    with pytest.raises(VercelSessionClosedError):
        await service.get_sandbox(name="preview")


@respx.mock
async def test_context_cleanup_preserves_handle_and_surfaces_cleanup_failures(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    destroy_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )
    session_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )

    async with vercel.session(service_options=_session_options()):
        async with sandbox.create_sandbox(name="preview", runtime="python3.13") as handle:
            assert handle.name == "preview"

        assert destroy_route.called
        runtime_session = await handle.session()
        assert runtime_session.id == "sbx_runtime"
        assert session_route.called

    respx.post("https://cleanup.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.delete("https://cleanup.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            500,
            json={"error": {"code": "sandbox_failed", "message": "delete failed"}},
        )
    )

    async with vercel.session(service_options=_session_options(base_url="https://cleanup.test")):
        with pytest.raises(SandboxCleanupError) as exc_info:
            async with sandbox.create_sandbox(name="preview", runtime="python3.13"):
                pass

    assert exc_info.value.resource_type == "sandbox"
    assert exc_info.value.resource_id == "preview"
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "sandbox_failed"


@respx.mock
async def test_explicit_destroy_leaves_server_to_answer_later_operations(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    destroy_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(exit_code=None))
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        destroyed = await handle.destroy()

        assert destroyed.status is SandboxStatus.STOPPED
        command = await handle.start_command("python", ["--version"])
        assert command.id == "cmd_123"

    assert destroy_route.called
    assert command_route.called


@respx.mock
async def test_explicit_runtime_session_stop_leaves_server_to_answer_later_operations(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/stop").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(
                session_id="sbx_runtime",
                status="stopped",
                session_status="stopped",
            ),
        )
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(exit_code=None))
    )
    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = await handle.session()
        stopped = await runtime_session.stop()

        assert stopped.status is SandboxStatus.STOPPED
        command = await runtime_session.start_command("python", ["--version"])
        assert command.id == "cmd_123"

    assert stop_route.called
    assert command_route.called


@respx.mock
async def test_child_runtime_session_usable_after_parent_sandbox_context_exit(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(exit_code=None))
    )
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )

    async with vercel.session(service_options=_session_options()):
        async with sandbox.create_sandbox(name="preview", runtime="python3.13") as handle:
            runtime_session = await handle.session()

        command = await runtime_session.start_command("python", ["--version"])
        assert command.id == "cmd_123"

    assert command_route.called


@respx.mock
async def test_runtime_session_context_cleanup_does_not_invalidate_handles(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(exit_code=None))
    )
    current_command_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd"
    ).mock(return_value=httpx.Response(200, json=_command_response(exit_code=None)))
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/stop").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(
                session_id="sbx_runtime",
                status="stopped",
                session_status="stopped",
            ),
        )
    )
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )

    async with vercel.session(service_options=_session_options()):
        async with sandbox.create_sandbox(name="preview", runtime="python3.13") as handle:
            assert handle.current_session is not None
            async with handle.session() as runtime_session:
                pass

            assert (await runtime_session.start_command("python", ["--version"])).id == "cmd_123"

        assert (await handle.current_session.start_command("python", ["--version"])).id == "cmd_123"

    assert command_route.called
    assert current_command_route.called


@respx.mock
async def test_command_handle_lifecycle_supports_logs_lookup_list_and_kill(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    start_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(
            200,
            json=_command_response(
                command_id="cmd_sleep",
                session_id="sbx_123",
                command="sh",
                args=["-c", "printf hi; sleep 30"],
                exit_code=None,
            ),
        )
    )
    logs_route = respx.get(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_sleep/logs"
    ).mock(
        return_value=httpx.Response(
            200,
            content=b'{"data":"hi","stream":"stdout"}\n',
            headers={"content-type": "application/x-ndjson"},
        )
    )
    get_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_sleep").mock(
        return_value=httpx.Response(
            200,
            json=_command_response(
                command_id="cmd_sleep",
                session_id="sbx_123",
                command="sh",
                args=["-c", "printf hi; sleep 30"],
                exit_code=0,
            ),
        )
    )
    list_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(
            200,
            json={
                "commands": [
                    _command_response(command_id="cmd_sleep", session_id="sbx_123")["command"]
                ]
            },
        )
    )
    kill_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_sleep/kill"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_command_response(
                command_id="cmd_sleep",
                session_id="sbx_123",
                command="sh",
                args=["-c", "printf hi; sleep 30"],
                exit_code=None,
            ),
        )
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        command = await handle.start_command(
            "sh",
            ["-c", "printf hi; sleep 30"],
        )

        assert command.id == "cmd_sleep"
        assert command.status == "running"
        assert len(get_route.calls) == 0
        assert await command.output("stdout") == "hi"
        fetched = await handle.get_command("cmd_sleep")
        assert fetched.id == command.id
        finished = await command.wait()
        assert finished.exit_code == 0
        assert [item.id for item in await handle.query_commands()] == ["cmd_sleep"]
        killed = await command.kill("TERM")

    assert start_route.called
    assert logs_route.called
    assert get_route.called
    assert [dict(call.request.url.params) for call in get_route.calls] == [
        {"teamId": "team_123", "wait": "false"},
        {"teamId": "team_123", "wait": "true"},
    ]
    assert list_route.called
    assert kill_route.called
    assert json.loads(kill_route.calls.last.request.content) == {"signal": 15}
    assert killed.id == "cmd_sleep"


@respx.mock
async def test_sandbox_context_preserves_command_handles_and_lazy_logs(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    start_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(
            200,
            json=_command_response(command_id="cmd_context", session_id="sbx_123", exit_code=None),
        )
    )
    get_route = respx.get(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_context"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_command_response(command_id="cmd_context", session_id="sbx_123"),
        )
    )
    list_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(
            200,
            json={
                "commands": [
                    _command_response(command_id="cmd_context", session_id="sbx_123")["command"]
                ]
            },
        )
    )
    kill_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_context/kill"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_command_response(command_id="cmd_context", session_id="sbx_123"),
        )
    )
    logs_route = respx.get(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_context/logs"
    ).mock(return_value=httpx.Response(200, content=b""))
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )

    async with vercel.session(service_options=_session_options()):
        async with sandbox.create_sandbox(name="preview", runtime="python3.13") as handle:
            ran = await handle.run_command("true")
            started = await handle.start_command("sleep", ["30"])
            fetched = await handle.get_command("cmd_context")
            queried = (await handle.query_commands())[0]
            refreshed = await started.refresh()
            waited = await started.wait()
            killed = await started.kill()
            pending_logs = started.logs()

        get_call_count = get_route.call_count
        for command in [ran, started, fetched, queried, refreshed, waited, killed]:
            assert (await command.refresh()).id == "cmd_context"
        with pytest.raises(StopAsyncIteration):
            await anext(pending_logs)

    assert start_route.call_count == 2
    assert list_route.called
    assert kill_route.called
    assert get_route.call_count == get_call_count + 7
    assert logs_route.called


@respx.mock
async def test_runtime_session_context_preserves_command_producer_results(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    start_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(
            200,
            json=_command_response(
                command_id="cmd_context", session_id="sbx_runtime", exit_code=None
            ),
        )
    )
    get_route = respx.get(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd/cmd_context"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_command_response(command_id="cmd_context", session_id="sbx_runtime"),
        )
    )
    list_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(
            200,
            json={
                "commands": [
                    _command_response(command_id="cmd_context", session_id="sbx_runtime")["command"]
                ]
            },
        )
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/stop").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(
                session_id="sbx_runtime",
                status="stopped",
                session_status="stopped",
            ),
        )
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        async with handle.session() as runtime_session:
            ran = await runtime_session.run_command("true")
            started = await runtime_session.start_command("sleep", ["30"])
            fetched = await runtime_session.get_command("cmd_context")
            queried = (await runtime_session.query_commands())[0]

        get_call_count = get_route.call_count
        for command in [ran, started, fetched, queried]:
            assert (await command.refresh()).id == "cmd_context"

    assert start_route.call_count == 2
    assert list_route.called
    assert get_route.call_count == get_call_count + 4


@respx.mock
async def test_sandbox_filesystem_workflow_uses_public_handles(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    mkdir_requests: list[dict[str, object]] = []
    write_requests: list[httpx.Request] = []
    read_requests: list[dict[str, object]] = []

    def mkdir_handler(request: httpx.Request) -> httpx.Response:
        mkdir_requests.append(json.loads(request.content))
        return httpx.Response(200, json={})

    def write_handler(request: httpx.Request) -> httpx.Response:
        write_requests.append(request)
        return httpx.Response(200, json={})

    def read_handler(request: httpx.Request) -> httpx.Response:
        read_requests.append(json.loads(request.content))
        return httpx.Response(200, content=b"result: 42\n")

    mkdir_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/fs/mkdir").mock(
        side_effect=mkdir_handler
    )
    write_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/fs/write").mock(
        side_effect=write_handler
    )
    read_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/fs/read").mock(
        side_effect=read_handler
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        await handle.mkdir("work/output", recursive=True)
        await handle.write_files(
            [
                WriteFile(
                    path="work/script.py",
                    content="print('result: 42')\n",
                    mode=0o644,
                ),
                WriteFile(path="work/data.bin", content=b"\x00\x01"),
            ]
        )
        content = await handle.read_file("work/output/result.txt")
        text = await handle.read_text("work/output/result.txt")

    assert content == b"result: 42\n"
    assert text == "result: 42\n"
    assert mkdir_route.called
    assert mkdir_requests == [{"path": "work/output", "recursive": True}]
    assert write_route.called
    write_request = write_requests[0]
    assert write_request.headers["content-type"] == "application/gzip"
    assert write_request.headers["x-cwd"] == "/"
    files = _extract_tar_files(write_request.content)
    assert set(files) == {
        "vercel/sandbox/work/script.py",
        "vercel/sandbox/work/data.bin",
    }
    script_info, script_content = files["vercel/sandbox/work/script.py"]
    _, data_content = files["vercel/sandbox/work/data.bin"]
    assert script_info.mode == 0o644
    assert script_content == b"print('result: 42')\n"
    assert data_content == b"\x00\x01"
    assert read_route.called
    assert read_requests == [{"path": "work/output/result.txt"}, {"path": "work/output/result.txt"}]


@respx.mock
async def test_session_controls_and_sandbox_update_public_flow(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    patch_requests: list[dict[str, object]] = []

    def patch_handler(request: httpx.Request) -> httpx.Response:
        patch_requests.append(json.loads(request.content))
        assert dict(request.url.params) == {"teamId": "team_123", "projectId": "prj_123"}
        return httpx.Response(200, json=_sandbox_response())

    update_route = respx.patch("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=patch_handler
    )
    extended_session = _sandbox_response()["session"]
    assert isinstance(extended_session, dict)
    extend_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/extend-timeout"
    ).mock(
        return_value=httpx.Response(
            200,
            json={"session": {**extended_session, "timeout": 420000}},
        )
    )
    network_policy_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/network-policy"
    ).mock(
        return_value=httpx.Response(
            200,
            json={"session": _sandbox_response()["session"]},
        )
    )
    get_session_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123").mock(
        return_value=httpx.Response(
            200,
            json={"session": _sandbox_response()["session"], "routes": []},
        )
    )
    list_sessions_route = respx.get("https://sandbox.test/v2/sandboxes/sessions").mock(
        return_value=httpx.Response(
            200,
            json={
                "sessions": [
                    _sandbox_response()["session"],
                    _sandbox_response(session_id="sbx_old", status="stopped")["session"],
                ],
                "pagination": {"count": 2, "next": None},
            },
        )
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        updated = await handle.update(
            ports=[3000],
            execution_time_limit=300000,
            resources=SandboxResources(vcpus=2, memory=4096),
            tags={"env": "test"},
            persistent=True,
        )
        extended = await handle.extend_execution_time_limit(120000)
        policy: JSONValue = {"mode": "allow-all"}
        policy_updated = await handle.update_network_policy(policy)
        assert handle.current_session is not None
        refreshed = await handle.current_session.refresh()
        session_extended = await handle.current_session.extend_execution_time_limit(30_000)
        handle_sessions = await handle.list_sessions(page_size=2, sort_order="asc")
        project_sessions = [
            item
            async for item in sandbox.query_sessions(
                project_id="prj_123",
                name="preview",
                page_size=2,
                sort_order="asc",
            )
        ]

    assert updated.name == "preview"
    assert extended.execution_time_limit == 420000
    assert session_extended.execution_time_limit == 420000
    assert policy_updated.id == "sbx_123"
    assert refreshed.id == "sbx_123"
    assert [item.id for item in handle_sessions] == ["sbx_123", "sbx_old"]
    assert [item.id for item in project_sessions] == ["sbx_123", "sbx_old"]
    assert update_route.called
    assert patch_requests == [
        {
            "ports": [3000],
            "timeout": 300000,
            "resources": {"vcpus": 2, "memory": 4096},
            "persistent": True,
            "tags": {"env": "test"},
        }
    ]
    assert [json.loads(call.request.content) for call in extend_route.calls] == [
        {"duration": 120000},
        {"duration": 30000},
    ]
    assert json.loads(network_policy_route.calls.last.request.content) == {"mode": "allow-all"}
    assert get_session_route.called
    assert [dict(call.request.url.params) for call in list_sessions_route.calls] == [
        {
            "teamId": "team_123",
            "project": "prj_123",
            "name": "preview",
            "limit": "2",
            "sortOrder": "asc",
        },
        {
            "teamId": "team_123",
            "project": "prj_123",
            "name": "preview",
            "limit": "2",
            "sortOrder": "asc",
        },
    ]


@respx.mock
async def test_snapshot_restore_public_flow(mock_env_clear: None) -> None:
    create_requests: list[dict[str, object]] = []

    def create_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        create_requests.append(body)
        name = str(body["name"])
        return httpx.Response(200, json=_sandbox_response(name=name))

    create_route = respx.post("https://sandbox.test/v2/sandboxes").mock(side_effect=create_handler)
    write_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/fs/write").mock(
        return_value=httpx.Response(200, json={})
    )
    snapshot_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/snapshot").mock(
        return_value=httpx.Response(
            201,
            json={
                **_snapshot_response(snapshot_id="snap_restore"),
                "session": _sandbox_response(status="stopped", session_status="stopped")["session"],
            },
        )
    )
    list_route = respx.get("https://sandbox.test/v2/sandboxes/snapshots").mock(
        return_value=httpx.Response(
            200,
            json={
                "snapshots": [_snapshot_response(snapshot_id="snap_restore")["snapshot"]],
                "pagination": {"count": 1, "next": None},
            },
        )
    )
    get_route = respx.get("https://sandbox.test/v2/sandboxes/snapshots/snap_restore").mock(
        return_value=httpx.Response(200, json=_snapshot_response(snapshot_id="snap_restore"))
    )
    read_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/fs/read").mock(
        return_value=httpx.Response(200, content=b"from snapshot\n")
    )
    delete_route = respx.delete("https://sandbox.test/v2/sandboxes/snapshots/snap_restore").mock(
        return_value=httpx.Response(
            200,
            json=_snapshot_response(snapshot_id="snap_restore", status="deleted"),
        )
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(exit_code=None))
    )

    async with vercel.session(service_options=_session_options()):
        base = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        await base.write_files([WriteFile(path="state.txt", content="from snapshot\n")])
        created = await base.snapshot(expiration=0)
        assert base.current_session is not None
        assert (await base.current_session.start_command("python", ["--version"])).id == "cmd_123"

        restored = await sandbox.create_sandbox(
            name="restored",
            runtime="python3.13",
            source=SnapshotSource(snapshot_id=created.id),
        )
        assert await restored.read_text("state.txt") == "from snapshot\n"

        listed_from_handle = await base.list_snapshots(page_size=10)
        listed_from_module = [
            item
            async for item in sandbox.query_snapshots(
                project_id="prj_123",
                name="preview",
                page_size=10,
            )
        ]
        fetched = await sandbox.get_snapshot(snapshot_id=created.id)
        deleted = await created.delete()
        assert (await created.delete()).status == "deleted"

    assert create_route.call_count == 2
    assert create_requests[1]["source"] == {"type": "snapshot", "snapshotId": "snap_restore"}
    assert write_route.called
    assert json.loads(snapshot_route.calls.last.request.content) == {"expiration": 0}
    assert [snapshot.id for snapshot in listed_from_handle] == ["snap_restore"]
    assert [snapshot.id for snapshot in listed_from_module] == ["snap_restore"]
    assert isinstance(fetched, Snapshot)
    assert fetched.id == "snap_restore"
    assert deleted.status == "deleted"
    assert [dict(call.request.url.params) for call in list_route.calls] == [
        {
            "teamId": "team_123",
            "project": "prj_123",
            "name": "preview",
            "limit": "10",
        },
        {
            "teamId": "team_123",
            "project": "prj_123",
            "name": "preview",
            "limit": "10",
        },
    ]
    assert get_route.called
    assert read_route.called
    assert delete_route.call_count == 2
    assert command_route.called


@respx.mock
def test_sync_session_controls_and_sandbox_update_public_flow(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    update_route = respx.patch("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    extended_session = _sandbox_response()["session"]
    assert isinstance(extended_session, dict)
    extend_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/extend-timeout"
    ).mock(
        return_value=httpx.Response(
            200,
            json={"session": {**extended_session, "timeout": 420000}},
        )
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        updated = handle.update(execution_time_limit=300000)
        extended = handle.extend_execution_time_limit(120000)
        assert handle.current_session is not None
        session_extended = handle.current_session.extend_execution_time_limit(30_000)

    assert updated.name == "preview"
    assert extended.execution_time_limit == 420000
    assert session_extended.execution_time_limit == 420000
    assert json.loads(update_route.calls.last.request.content) == {"timeout": 300000}
    assert [json.loads(call.request.content) for call in extend_route.calls] == [
        {"duration": 120000},
        {"duration": 30000},
    ]


@respx.mock
def test_sync_snapshot_create_list_get_delete_parity(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    snapshot_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/snapshot").mock(
        return_value=httpx.Response(
            201,
            json={
                **_snapshot_response(snapshot_id="snap_sync"),
                "session": _sandbox_response(status="stopped", session_status="stopped")["session"],
            },
        )
    )
    list_route = respx.get("https://sandbox.test/v2/sandboxes/snapshots").mock(
        return_value=httpx.Response(
            200,
            json={
                "snapshots": [_snapshot_response(snapshot_id="snap_sync")["snapshot"]],
                "pagination": {"count": 1, "next": None},
            },
        )
    )
    get_route = respx.get("https://sandbox.test/v2/sandboxes/snapshots/snap_sync").mock(
        return_value=httpx.Response(200, json=_snapshot_response(snapshot_id="snap_sync"))
    )
    delete_route = respx.delete("https://sandbox.test/v2/sandboxes/snapshots/snap_sync").mock(
        return_value=httpx.Response(
            200,
            json=_snapshot_response(snapshot_id="snap_sync", status="deleted"),
        )
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(exit_code=None))
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        created = handle.snapshot(expiration=0)
        assert handle.current_session is not None
        assert handle.current_session.start_command("python", ["--version"]).id == "cmd_123"
        listed = handle.list_snapshots(page_size=10)
        project_listed = list(
            sandbox_sync.query_snapshots(project_id="prj_123", name="preview", page_size=10)
        )
        fetched = sandbox_sync.get_snapshot(snapshot_id=created.id)
        deleted = created.delete()
        assert created.delete().status == "deleted"

    assert created.id == "snap_sync"
    assert [snapshot.id for snapshot in listed] == ["snap_sync"]
    assert [snapshot.id for snapshot in project_listed] == ["snap_sync"]
    assert fetched.id == "snap_sync"
    assert deleted.status == "deleted"
    assert json.loads(snapshot_route.calls.last.request.content) == {"expiration": 0}
    assert list_route.call_count == 2
    assert get_route.called
    assert delete_route.call_count == 2
    assert command_route.called


@respx.mock
def test_sync_create_runtime_command_and_cleanup_parity(mock_env_clear: None) -> None:
    create_route = respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    runtime_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    start_command_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd"
    ).mock(return_value=httpx.Response(200, json=_command_response(exit_code=None)))
    wait_command_route = respx.get(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd/cmd_123"
    ).mock(return_value=httpx.Response(200, json=_command_response(exit_code=0)))
    stop_runtime_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/stop"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(
                session_id="sbx_runtime",
                status="stopped",
                session_status="stopped",
            ),
        )
    )
    destroy_sandbox_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )

    with vercel.session(service_options=_session_options()):
        with sandbox_sync.create_sandbox(
            name="preview",
            runtime="python3.13",
            execution_time_limit=60_000,
            resources=SandboxResources(vcpus=2, memory=4096),
        ) as handle:
            with handle.session() as runtime_session:
                result = runtime_session.run_command("python", ["--version"])

            assert result.exit_code == 0
            assert runtime_session.start_command("python", ["--version"]).id == "cmd_123"

        assert handle.session().id == "sbx_runtime"

    assert create_route.called
    create_body = json.loads(create_route.calls.last.request.content)
    assert create_body["timeout"] == 60000
    assert create_body["resources"] == {"vcpus": 2, "memory": 4096}
    assert runtime_route.call_count == 2
    assert start_command_route.call_count == 2
    assert wait_command_route.called
    assert stop_runtime_route.called
    assert destroy_sandbox_route.called


@respx.mock
def test_sync_command_handle_lifecycle_supports_lookup_list_and_kill(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    start_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(
            200,
            json=_command_response(
                command_id="cmd_sleep",
                session_id="sbx_123",
                command="sleep",
                args=["30"],
                exit_code=None,
            ),
        )
    )
    logs_route = respx.get(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_sleep/logs"
    ).mock(
        return_value=httpx.Response(
            200,
            stream=httpx.ByteStream(
                b'{"data":"one\\n","stream":"stdout"}\n{"data":"two\\n","stream":"stderr"}\n'
            ),
            headers={"content-type": "application/x-ndjson"},
        )
    )
    get_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_sleep").mock(
        return_value=httpx.Response(
            200,
            json=_command_response(
                command_id="cmd_sleep",
                session_id="sbx_123",
                command="sleep",
                args=["30"],
                exit_code=0,
            ),
        )
    )
    list_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(
            200,
            json={
                "commands": [
                    _command_response(command_id="cmd_sleep", session_id="sbx_123")["command"]
                ]
            },
        )
    )
    kill_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_sleep/kill"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_command_response(
                command_id="cmd_sleep",
                session_id="sbx_123",
                command="sleep",
                args=["30"],
                exit_code=None,
            ),
        )
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        command = handle.start_command("sleep", ["30"])

        assert command.id == "cmd_sleep"
        assert command.status == "running"
        assert len(get_route.calls) == 0
        events = list(command.logs())
        assert [(event.stream, event.data) for event in events] == [
            ("stdout", "one\n"),
            ("stderr", "two\n"),
        ]
        assert handle.get_command("cmd_sleep").id == command.id
        assert command.wait().exit_code == 0
        assert [item.id for item in handle.query_commands()] == ["cmd_sleep"]
        killed = command.kill(9)

    assert start_route.called
    assert logs_route.called
    assert get_route.called
    assert [dict(call.request.url.params) for call in get_route.calls] == [
        {"teamId": "team_123", "wait": "false"},
        {"teamId": "team_123", "wait": "true"},
    ]
    assert list_route.called
    assert kill_route.called
    assert json.loads(kill_route.calls.last.request.content) == {"signal": 9}
    assert killed.id == "cmd_sleep"


@respx.mock
def test_sync_sandbox_context_preserves_command_handles(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    start_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(
            200,
            json=_command_response(command_id="cmd_context", session_id="sbx_123", exit_code=None),
        )
    )
    get_route = respx.get(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_context"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_command_response(command_id="cmd_context", session_id="sbx_123"),
        )
    )
    list_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(
            200,
            json={
                "commands": [
                    _command_response(command_id="cmd_context", session_id="sbx_123")["command"]
                ]
            },
        )
    )
    kill_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_context/kill"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_command_response(command_id="cmd_context", session_id="sbx_123"),
        )
    )
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )

    with vercel.session(service_options=_session_options()):
        with sandbox_sync.create_sandbox(name="preview", runtime="python3.13") as handle:
            ran = handle.run_command("true")
            started = handle.start_command("sleep", ["30"])
            fetched = handle.get_command("cmd_context")
            queried = handle.query_commands()[0]
            refreshed = started.refresh()
            waited = started.wait()
            killed = started.kill()

        get_call_count = get_route.call_count
        for command in [ran, started, fetched, queried, refreshed, waited, killed]:
            assert command.refresh().id == "cmd_context"

    assert start_route.call_count == 2
    assert list_route.called
    assert kill_route.called
    assert get_route.call_count == get_call_count + 7


@respx.mock
def test_sync_runtime_session_context_preserves_all_command_producer_results(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    start_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(
            200,
            json=_command_response(
                command_id="cmd_context", session_id="sbx_runtime", exit_code=None
            ),
        )
    )
    get_route = respx.get(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd/cmd_context"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_command_response(command_id="cmd_context", session_id="sbx_runtime"),
        )
    )
    list_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(
            200,
            json={
                "commands": [
                    _command_response(command_id="cmd_context", session_id="sbx_runtime")["command"]
                ]
            },
        )
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/stop").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(
                session_id="sbx_runtime",
                status="stopped",
                session_status="stopped",
            ),
        )
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        with handle.session() as runtime_session:
            ran = runtime_session.run_command("true")
            started = runtime_session.start_command("sleep", ["30"])
            fetched = runtime_session.get_command("cmd_context")
            queried = runtime_session.query_commands()[0]

        get_call_count = get_route.call_count
        for command in [ran, started, fetched, queried]:
            assert command.refresh().id == "cmd_context"

    assert start_route.call_count == 2
    assert list_route.called
    assert get_route.call_count == get_call_count + 4


@respx.mock
def test_sync_runtime_session_filesystem_parity(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    mkdir_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/fs/mkdir"
    ).mock(return_value=httpx.Response(200, json={}))
    write_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/fs/write"
    ).mock(return_value=httpx.Response(200, json={}))
    read_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/fs/read").mock(
        return_value=httpx.Response(200, content=b"sync content")
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = handle.session()
        runtime_session.mkdir("tmp", cwd="/vercel/sandbox", recursive=False)
        runtime_session.write_files(
            [sandbox_sync.WriteFile(path="tmp/file.txt", content="sync content", mode=0o600)],
            cwd="/vercel/sandbox",
        )
        content = runtime_session.read_file("tmp/file.txt", cwd="/vercel/sandbox")
        text = runtime_session.read_text("tmp/file.txt", cwd="/vercel/sandbox")

    assert content == b"sync content"
    assert text == "sync content"
    assert json.loads(mkdir_route.calls.last.request.content) == {
        "path": "tmp",
        "cwd": "/vercel/sandbox",
        "recursive": False,
    }
    assert write_route.calls.last.request.headers["x-cwd"] == "/"
    files = _extract_tar_files(write_route.calls.last.request.content)
    assert set(files) == {"vercel/sandbox/tmp/file.txt"}
    info, data = files["vercel/sandbox/tmp/file.txt"]
    assert info.mode == 0o600
    assert data == b"sync content"
    assert [json.loads(call.request.content) for call in read_route.calls] == [
        {
            "path": "tmp/file.txt",
            "cwd": "/vercel/sandbox",
        },
        {
            "path": "tmp/file.txt",
            "cwd": "/vercel/sandbox",
        },
    ]


@respx.mock
def test_sync_runtime_session_command_logs_parity(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    logs_route = respx.get(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd/cmd_123/logs"
    ).mock(
        return_value=httpx.Response(
            200,
            stream=httpx.ByteStream(
                b'{"data":"one\\n","stream":"stdout"}\n{"data":"two\\n","stream":"stderr"}\n'
            ),
            headers={"content-type": "application/x-ndjson"},
        )
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = handle.session()
        events = list(runtime_session.command_logs("cmd_123"))

    assert [(event.stream, event.data) for event in events] == [
        ("stdout", "one\n"),
        ("stderr", "two\n"),
    ]
    assert logs_route.called


@respx.mock
def test_sync_explicit_destroy_leaves_server_to_answer_later_operations(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    destroy_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )
    session_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        destroyed = handle.destroy()

        assert destroyed.status is SandboxStatus.STOPPED
        assert handle.session().id == "sbx_runtime"

    assert destroy_route.called
    assert session_route.called


@respx.mock
def test_sync_explicit_runtime_session_stop_leaves_server_to_answer_later_operations(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/stop").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(
                session_id="sbx_runtime",
                status="stopped",
                session_status="stopped",
            ),
        )
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(exit_code=None))
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = handle.session()
        stopped = runtime_session.stop()

        assert stopped.status is SandboxStatus.STOPPED
        assert runtime_session.start_command("python", ["--version"]).id == "cmd_123"

    assert stop_route.called
    assert command_route.called


@respx.mock
def test_sync_child_runtime_session_usable_after_parent_sandbox_context_exit(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_runtime"))
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(exit_code=None))
    )
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )

    with vercel.session(service_options=_session_options()):
        with sandbox_sync.create_sandbox(name="preview", runtime="python3.13") as handle:
            runtime_session = handle.session()

        assert runtime_session.start_command("python", ["--version"]).id == "cmd_123"

    assert command_route.called


@respx.mock
def test_sync_query_sandboxes_serializes_tag_filters(mock_env_clear: None) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params.multi_items() == [
            ("teamId", "team_123"),
            ("project", "prj_123"),
            ("tags", "env:prod"),
            ("tags", "team:api"),
        ]
        return httpx.Response(200, json={"sandboxes": [_sandbox_response()["sandbox"]]})

    route = respx.get("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)

    with vercel.session(service_options=_session_options()):
        handles = list(
            sandbox_sync.query_sandboxes(
                tags=[
                    TagFilter(key="env", value="prod"),
                    TagFilter(key="team", value="api"),
                ]
            )
        )

    assert route.called
    assert handles[0].name == "preview"


@respx.mock
def test_sync_query_sandbox_context_cleanup_uses_query_project_id(
    mock_env_clear: None,
) -> None:
    delete_requests: list[dict[str, str]] = []

    respx.get("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json={"sandboxes": [_sandbox_response()["sandbox"]]})
    )

    def delete_handler(request: httpx.Request) -> httpx.Response:
        delete_requests.append(dict(request.url.params))
        return httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )

    delete_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=delete_handler
    )

    with vercel.session(service_options=_session_options()):
        handle = next(sandbox_sync.query_sandboxes(project_id="custom_project"))
        with handle:
            pass

    assert delete_route.called
    assert handle.project_id == "custom_project"
    assert delete_requests == [{"teamId": "team_123", "projectId": "custom_project"}]


def test_sync_query_sandboxes_binds_session_at_iterator_creation(
    mock_env_clear: None,
) -> None:
    with vercel.session(service_options=_session_options()):
        handles = sandbox_sync.query_sandboxes()

    with pytest.raises(VercelSessionClosedError):
        next(handles)


@respx.mock
def test_sync_query_sandboxes_uses_page_size_and_supports_islice(
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
                    page_size=2,
                    cursor="cursor_1",
                    sort_by="createdAt",
                    sort_order="desc",
                    name_prefix="preview",
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
            "sortBy": "createdAt",
            "sortOrder": "desc",
            "namePrefix": "preview",
        },
        {
            "teamId": "team_123",
            "project": "prj_123",
            "limit": "2",
            "cursor": "cursor_2",
            "sortBy": "createdAt",
            "sortOrder": "desc",
            "namePrefix": "preview",
        },
    ]
