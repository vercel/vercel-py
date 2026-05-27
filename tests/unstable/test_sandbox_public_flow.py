import gc
import json

import httpx
import pytest
import respx

from vercel import unstable as vercel
from vercel._internal.unstable.errors import VercelSessionClosedError
from vercel.unstable import sandbox
from vercel.unstable.sandbox import (
    SandboxInvalidHandleError,
    SandboxServiceOptions,
    SandboxStatus,
    SandboxTerminalStateError,
)


def _create_sandbox_response(
    *,
    session_id: str = "sbx_123",
    status: str = "running",
    session_status: str | None = None,
) -> dict[str, object]:
    return {
        "sandbox": {
            "name": "preview",
            "currentSessionId": session_id,
            "status": status,
            "persistent": True,
            "runtime": "python3.13",
            "createdAt": 1,
            "updatedAt": 2,
        },
        "session": {
            "id": session_id,
            "sourceSandboxName": "preview",
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


@respx.mock
async def test_public_create_sandbox_uses_session_options(mock_env_clear: None) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/v2/sandboxes"
        assert dict(request.url.params) == {"teamId": "team_123"}
        assert request.headers["authorization"] == "Bearer token"
        assert json.loads(request.content) == {
            "projectId": "prj_123",
            "name": "preview",
            "runtime": "python3.13",
        }
        return httpx.Response(200, json=_create_sandbox_response())

    route = respx.post("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)

    async with vercel.session(
        service_options=[
            SandboxServiceOptions(
                base_url="https://sandbox.test",
                token="token",
                team_id="team_123",
                project_id="prj_123",
            )
        ]
    ):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")

    assert route.called
    assert handle.name == "preview"
    assert handle.status is SandboxStatus.RUNNING
    assert handle.current_session is not None
    assert handle.current_session.id == "sbx_123"
    assert handle.current_session.status is SandboxStatus.RUNNING
    assert handle.current_session.project_id == "prj_123"
    route_data = handle.routes[0]
    assert route_data.url == "https://preview.sandbox.test"
    assert route_data.subdomain == "preview"
    assert route_data.port == 3000
    assert route_data.system is False


@respx.mock
async def test_create_sandbox_terminal_state_raises_typed_error(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json=_create_sandbox_response(status="running", session_status="failed"),
        )
    )

    async with vercel.session(
        service_options=[
            SandboxServiceOptions(
                base_url="https://sandbox.test",
                token="token",
                team_id="team_123",
                project_id="prj_123",
            )
        ]
    ):
        with pytest.raises(SandboxTerminalStateError) as exc_info:
            await sandbox.create_sandbox(name="preview", runtime="python3.13")

    assert exc_info.value.status is SandboxStatus.FAILED


@respx.mock
async def test_create_sandbox_context_destroys_and_invalidates_handle(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_create_sandbox_response())
    )

    def delete_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/sandboxes/preview"
        assert dict(request.url.params) == {"teamId": "team_123", "projectId": "prj_123"}
        return httpx.Response(
            200,
            json=_create_sandbox_response(status="stopped", session_status="stopped"),
        )

    delete_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=delete_handler
    )

    async with vercel.session(
        service_options=[
            SandboxServiceOptions(
                base_url="https://sandbox.test",
                token="token",
                team_id="team_123",
                project_id="prj_123",
            )
        ]
    ):
        async with sandbox.create_sandbox(name="preview", runtime="python3.13") as handle:
            assert handle.name == "preview"

        assert delete_route.called
        with pytest.raises(SandboxInvalidHandleError):
            handle.session()


@respx.mock
async def test_create_sandbox_operation_can_only_be_consumed_once(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_create_sandbox_response())
    )

    async with vercel.session(
        service_options=[
            SandboxServiceOptions(
                base_url="https://sandbox.test",
                token="token",
                team_id="team_123",
                project_id="prj_123",
            )
        ]
    ):
        operation = sandbox.create_sandbox(name="preview", runtime="python3.13")
        await operation

        with pytest.raises(RuntimeError, match="can only be used once"):
            await operation


async def test_create_sandbox_operation_uses_captured_session(mock_env_clear: None) -> None:
    async with vercel.session():
        operation = sandbox.create_sandbox(name="preview", runtime="python3.13")

    with pytest.raises(VercelSessionClosedError):
        await operation


def test_unconsumed_create_sandbox_operation_warns() -> None:
    with pytest.warns(RuntimeWarning, match="never awaited or entered"):
        operation = sandbox.create_sandbox(name="preview", runtime="python3.13")
        del operation
        gc.collect()


@respx.mock
async def test_await_runtime_session_uses_parent_handle_and_session_options(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_create_sandbox_response())
    )

    def get_handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/v2/sandboxes/preview"
        assert dict(request.url.params) == {
            "teamId": "team_123",
            "projectId": "prj_123",
            "resume": "true",
        }
        return httpx.Response(
            200,
            json=_create_sandbox_response(session_id="sbx_runtime"),
        )

    runtime_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=get_handler
    )

    async with vercel.session(
        service_options=[
            SandboxServiceOptions(
                base_url="https://sandbox.test",
                token="token",
                team_id="team_123",
                project_id="prj_123",
            )
        ]
    ):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = await handle.session()

    assert runtime_route.called
    assert runtime_session.id == "sbx_runtime"
    assert runtime_session.status is SandboxStatus.RUNNING
    with pytest.raises(SandboxInvalidHandleError):
        await runtime_session.run_command("python", ["--version"])


@respx.mock
async def test_runtime_session_context_stops_and_invalidates_only_runtime_handle(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_create_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_create_sandbox_response(session_id="sbx_runtime"),
        )
    )

    def stop_handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/v2/sandboxes/sessions/sbx_runtime/stop"
        assert dict(request.url.params) == {"teamId": "team_123"}
        assert json.loads(request.content) == {}
        return httpx.Response(
            200,
            json=_create_sandbox_response(
                session_id="sbx_runtime",
                status="stopped",
                session_status="stopped",
            ),
        )

    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/stop").mock(
        side_effect=stop_handler
    )

    async with vercel.session(
        service_options=[
            SandboxServiceOptions(
                base_url="https://sandbox.test",
                token="token",
                team_id="team_123",
                project_id="prj_123",
            )
        ]
    ):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")

        async with handle.session() as runtime_session:
            assert runtime_session.id == "sbx_runtime"
            with pytest.raises(NotImplementedError):
                await runtime_session.run_command("python", ["--version"])

        assert stop_route.called
        with pytest.raises(SandboxInvalidHandleError):
            await runtime_session.run_command("python", ["--version"])

        assert handle.session() is not None
