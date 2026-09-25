import json
from datetime import timedelta

import httpx2 as httpx
import pytest
from httpx2._types import HeaderTypes, QueryParamTypes

from vercel._internal.core.http import (
    NO_TIMEOUT,
    BaseTransport,
    JSONBody,
    ReadResponsePolicy,
    RequestBody,
    RequestTimeout,
    StreamingRequest,
    StreamingResponse,
)
from vercel._internal.core.url import format_url_path
from vercel.sandbox._internal.api_client import SandboxApiClient, _WriteFilesUpload
from vercel.sandbox._internal.errors import SandboxApiError, SandboxResponseError
from vercel.sandbox._internal.options import SandboxCredentials
from vercel.sandbox._internal.process_output import ProcessOutputRouter


class InvalidJsonTransport(BaseTransport):
    def __init__(self) -> None:
        self.paths: list[str] = []

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
        self.paths.append(path)
        return httpx.Response(
            200,
            content=b"not-json",
            request=httpx.Request(method, path),
        )


class RecordingJsonTransport(BaseTransport):
    def __init__(self, data: object) -> None:
        self.data = data
        self.request: tuple[str, str, str | None, QueryParamTypes | None, RequestBody] | None = None
        self.timeout: RequestTimeout = None

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
        self.request = (method, path, token, params, body)
        self.timeout = timeout
        return httpx.Response(200, json=self.data, request=httpx.Request(method, path))


class _CompletedResponse(StreamingResponse):
    def __init__(self, response: httpx.Response, *, lines: tuple[str, ...] = ()) -> None:
        self.response = response
        self.closed = False
        self.lines = lines

    async def __anext__(self) -> bytes:
        raise StopAsyncIteration

    async def aiter_lines(self):  # type: ignore[no-untyped-def]
        for line in self.lines:
            yield line

    async def aclose(self) -> None:
        self.closed = True


class _CompletedRequest(StreamingRequest):
    def __init__(self, response: _CompletedResponse) -> None:
        self.response = response

    async def write(self, data: bytes) -> None:
        raise NotImplementedError

    async def finish(self) -> StreamingResponse:
        return self.response

    async def abort(self) -> None:
        raise NotImplementedError


class RecordingStreamTransport(RecordingJsonTransport):
    def __init__(self, *, lines: tuple[str, ...] = ()) -> None:
        super().__init__({})
        self.lines = lines
        self.timeout: RequestTimeout = None

    async def open_response_stream(
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
        read_response: ReadResponsePolicy = ReadResponsePolicy.NON_SUCCESS_ONLY,
        chunk_size: int | None = None,
    ) -> StreamingResponse:
        self.timeout = timeout
        response = httpx.Response(200, request=httpx.Request(method, path))
        return _CompletedResponse(response, lines=self.lines)


def _sandbox_client(transport: BaseTransport) -> SandboxApiClient:
    async def credentials_factory() -> SandboxCredentials:
        return SandboxCredentials(
            token="token",
            team_id="team_123",
            project_id="prj_123",
        )

    return SandboxApiClient(
        base_url="https://sandbox.test",
        credentials_factory=credentials_factory,
        transport=transport,
        file_transfer_timeout=timedelta(minutes=5),
    )


def _command_response(*, exit_code: int | None) -> dict[str, object]:
    return {
        "command": {
            "id": "cmd_1",
            "name": "python",
            "args": [],
            "cwd": "/vercel/sandbox",
            "sessionId": "sbx_1",
            "exitCode": exit_code,
            "startedAt": 1,
        }
    }


@pytest.mark.parametrize(
    ("operation", "expected_timeout"),
    [
        ("wait", NO_TIMEOUT),
        ("poll", None),
        ("run", NO_TIMEOUT),
        ("logs", NO_TIMEOUT),
        ("read", timedelta(minutes=5)),
    ],
)
async def test_operations_choose_request_timeout(
    operation: str, expected_timeout: RequestTimeout
) -> None:
    if operation in {"wait", "poll"}:
        transport = RecordingJsonTransport(_command_response(exit_code=0))
    else:
        transport = RecordingStreamTransport(
            lines=(
                json.dumps(_command_response(exit_code=None)),
                json.dumps(_command_response(exit_code=0)),
            )
        )
    client = _sandbox_client(transport)

    if operation in {"wait", "poll"}:
        await client.get_command(session_id="sbx_1", command_id="cmd_1", wait=operation == "wait")
    elif operation == "run":
        await client.run_process(
            session_id="sbx_1",
            command="python",
            output_router=ProcessOutputRouter(stdout=None, stderr=None, capture_output=False),
        )
    elif operation == "logs":
        response = await client.command_logs_response(session_id="sbx_1", command_id="cmd_1")
        await response.aclose()
    else:
        response = await client.open_read_response(session_id="sbx_1", path="large.bin")
        await response.aclose()

    if expected_timeout is NO_TIMEOUT or expected_timeout is None:
        assert transport.timeout is expected_timeout
    else:
        assert transport.timeout == expected_timeout


async def test_invalid_json_response_raises_response_error(mock_env_clear: None) -> None:
    transport = InvalidJsonTransport()
    client = _sandbox_client(transport)

    with pytest.raises(SandboxResponseError):
        await client.get_sandbox(name="preview")
    assert transport.paths == ["https://sandbox.test/v2/sandboxes/preview"]


@pytest.mark.parametrize("status", [204, 400])
async def test_write_files_upload_finish_closes_stream(status: int) -> None:
    raw_response = httpx.Response(
        status,
        json={"error": {"message": "upload failed"}},
        request=httpx.Request("POST", "https://sandbox.test/upload"),
    )
    stream = _CompletedResponse(raw_response)
    upload = _WriteFilesUpload(_CompletedRequest(stream))

    if status < 400:
        await upload.finish()
    else:
        with pytest.raises(SandboxApiError):
            await upload.finish()

    assert stream.closed


def test_format_url_path_quotes_placeholder_values() -> None:
    assert format_url_path(
        "v2/sandboxes/{name}/{command_id}",
        name="name/with spaces",
        command_id="cmd?x=1",
    ) == ("v2/sandboxes/name%2Fwith%20spaces/cmd%3Fx%3D1")


async def test_fork_sandbox_encodes_source_query_and_overrides() -> None:
    transport = RecordingJsonTransport(
        {
            "sandbox": {
                "name": "forked",
                "currentSessionId": "sbx_fork",
                "status": "running",
            },
            "session": {
                "id": "sbx_fork",
                "sourceSandboxName": "forked",
                "projectId": "prj_other",
                "status": "running",
            },
        }
    )
    client = _sandbox_client(transport)

    result = await client.fork_sandbox(
        source_sandbox="source/with spaces",
        project_id="prj_other",
        name="forked",
        ports=[],
        execution_time_limit=timedelta(seconds=12.5),
        image="team/project/image:v1",
        persistent=False,
        env={},
        tags={},
    )

    assert result.name == "forked"
    assert transport.request is not None
    method, path, token, params, body = transport.request
    assert method == "POST"
    assert path == "https://sandbox.test/v2/sandboxes/source%2Fwith%20spaces/fork"
    assert token == "token"
    assert params == {"teamId": "team_123", "projectId": "prj_other"}
    assert isinstance(body, JSONBody)
    assert body.data == {
        "name": "forked",
        "ports": [],
        "timeout": 12500,
        "image": "team/project/image:v1",
        "persistent": False,
        "env": {},
        "tags": {},
        "mounts": {},
    }


@pytest.mark.parametrize(
    ("delete_orphan_snapshots", "expected_params"),
    [
        (False, {"teamId": "team_123", "projectId": "prj_123"}),
        (
            True,
            {
                "teamId": "team_123",
                "projectId": "prj_123",
                "deleteOrphanSnapshots": "true",
            },
        ),
    ],
)
async def test_destroy_sandbox_forwards_delete_orphan_snapshots(
    delete_orphan_snapshots: bool,
    expected_params: dict[str, str],
) -> None:
    transport = RecordingJsonTransport(
        {"sandbox": {"name": "preview", "currentSessionId": "sbx_123"}}
    )
    client = _sandbox_client(transport)

    await client.destroy_sandbox(delete_orphan_snapshots=delete_orphan_snapshots, name="preview")

    assert transport.request is not None
    method, path, _, params, _ = transport.request
    assert method == "DELETE"
    assert path == "https://sandbox.test/v2/sandboxes/preview"
    assert params == expected_params


async def test_stop_runtime_session_rejects_replacement_identity() -> None:
    client = _sandbox_client(
        RecordingJsonTransport({"session": {"id": "sbx_other", "status": "stopped"}})
    )

    with pytest.raises(SandboxResponseError, match="different session identity"):
        await client.stop_runtime_session(session_id="sbx_123")
