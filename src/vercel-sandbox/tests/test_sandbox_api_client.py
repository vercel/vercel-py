from datetime import timedelta

import httpx
import pytest
from httpx._types import HeaderTypes, QueryParamTypes

from vercel.internal.core.http import (
    BaseTransport,
    ReadResponsePolicy,
    RequestBody,
    StreamingRequest,
    StreamingResponse,
)
from vercel.internal.core.url import format_url_path
from vercel.sandbox._internal.api_client import SandboxApiClient, _WriteFilesUpload
from vercel.sandbox._internal.errors import SandboxApiError, SandboxResponseError
from vercel.sandbox._internal.options import SandboxCredentials


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
        timeout: timedelta | None = None,
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


class _CompletedResponse(StreamingResponse):
    def __init__(self, response: httpx.Response) -> None:
        self.response = response
        self.closed = False

    async def __anext__(self) -> bytes:
        raise StopAsyncIteration

    async def aiter_lines(self):  # type: ignore[no-untyped-def]
        if False:
            yield ""

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
