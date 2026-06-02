from datetime import timedelta

import httpx
import pytest
from httpx._types import HeaderTypes, QueryParamTypes

from vercel._internal.http import BaseTransport, ReadResponsePolicy, RequestBody
from vercel._internal.unstable.sandbox.api_client import SandboxApiClient
from vercel._internal.unstable.sandbox.errors import SandboxResponseError
from vercel._internal.unstable.sandbox.options import SandboxCredentials
from vercel._internal.url import format_url_path


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


async def test_invalid_json_response_raises_response_error(mock_env_clear: None) -> None:
    async def credentials_factory() -> SandboxCredentials:
        return SandboxCredentials(
            token="token",
            team_id="team_123",
            project_id="prj_123",
        )

    transport = InvalidJsonTransport()
    client = SandboxApiClient(
        base_url="https://sandbox.test",
        credentials_factory=credentials_factory,
        transport=transport,
    )

    with pytest.raises(SandboxResponseError):
        await client.get_sandbox(name="preview")
    assert transport.paths == ["https://sandbox.test/v2/sandboxes/preview"]


def test_format_url_path_quotes_placeholder_values() -> None:
    assert format_url_path(
        "v2/sandboxes/{name}/{command_id}",
        name="name/with spaces",
        command_id="cmd?x=1",
    ) == ("v2/sandboxes/name%2Fwith%20spaces/cmd%3Fx%3D1")
