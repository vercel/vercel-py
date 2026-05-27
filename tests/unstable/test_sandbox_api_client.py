from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

import httpx
import pytest
from httpx._types import HeaderTypes, QueryParamTypes

from vercel._internal.http import BaseTransport, JSONBody, RequestBody
from vercel._internal.unstable.sandbox.api_client import SandboxApiClient
from vercel._internal.unstable.sandbox.errors import SandboxApiError, SandboxResponseError
from vercel._internal.unstable.sandbox.models import JSONObject, SandboxStatus
from vercel._internal.unstable.sandbox.options import (
    SandboxCredentials,
    SandboxServiceOptions,
)
from vercel._internal.unstable.sandbox.service import SandboxService
from vercel._internal.unstable.session import AliveToken


@dataclass(frozen=True, slots=True)
class RecordedRequest:
    method: str
    path: str
    token: str | None
    params: QueryParamTypes | None
    body: RequestBody
    headers: HeaderTypes | None


class RecordingTransport(BaseTransport):
    def __init__(
        self,
        responses: Sequence[object],
        *,
        status_codes: Sequence[int] | None = None,
    ) -> None:
        self.requests: list[RecordedRequest] = []
        self._responses = list(responses)
        self._status_codes = list(status_codes or [200] * len(responses))

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
    ) -> httpx.Response:
        self.requests.append(
            RecordedRequest(
                method=method,
                path=path,
                token=token,
                params=params,
                body=body,
                headers=headers,
            )
        )
        response = self._responses.pop(0)
        status_code = self._status_codes.pop(0)
        if isinstance(response, httpx.Response):
            return response
        return httpx.Response(
            status_code,
            json=response,
            request=httpx.Request(method, f"https://sandbox.test/{path}"),
        )


class CountingCredentialsFactory:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> SandboxCredentials:
        self.calls += 1
        return SandboxCredentials(
            token=f"token-{self.calls}",
            team_id="team_123",
            project_id="prj_123",
        )


def _client(
    *,
    options: SandboxServiceOptions,
    transport: RecordingTransport,
) -> SandboxApiClient:
    return SandboxApiClient(
        base_url=options.base_url,
        credentials_factory=options.credentials_factory,
        transport=transport,
    )


async def _no_sleep(_: float) -> None:
    return None


def _sandbox_response(
    *,
    name: str = "preview",
    session_id: str = "sbx_123",
    status: str = "running",
    session_status: str | None = None,
) -> JSONObject:
    return cast(
        JSONObject,
        {
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
                "createdAt": 1,
                "updatedAt": 2,
            },
            "routes": [
                {
                    "url": "https://preview.vercel.run",
                    "subdomain": "preview",
                    "port": 3000,
                }
            ],
        },
    )


def _sandbox_list_response() -> JSONObject:
    return cast(
        JSONObject,
        {
            "sandboxes": [
                {
                    "name": "listed",
                    "currentSessionId": "sbx_listed",
                    "status": "running",
                    "persistent": True,
                    "runtime": "python3.13",
                    "createdAt": 1,
                    "updatedAt": 2,
                }
            ],
            "pagination": {"count": 1, "next": None},
        },
    )


async def test_api_client_create_uses_v2_contract_and_request_time_auth(
    mock_env_clear: None,
) -> None:
    credentials_factory = CountingCredentialsFactory()
    transport = RecordingTransport([_sandbox_response()])
    client = _client(
        options=SandboxServiceOptions(
            credentials_factory=credentials_factory,
        ),
        transport=transport,
    )

    await client.create_sandbox(
        name="preview",
        runtime="python3.13",
        snapshot_expiration=timedelta(minutes=20),
        tags={"env": "test"},
    )

    assert credentials_factory.calls == 1
    request = transport.requests[0]
    assert request.method == "POST"
    assert request.path == "v2/sandboxes"
    assert request.token == "token-1"
    assert request.params == {"teamId": "team_123"}
    assert isinstance(request.body, JSONBody)
    assert request.body.data == {
        "projectId": "prj_123",
        "name": "preview",
        "runtime": "python3.13",
        "snapshotExpiration": 1_200_000,
        "tags": {"env": "test"},
    }


async def test_constant_credentials_are_normalized_to_request_time_factory(
    mock_env_clear: None,
) -> None:
    options = SandboxServiceOptions(
        token="token",
        team_id="team_123",
        project_id="prj_123",
    )

    first = await options.credentials_factory()
    second = await options.credentials_factory()

    assert first == SandboxCredentials(
        token="token",
        team_id="team_123",
        project_id="prj_123",
    )
    assert second == first
    assert second is not first


async def test_api_client_uses_project_query_shapes(mock_env_clear: None) -> None:
    credentials_factory = CountingCredentialsFactory()
    transport = RecordingTransport(
        [
            _sandbox_response(name="needs/quote"),
            _sandbox_list_response(),
            _sandbox_response(name="needs/quote"),
        ]
    )
    client = _client(
        options=SandboxServiceOptions(
            credentials_factory=credentials_factory,
        ),
        transport=transport,
    )

    await client.get_sandbox(name="needs/quote")
    await client.query_sandboxes(limit=1, sort_by="name", name_prefix="needs")
    await client.destroy_sandbox(name="needs/quote")

    assert credentials_factory.calls == 3
    get_request, query_request, destroy_request = transport.requests
    assert get_request.path == "v2/sandboxes/needs%2Fquote"
    assert get_request.token == "token-1"
    assert get_request.params == {
        "teamId": "team_123",
        "projectId": "prj_123",
        "resume": "true",
    }
    assert query_request.path == "v2/sandboxes"
    assert query_request.token == "token-2"
    assert query_request.params == {
        "teamId": "team_123",
        "project": "prj_123",
        "limit": 1,
        "sortBy": "name",
        "namePrefix": "needs",
    }
    assert destroy_request.method == "DELETE"
    assert destroy_request.token == "token-3"
    assert destroy_request.params == {"teamId": "team_123", "projectId": "prj_123"}


async def test_api_client_stops_runtime_session_with_v2_contract(
    mock_env_clear: None,
) -> None:
    credentials_factory = CountingCredentialsFactory()
    transport = RecordingTransport([_sandbox_response(session_id="sbx/quote")])
    client = _client(
        options=SandboxServiceOptions(
            credentials_factory=credentials_factory,
        ),
        transport=transport,
    )

    await client.destroy_runtime_session(session_id="sbx/quote")

    assert credentials_factory.calls == 1
    request = transport.requests[0]
    assert request.method == "POST"
    assert request.path == "v2/sandboxes/sessions/sbx%2Fquote/stop"
    assert request.token == "token-1"
    assert request.params == {"teamId": "team_123"}
    assert isinstance(request.body, JSONBody)
    assert request.body.data == {}


async def test_sandbox_service_maps_v2_sandbox_handles(mock_env_clear: None) -> None:
    transport = RecordingTransport([_sandbox_response(), _sandbox_list_response()])
    options = SandboxServiceOptions(
        token="token",
        team_id="team_123",
        project_id="prj_123",
    )
    api_client = _client(
        options=options,
        transport=transport,
    )
    service = SandboxService(
        api_client=api_client,
        alive_token=AliveToken(),
        options=options,
    )

    created = await service.create_sandbox(name="preview", runtime="python3.13")
    listed = await service.query_sandboxes()

    assert created.name == "preview"
    assert created.current_session_id == "sbx_123"
    assert created.current_session is not None
    assert created.current_session.project_id == "prj_123"
    assert created.routes[0].url == "https://preview.vercel.run"
    assert listed[0].name == "listed"
    assert listed[0].current_session_id == "sbx_listed"


async def test_sandbox_service_waits_for_ready_create_status(mock_env_clear: None) -> None:
    transport = RecordingTransport(
        [
            _sandbox_response(status="running", session_status="pending"),
            _sandbox_response(),
        ]
    )
    options = SandboxServiceOptions(
        token="token",
        team_id="team_123",
        project_id="prj_123",
    )
    service = SandboxService(
        api_client=_client(options=options, transport=transport),
        alive_token=AliveToken(),
        options=options,
        sleep=_no_sleep,
    )

    created = await service.create_sandbox(name="preview", runtime="python3.13")

    assert created.name == "preview"
    assert created.current_session is not None
    assert created.current_session.status is SandboxStatus.RUNNING
    create_request, poll_request = transport.requests
    assert create_request.method == "POST"
    assert poll_request.method == "GET"
    assert poll_request.path == "v2/sandboxes/preview"
    assert poll_request.params == {
        "teamId": "team_123",
        "projectId": "prj_123",
        "resume": "false",
    }


async def test_api_error_preserves_status_and_data(mock_env_clear: None) -> None:
    transport = RecordingTransport(
        [cast(JSONObject, {"error": {"code": "bad_request", "message": "Nope"}})],
        status_codes=[400],
    )
    client = _client(
        options=SandboxServiceOptions(
            token="token",
            team_id="team_123",
            project_id="prj_123",
        ),
        transport=transport,
    )

    with pytest.raises(SandboxApiError) as exc_info:
        await client.get_sandbox(name="preview")

    assert exc_info.value.status_code == 400
    assert exc_info.value.data == {"error": {"code": "bad_request", "message": "Nope"}}
    assert not isinstance(exc_info.value, SandboxResponseError)


async def test_invalid_json_response_raises_response_error(mock_env_clear: None) -> None:
    transport = RecordingTransport(
        [
            httpx.Response(
                200,
                content=b"not-json",
                request=httpx.Request("GET", "https://sandbox.test/v2/sandboxes/preview"),
            )
        ]
    )
    client = _client(
        options=SandboxServiceOptions(
            token="token",
            team_id="team_123",
            project_id="prj_123",
        ),
        transport=transport,
    )

    with pytest.raises(SandboxResponseError):
        await client.get_sandbox(name="preview")


@pytest.mark.parametrize("response", [[], "nope", 1], ids=["array", "string", "number"])
async def test_non_object_json_response_raises_response_error(
    mock_env_clear: None,
    response: object,
) -> None:
    transport = RecordingTransport([response])
    client = _client(
        options=SandboxServiceOptions(
            token="token",
            team_id="team_123",
            project_id="prj_123",
        ),
        transport=transport,
    )

    with pytest.raises(SandboxResponseError) as exc_info:
        await client.get_sandbox(name="preview")

    assert exc_info.value.data == response


async def test_missing_required_sandbox_fields_raise_response_error(
    mock_env_clear: None,
) -> None:
    transport = RecordingTransport(
        [
            cast(
                JSONObject,
                {
                    "sandbox": {"name": "preview"},
                    "routes": [],
                },
            )
        ]
    )
    client = _client(
        options=SandboxServiceOptions(
            token="token",
            team_id="team_123",
            project_id="prj_123",
        ),
        transport=transport,
    )

    with pytest.raises(SandboxResponseError):
        await client.create_sandbox(name="preview")


@pytest.mark.parametrize("operation", ["create", "get"])
async def test_missing_sandbox_object_in_create_or_get_raises_response_error(
    mock_env_clear: None,
    operation: str,
) -> None:
    transport = RecordingTransport(
        [
            cast(
                JSONObject,
                {
                    "session": {
                        "id": "sbx_123",
                        "sourceSandboxName": "preview",
                        "projectId": "prj_123",
                    },
                    "routes": [],
                },
            )
        ]
    )
    options = SandboxServiceOptions(
        token="token",
        team_id="team_123",
        project_id="prj_123",
    )
    service = SandboxService(
        api_client=_client(options=options, transport=transport),
        alive_token=AliveToken(),
        options=options,
    )

    with pytest.raises(SandboxResponseError) as exc_info:
        if operation == "create":
            await service.create_sandbox(name="preview")
        else:
            await service.get_sandbox(name="preview")

    assert isinstance(exc_info.value.data, dict)
    assert exc_info.value.data["sandbox"] is None
    assert exc_info.value.data["session"]["id"] == "sbx_123"
