import asyncio
import gc
import json
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from itertools import islice
from threading import Condition, Event
from typing import Any

import anyio
import httpx
import pytest
import respx
from pydantic import BaseModel, ValidationError
from sandbox_fixtures import sandbox_service_options as _session_options

from vercel import sandbox
from vercel._internal.core.session import get_active_session
from vercel.api import session
from vercel.errors import VercelSessionClosedError
from vercel.sandbox import (
    GitSource,
    NetworkPolicy,
    NetworkPolicyKeyValueMatcher,
    NetworkPolicyMatcher,
    NetworkPolicyRequestMatcher,
    NetworkPolicyRule,
    NetworkPolicySubnets,
    NetworkPolicyTransform,
    SandboxApiError,
    SandboxCleanupError,
    SandboxQuery,
    SandboxQueryByCreatedAt,
    SandboxQueryByCurrentSnapshotId,
    SandboxQueryByName,
    SandboxQueryByStatusUpdatedAt,
    SandboxResources,
    SandboxResponseError,
    SandboxSource,
    SandboxStatus,
    SandboxTerminalStateError,
    SandboxTimeoutError,
    SnapshotExpiration,
    SnapshotRetention,
    SnapshotRetentionState,
    SnapshotSource,
    TagFilter,
    TarballSource,
    sync as sandbox_sync,
)
from vercel.sandbox._internal import (
    async_runtime as sandbox_async_runtime,
    sync_runtime as sandbox_sync_runtime,
)
from vercel.sandbox._internal.service import get_sandbox_service
from vercel.sandbox._internal.state import (
    SandboxRuntimeSessionState,
    SandboxState,
)


def _sandbox_response(
    *,
    name: str = "preview",
    session_id: str = "sbx_123",
    status: str = "running",
    session_status: str | None = None,
    project_id: str = "prj_123",
) -> dict[str, Any]:
    return {
        "sandbox": {
            "name": name,
            "currentSessionId": session_id,
            "image": "vercel/sandbox/universal:latest",
            "status": status,
            "persistent": True,
            "region": "iad1",
            "failoverRegions": ["sfo1", "cle1"],
            "timeout": 300000,
            "snapshotExpiration": 0,
            "keepLastSnapshots": {
                "count": 2,
                "expiration": 86400000,
                "deleteEvicted": False,
            },
            "createdAt": 1,
            "updatedAt": 2,
        },
        "session": {
            "id": session_id,
            "sourceSandboxName": name,
            "projectId": project_id,
            "status": session_status or status,
            "cwd": "/vercel/sandbox",
            "region": "cle1",
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
async def test_async_lifecycle_forwards_private_parameters_without_leaking_to_polls(
    mock_env_clear: None,
) -> None:
    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="created", status="pending"))
    )
    create_poll = respx.get("https://sandbox.test/v2/sandboxes/created").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="created"))
    )
    fork_route = respx.post("https://sandbox.test/v2/sandboxes/created/fork").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="forked", status="pending"))
    )
    fork_poll = respx.get("https://sandbox.test/v2/sandboxes/forked").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="forked"))
    )
    get_route = respx.get("https://sandbox.test/v2/sandboxes/fetched").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="fetched"))
    )
    resume_route = respx.get("https://sandbox.test/v2/sandboxes/resumed").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="resumed"))
    )

    async with session(service_options=_session_options()):
        await sandbox.create_sandbox(name="created", __networkId="network_123")
        await sandbox.fork_sandbox(
            source_sandbox="created", name="forked", __networkId="network_123"
        )
        await sandbox.get_sandbox(name="fetched", __includeSystemRoutes=True)
        await sandbox.resume_sandbox(name="resumed", __includeSystemRoutes=True)

    assert json.loads(create_route.calls.last.request.content)["__networkId"] == "network_123"
    assert "__networkId" not in create_poll.calls.last.request.url.params
    assert json.loads(fork_route.calls.last.request.content)["__networkId"] == "network_123"
    assert "__networkId" not in fork_poll.calls.last.request.url.params
    assert get_route.calls.last.request.url.params["__includeSystemRoutes"] == "true"
    assert resume_route.calls.last.request.url.params["__includeSystemRoutes"] == "true"


@respx.mock
def test_sync_get_forwards_private_parameters(mock_env_clear: None) -> None:
    route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    with session(service_options=_session_options()):
        sandbox_sync.get_sandbox(name="preview", __includeSystemRoutes=True)

    assert route.calls.last.request.url.params["__includeSystemRoutes"] == "true"


def test_public_sandbox_functions_reject_unknown_parameters() -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'network_id'"):
        sandbox.create_sandbox(network_id="network_123")
    with pytest.raises(TypeError, match="unexpected keyword argument 'network_id'"):
        sandbox_sync.create_sandbox(network_id="network_123")


def _image_sandbox_response(
    *,
    image: str = "my-repository@sha256:resolved",
    name: str = "preview",
    session_id: str = "sbx_123",
    project_id: str = "prj_123",
) -> dict[str, Any]:
    response = _sandbox_response(
        name=name,
        session_id=session_id,
        project_id=project_id,
    )
    sandbox_payload = response["sandbox"]
    assert isinstance(sandbox_payload, dict)
    sandbox_payload["image"] = image
    return response


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
            "regions": ["iad1", "sfo1", "cle1"],
            "status": status,
            "sizeBytes": 1024,
            "createdAt": 1,
            "updatedAt": 2,
        }
    }


def _logs_response(*records: object) -> httpx.Response:
    return httpx.Response(
        200,
        text="\n".join(
            record if isinstance(record, str) else json.dumps(record) for record in records
        )
        + "\n",
    )


def _network_policy_matcher() -> NetworkPolicyRequestMatcher:
    return NetworkPolicyRequestMatcher(
        path=NetworkPolicyMatcher.starts_with("/v1/"),
        method=["POST"],
        query=[
            NetworkPolicyKeyValueMatcher(
                key=NetworkPolicyMatcher.exact("stream"),
                value=NetworkPolicyMatcher.regex("^(true|false)$"),
            )
        ],
        headers=[
            NetworkPolicyKeyValueMatcher(
                key=NetworkPolicyMatcher.exact("authorization"),
                value=NetworkPolicyMatcher.starts_with("Bearer "),
            )
        ],
    )


def _authored_network_policy() -> NetworkPolicy:
    matcher = _network_policy_matcher()
    return NetworkPolicy.custom(
        allow={
            "example.com": (),
            "api.example.com": [
                NetworkPolicyRule(
                    match=matcher,
                    transform=[
                        NetworkPolicyTransform(
                            headers={"Authorization": "Bearer secret", "X-Trace": "one"}
                        ),
                        NetworkPolicyTransform(headers={"X-Trace": "two"}),
                    ],
                    forward_url="https://forward-proxy.internal/ingress/",
                ),
                NetworkPolicyRule(
                    transform=[NetworkPolicyTransform(headers={"X-Fallback": "fallback"})]
                ),
            ],
        },
        subnets=NetworkPolicySubnets(
            allow=["10.0.0.0/8"],
            deny=["10.1.0.0/16"],
        ),
    )


def _normalized_network_policy_response() -> dict[str, object]:
    match = {
        "path": {"startsWith": "/v1/"},
        "method": ["POST"],
        "queryString": [
            {
                "key": {"exact": "stream"},
                "value": {"regex": "^(true|false)$"},
            }
        ],
        "headers": [
            {
                "key": {"exact": "authorization"},
                "value": {"startsWith": "Bearer "},
            }
        ],
    }
    return {
        "mode": "custom",
        "allowedDomains": ["example.com", "api.example.com"],
        "allowedCIDRs": ["10.0.0.0/8"],
        "deniedCIDRs": ["10.1.0.0/16"],
        "injectionRules": [
            {
                "domain": "api.example.com",
                "headerNames": ["Authorization", "X-Trace"],
                "match": match,
            },
            {
                "domain": "api.example.com",
                "headerNames": ["X-Fallback"],
            },
        ],
        "forwardRules": [
            {
                "domain": "api.example.com",
                "forwardURL": "https://forward-proxy.internal/ingress/",
                "match": match,
            }
        ],
    }


def _parsed_network_policy_response() -> NetworkPolicy:
    matcher = _network_policy_matcher()
    return NetworkPolicy.custom(
        allow={
            "example.com": (),
            "api.example.com": [
                NetworkPolicyRule(
                    match=matcher,
                    transform=[NetworkPolicyTransform(header_names=["Authorization", "X-Trace"])],
                ),
                NetworkPolicyRule(transform=[NetworkPolicyTransform(header_names=["X-Fallback"])]),
                NetworkPolicyRule(
                    match=matcher,
                    forward_url="https://forward-proxy.internal/ingress/",
                ),
            ],
        },
        subnets=NetworkPolicySubnets(
            allow=["10.0.0.0/8"],
            deny=["10.1.0.0/16"],
        ),
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
        assert request.url.path == "/v3/sandboxes"
        assert dict(request.url.params) == {"teamId": "team_123"}
        assert request.headers["authorization"] == "Bearer token"
        assert request.headers["user-agent"].startswith("vercel-sandbox/")
        assert "Python/" in request.headers["user-agent"]
        assert json.loads(request.content) == {
            "projectId": "prj_other",
            "name": "preview",
            "source": {
                "type": "git",
                "url": "https://github.com/vercel/vercel-py",
                "revision": "main",
            },
            "timeout": 120000,
            "resources": {"vcpus": 2, "memory": 4096},
            "snapshotExpiration": 86400000,
            "keepLastSnapshots": {
                "count": 3,
                "expiration": 172800000,
                "deleteEvicted": False,
            },
            "tags": {"env": "test"},
            "region": "iad1",
            "failoverRegions": ["sfo1", "cle1"],
        }
        response = _sandbox_response(project_id="prj_other")
        payload = response["sandbox"]
        assert isinstance(payload, dict)
        payload["tags"] = {"env": "test"}
        return httpx.Response(200, json=response)

    route = respx.post("https://sandbox.test/v3/sandboxes").mock(side_effect=handler)
    update_responses = iter(
        [
            {
                "sandbox": {
                    "name": "preview",
                    "currentSessionId": "sbx_123",
                    "tags": {"env": "updated"},
                    "region": "sfo1",
                    "failoverRegions": [],
                }
            },
            {
                "sandbox": {
                    "name": "preview",
                    "currentSessionId": "sbx_123",
                    "tags": {},
                },
                "routes": [],
            },
            {
                "sandbox": {
                    "name": "preview",
                    "currentSessionId": "sbx_123",
                    "tags": {},
                }
            },
        ]
    )
    update_requests: list[httpx.Request] = []

    def update_handler(request: httpx.Request) -> httpx.Response:
        update_requests.append(request)
        return httpx.Response(200, json=next(update_responses))

    update_route = respx.patch("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=update_handler
    )

    async with session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(
            project_id="prj_other",
            name="preview",
            source=GitSource(
                url="https://github.com/vercel/vercel-py",
                revision="main",
            ),
            execution_time_limit=120,
            resources=SandboxResources(vcpus=2, memory=4096),
            snapshot_expiration=SnapshotExpiration(timedelta(days=1)),
            snapshot_retention=SnapshotRetention(
                count=3,
                expiration=timedelta(days=2),
                delete_evicted=False,
            ),
            tags={"env": "test"},
            region="iad1",
            failover_regions=("sfo1", "cle1"),
        )
        assert handle.image == "vercel/sandbox/universal:latest"
        assert handle.region == "iad1"
        assert handle.failover_regions == ("sfo1", "cle1")
        assert not hasattr(handle, "runtime")
        assert handle.current_session is not None
        assert not hasattr(handle.current_session, "runtime")

        with pytest.raises(AttributeError):
            handle.status = SandboxStatus.STOPPED  # type: ignore[misc]
        assert handle.tags is not None
        handle.tags["env"] = "mutated"
        retained_session = handle.current_session

        await handle.update(
            tags={"env": "updated"},
            execution_time_limit=4.5,
            snapshot_expiration=0,
            snapshot_retention=SnapshotRetention(count=1, expiration=0),
            region="sfo1",
            failover_regions=(),
        )
        assert handle.tags == {"env": "updated"}
        assert handle.region == "sfo1"
        assert handle.failover_regions == ()
        assert handle.current_session is retained_session
        assert handle.current_session.region == "cle1"
        assert handle.routes[0].url == "https://preview.sandbox.test"
        assert handle.project_id == "prj_other"

        await handle.update(tags={}, ports=[])
        assert handle.tags == {}
        assert handle.routes == ()
        assert handle.project_id == "prj_other"
        assert handle.current_session is retained_session

        await handle.update(snapshot_retention=None)

    assert route.called
    assert update_route.call_count == 3
    assert [dict(request.url.params) for request in update_requests] == [
        {"teamId": "team_123", "projectId": "prj_other"},
        {"teamId": "team_123", "projectId": "prj_other"},
        {"teamId": "team_123", "projectId": "prj_other"},
    ]
    assert [json.loads(request.content) for request in update_requests] == [
        {
            "timeout": 4500,
            "snapshotExpiration": 0,
            "keepLastSnapshots": {
                "count": 1,
                "expiration": 0,
                "deleteEvicted": True,
            },
            "tags": {"env": "updated"},
            "region": "sfo1",
            "failoverRegions": [],
        },
        {"ports": [], "tags": {}},
        {"keepLastSnapshots": None},
    ]
    assert handle.status is None
    assert handle.tags == {}
    assert handle.current_session is not None
    assert handle.current_session.project_id == "prj_other"
    assert handle.routes == ()
    assert not hasattr(handle, "model_dump")


@respx.mock
async def test_public_fork_sandbox_encodes_overrides_polls_and_cleans_up(
    mock_env_clear: None,
) -> None:
    pending = _sandbox_response(name="forked", session_id="sbx_fork", status="pending")
    fork_route = respx.post("https://sandbox.test/v2/sandboxes/source/fork").mock(
        return_value=httpx.Response(200, json=pending)
    )
    get_route = respx.get("https://sandbox.test/v2/sandboxes/forked").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(name="forked", session_id="sbx_fork")
        )
    )
    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_fork/stop").mock(
        return_value=httpx.Response(
            200,
            json={
                "session": _sandbox_response(
                    name="forked",
                    session_id="sbx_fork",
                    status="stopped",
                    session_status="stopped",
                )["session"]
            },
        )
    )

    async with session(service_options=_session_options()):
        async with sandbox.fork_sandbox(
            source_sandbox="source",
            project_id="prj_other",
            name="forked",
            ports=[],
            execution_time_limit=12.5,
            resources=SandboxResources(vcpus=2, memory=4096),
            image="team/project/image:v1",
            persistent=False,
            network_policy=NetworkPolicy.deny_all(),
            env={},
            tags={},
            snapshot_expiration=0,
            region="iad1",
            failover_regions=("sfo1",),
            snapshot_retention=SnapshotRetention(
                count=2,
                expiration=timedelta(days=1),
                delete_evicted=False,
            ),
            destroy=False,
        ) as forked:
            assert forked.name == "forked"

    request = fork_route.calls.last.request
    assert dict(request.url.params) == {"teamId": "team_123", "projectId": "prj_other"}
    assert json.loads(request.content) == {
        "name": "forked",
        "ports": [],
        "region": "iad1",
        "failoverRegions": ["sfo1"],
        "timeout": 12500,
        "resources": {"vcpus": 2, "memory": 4096},
        "image": "team/project/image:v1",
        "persistent": False,
        "networkPolicy": {"mode": "deny-all"},
        "env": {},
        "tags": {},
        "snapshotExpiration": 0,
        "keepLastSnapshots": {
            "count": 2,
            "expiration": 86400000,
            "deleteEvicted": False,
        },
    }
    assert dict(get_route.calls.last.request.url.params) == {
        "teamId": "team_123",
        "projectId": "prj_other",
        "resume": "false",
    }
    assert stop_route.called


@respx.mock
def test_sync_fork_sandbox_uses_inherited_defaults(mock_env_clear: None) -> None:
    fork_route = respx.post("https://sandbox.test/v2/sandboxes/source/fork").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(name="forked", session_id="sbx_fork")
        )
    )

    with session(service_options=_session_options()):
        forked = sandbox_sync.fork_sandbox(source_sandbox="source")

    assert isinstance(forked, sandbox_sync.SyncSandbox)
    assert forked.name == "forked"
    request = fork_route.calls.last.request
    assert dict(request.url.params) == {"teamId": "team_123", "projectId": "prj_123"}
    assert json.loads(request.content) == {}


@respx.mock
async def test_service_region_defaults_placement_operations_and_allows_call_overrides(
    mock_env_clear: None,
) -> None:
    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="created"))
    )
    fork_route = respx.post("https://sandbox.test/v2/sandboxes/source/fork").mock(
        return_value=httpx.Response(200, json=_sandbox_response(name="forked"))
    )
    update_route = respx.patch("https://sandbox.test/v2/sandboxes/created").mock(
        return_value=httpx.Response(
            200,
            json={"sandbox": {"name": "created", "currentSessionId": "sbx_123"}},
        )
    )

    async with session(service_options=_session_options(region="iad1")):
        created = await sandbox.create_sandbox(name="created")
        await sandbox.fork_sandbox(source_sandbox="source", region="sfo1")
        await created.update(tags={"updated": "true"})

    assert json.loads(create_route.calls.last.request.content) == {
        "projectId": "prj_123",
        "name": "created",
        "region": "iad1",
    }
    assert json.loads(fork_route.calls.last.request.content) == {"region": "sfo1"}
    assert json.loads(update_route.calls.last.request.content) == {
        "tags": {"updated": "true"},
        "region": "iad1",
    }


@respx.mock
def test_sync_service_region_defaults_fork_from_environment(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_REGION", "cle1")
    fork_route = respx.post("https://sandbox.test/v2/sandboxes/source/fork").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(name="forked", session_id="sbx_fork")
        )
    )

    with session(service_options=_session_options(sync=True)):
        sandbox_sync.fork_sandbox(source_sandbox="source")

    assert json.loads(fork_route.calls.last.request.content) == {"region": "cle1"}


@respx.mock
async def test_network_policy_async_public_flow(mock_env_clear: None) -> None:
    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json={
                **_sandbox_response(),
                "sandbox": {
                    **_sandbox_response()["sandbox"],
                    "networkPolicy": {"mode": "allow-all"},
                },
                "session": {
                    **_sandbox_response()["session"],
                    "networkPolicy": {"mode": "allow-all"},
                },
            },
        )
    )
    get_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json={
                **_sandbox_response(),
                "sandbox": {
                    **_sandbox_response()["sandbox"],
                    "networkPolicy": {
                        "allow": {"docs.example.com": []},
                        "subnets": {"deny": ["192.0.2.0/24"]},
                    },
                },
            },
        )
    )
    update_route = respx.patch("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json={
                "sandbox": {
                    "name": "preview",
                    "currentSessionId": "sbx_123",
                    "networkPolicy": _normalized_network_policy_response(),
                }
            },
        )
    )
    session_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/network-policy"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "session": {
                    **_sandbox_response()["session"],
                    "networkPolicy": {"mode": "deny-all"},
                }
            },
        )
    )

    async with session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(
            name="preview",
            network_policy=NetworkPolicy.allow_all(),
        )
        assert handle.network_policy == NetworkPolicy.allow_all()
        assert handle.current_session is not None
        assert handle.current_session.network_policy == NetworkPolicy.allow_all()

        inspected = await sandbox.get_sandbox(name="preview")
        assert inspected.network_policy == NetworkPolicy.custom(
            allow={"docs.example.com": ()},
            subnets=NetworkPolicySubnets(deny=["192.0.2.0/24"]),
        )

        authored = _authored_network_policy()
        await handle.update(network_policy=authored)
        assert handle.network_policy == _parsed_network_policy_response()

        updated_session = await handle.update_network_policy(NetworkPolicy.deny_all())
        assert updated_session.network_policy == NetworkPolicy.deny_all()
        assert handle.current_session is updated_session

    assert json.loads(create_route.calls.last.request.content)["networkPolicy"] == {
        "mode": "allow-all"
    }
    assert json.loads(update_route.calls.last.request.content)["networkPolicy"] == {
        "allow": {
            "example.com": [],
            "api.example.com": [
                {
                    "match": {
                        "path": {"startsWith": "/v1/"},
                        "method": ["POST"],
                        "queryString": [
                            {
                                "key": {"exact": "stream"},
                                "value": {"regex": "^(true|false)$"},
                            }
                        ],
                        "headers": [
                            {
                                "key": {"exact": "authorization"},
                                "value": {"startsWith": "Bearer "},
                            }
                        ],
                    },
                    "transform": [
                        {
                            "headers": {
                                "Authorization": "Bearer secret",
                                "X-Trace": "one",
                            }
                        },
                        {"headers": {"X-Trace": "two"}},
                    ],
                    "forwardURL": "https://forward-proxy.internal/ingress/",
                },
                {"transform": [{"headers": {"X-Fallback": "fallback"}}]},
            ],
        },
        "subnets": {
            "allow": ["10.0.0.0/8"],
            "deny": ["10.1.0.0/16"],
        },
    }
    assert json.loads(session_route.calls.last.request.content) == {"mode": "deny-all"}
    assert get_route.called


@respx.mock
def test_network_policy_sync_public_parity(mock_env_clear: None) -> None:
    route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json={
                **_sandbox_response(),
                "sandbox": {
                    **_sandbox_response()["sandbox"],
                    "networkPolicy": _normalized_network_policy_response(),
                },
                "session": {
                    **_sandbox_response()["session"],
                    "networkPolicy": _normalized_network_policy_response(),
                },
            },
        )
    )

    with session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(
            name="preview",
            network_policy=_authored_network_policy(),
        )

    assert handle.network_policy == _parsed_network_policy_response()
    assert handle.current_session is not None
    assert handle.current_session.network_policy == handle.network_policy
    assert sandbox_sync.NetworkPolicy is NetworkPolicy
    assert json.loads(route.calls.last.request.content)["networkPolicy"] == {
        "allow": {
            "example.com": [],
            "api.example.com": [
                {
                    "match": {
                        "path": {"startsWith": "/v1/"},
                        "method": ["POST"],
                        "queryString": [
                            {
                                "key": {"exact": "stream"},
                                "value": {"regex": "^(true|false)$"},
                            }
                        ],
                        "headers": [
                            {
                                "key": {"exact": "authorization"},
                                "value": {"startsWith": "Bearer "},
                            }
                        ],
                    },
                    "transform": [
                        {
                            "headers": {
                                "Authorization": "Bearer secret",
                                "X-Trace": "one",
                            }
                        },
                        {"headers": {"X-Trace": "two"}},
                    ],
                    "forwardURL": "https://forward-proxy.internal/ingress/",
                },
                {"transform": [{"headers": {"X-Fallback": "fallback"}}]},
            ],
        },
        "subnets": {
            "allow": ["10.0.0.0/8"],
            "deny": ["10.1.0.0/16"],
        },
    }


@respx.mock
async def test_network_policy_structural_validation(mock_env_clear: None) -> None:
    with pytest.raises(ValueError, match="headers and header_names"):
        NetworkPolicyTransform(headers={"X": "secret"}, header_names=["X"])
    with pytest.raises(ValueError, match="requires a key or value"):
        NetworkPolicyKeyValueMatcher()
    with pytest.raises(ValueError, match="at least one matching dimension"):
        NetworkPolicyRequestMatcher()

    headers = {"X-Secret": "value"}
    rules = [NetworkPolicyRule(transform=[NetworkPolicyTransform(headers=headers)])]
    allow = {"example.com": rules}
    copied = NetworkPolicy.custom(allow=allow)
    headers["X-Secret"] = "changed"
    rules.clear()
    allow.clear()
    assert copied.allow["example.com"][0].transform[0].headers == {"X-Secret": "value"}
    with pytest.raises(TypeError):
        hash(copied)
    with pytest.raises(TypeError):
        copied.allow["other.example.com"] = ()  # type: ignore[index]

    malformed_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    **_sandbox_response(),
                    "sandbox": {
                        **_sandbox_response()["sandbox"],
                        "networkPolicy": {
                            "mode": "custom",
                            "injectionRules": [
                                {
                                    "domain": "example.com",
                                    "headerNames": "X-Secret",
                                }
                            ],
                        },
                    },
                },
            ),
            httpx.Response(
                200,
                json={
                    **_sandbox_response(),
                    "sandbox": {
                        **_sandbox_response()["sandbox"],
                        "networkPolicy": {
                            "allow": {
                                "example.com": [
                                    {
                                        "match": {
                                            "path": {
                                                "exact": "/v1",
                                                "regex": "^/v1$",
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                    },
                },
            ),
        ]
    )
    update_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/network-policy"
    ).mock(return_value=httpx.Response(200, json={"session": _sandbox_response()["session"]}))
    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    async with session(service_options=_session_options()):
        with pytest.raises(TypeError, match="must be a NetworkPolicy"):
            await sandbox.create_sandbox(
                name="preview",
                network_policy={"mode": "allow-all"},  # type: ignore[arg-type]
            )

        with pytest.raises(SandboxResponseError, match="malformed network policy"):
            await sandbox.get_sandbox(name="preview")
        with pytest.raises(SandboxResponseError, match="malformed network policy"):
            await sandbox.get_sandbox(name="preview")

        redacted = NetworkPolicy.custom(
            allow={
                "example.com": [
                    NetworkPolicyRule(transform=[NetworkPolicyTransform(header_names=["X-Secret"])])
                ]
            }
        )
        handle = sandbox.Sandbox(
            payload=SandboxState(
                name="preview",
                current_session_id="sbx_123",
            ),
            service=get_sandbox_service(get_active_session()),
        )
        with pytest.raises(ValueError, match="redacted"):
            await handle.update_network_policy(redacted)

    assert malformed_route.called
    assert not create_route.called
    assert not update_route.called


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
    route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    async with session(service_options=_session_options()):
        await sandbox.create_sandbox(name="preview", source=source)

    assert json.loads(route.calls.last.request.content)["source"] == expected


@respx.mock
@pytest.mark.parametrize(
    "image",
    [
        "my-repository",
        "my-repository:latest",
        "my-repository@sha256:request-digest",
        "vcr.vercel.com/team-slug/project-slug/my-repository:latest",
    ],
)
async def test_public_create_sandbox_serializes_image_without_runtime(
    mock_env_clear: None,
    image: str,
) -> None:
    route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_image_sandbox_response())
    )

    async with session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", image=image)

    assert json.loads(route.calls.last.request.content) == {
        "projectId": "prj_123",
        "name": "preview",
        "image": image,
    }
    assert handle.image == "my-repository@sha256:resolved"
    assert not hasattr(handle, "runtime")
    assert handle.current_session is not None
    assert not hasattr(handle.current_session, "runtime")


@respx.mock
def test_sync_create_sandbox_serializes_image_without_runtime(mock_env_clear: None) -> None:
    image = "my-repository:latest"
    route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_image_sandbox_response())
    )

    with session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", image=image)

    assert json.loads(route.calls.last.request.content) == {
        "projectId": "prj_123",
        "name": "preview",
        "image": image,
    }
    assert handle.image == "my-repository@sha256:resolved"
    assert not hasattr(handle, "runtime")
    assert handle.current_session is not None
    assert not hasattr(handle.current_session, "runtime")


@respx.mock
async def test_public_create_rejects_malformed_success_response(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(return_value=httpx.Response(200, json={}))

    async with session(service_options=_session_options()):
        with pytest.raises(SandboxResponseError):
            await sandbox.create_sandbox(name="preview")


@respx.mock
async def test_public_snapshot_expiration_validation_happens_before_requests(
    mock_env_clear: None,
) -> None:
    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    update_route = respx.patch("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    snapshot_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/snapshot").mock(
        return_value=httpx.Response(
            201,
            json={**_snapshot_response(), "session": _sandbox_response()["session"]},
        )
    )

    async with session(service_options=_session_options()):
        with pytest.raises(ValueError):
            sandbox.create_sandbox(snapshot_expiration=1)
        handle = await sandbox.get_sandbox(name="preview")
        with pytest.raises(ValueError):
            await handle.update(snapshot_expiration=1)
        with pytest.raises(ValueError):
            await handle.snapshot(expiration=1)

    assert not create_route.called
    assert not update_route.called
    assert not snapshot_route.called


@respx.mock
async def test_public_create_rejects_terminal_initial_state(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )

    async with session(service_options=_session_options()):
        with pytest.raises(SandboxTerminalStateError) as exc_info:
            await sandbox.create_sandbox(name="preview")

    assert exc_info.value.status is SandboxStatus.STOPPED
    assert isinstance(exc_info.value.sandbox, sandbox.Sandbox)
    assert exc_info.value.sandbox.name == "preview"


@respx.mock
def test_sync_create_terminal_error_contains_sync_handle(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )
    )

    with session(service_options=_session_options()):
        with pytest.raises(SandboxTerminalStateError) as exc_info:
            sandbox_sync.create_sandbox(name="preview")

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
    extend_route = respx.post(
        "https://sandbox.test/v2/sandboxes/sessions/sbx_123/extend-timeout"
    ).mock(return_value=httpx.Response(200, json={"session": _sandbox_response()["session"]}))
    snapshot_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/snapshot").mock(
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

    async with session(service_options=_session_options()):
        service = get_sandbox_service(get_active_session())
        state = await service.get_sandbox(name="preview")
        assert isinstance(state, SandboxState)
        assert isinstance(state.current_session, SandboxRuntimeSessionState)
        assert state.execution_time_limit == timedelta(minutes=5)
        assert state.snapshot_expiration == timedelta(0)
        assert state.snapshot_retention == SnapshotRetentionState(
            count=2,
            expiration=timedelta(days=1),
            delete_evicted=False,
        )
        assert state.raw is not None
        assert state.raw["timeout"] == 300000
        assert state.raw["snapshotExpiration"] == 0
        assert state.raw["keepLastSnapshots"] == {
            "count": 2,
            "expiration": 86400000,
            "deleteEvicted": False,
        }
        assert state.created_at == 1
        page_state = await service.query_sandboxes_page()
        assert isinstance(page_state.sandboxes[0], SandboxState)

        handle = await sandbox.get_sandbox(name="preview")
        assert isinstance(handle, sandbox.Sandbox)
        assert isinstance(handle.current_session, sandbox.SandboxRuntimeSession)
        assert isinstance(await handle.create_process("python"), sandbox.Process)
        updated_session = await handle.extend_execution_time_limit(2.5)
        assert isinstance(updated_session, sandbox.SandboxRuntimeSession)
        assert updated_session.execution_time_limit == timedelta(minutes=5)
        assert isinstance(await handle.snapshot(expiration=86400.5), sandbox.Snapshot)
        page = [item async for item in sandbox.query_sandboxes()]
        assert isinstance(page[0], sandbox.Sandbox)

    assert json.loads(extend_route.calls.last.request.content) == {"duration": 2500}
    assert json.loads(snapshot_route.calls.last.request.content) == {"expiration": 86400500}


@respx.mock
def test_sync_runtime_binds_only_sync_handles(mock_env_clear: None) -> None:
    assert not hasattr(sandbox_sync, "Process")
    update_requests: list[httpx.Request] = []

    def update_handler(request: httpx.Request) -> httpx.Response:
        update_requests.append(request)
        return httpx.Response(
            200,
            json={"sandbox": {"name": "preview", "currentSessionId": "sbx_123"}},
        )

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.patch("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=update_handler)
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        return_value=httpx.Response(200, json=_command_response())
    )
    snapshot_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/snapshot").mock(
        return_value=httpx.Response(
            201,
            json={**_snapshot_response(), "session": _sandbox_response()["session"]},
        )
    )

    with session(service_options=_session_options()):
        handle = sandbox_sync.get_sandbox(name="preview")
        assert isinstance(handle, sandbox_sync.SyncSandbox)
        assert isinstance(handle.current_session, sandbox_sync.SyncSandboxRuntimeSession)
        assert isinstance(handle.create_process("python"), sandbox_sync.SyncProcess)
        assert isinstance(handle.snapshot(expiration=timedelta(days=1)), sandbox_sync.SyncSnapshot)
        handle.update(tags={})
        handle.update(snapshot_retention=None)

    assert json.loads(snapshot_route.calls.last.request.content) == {"expiration": 86400000}
    assert [json.loads(request.content) for request in update_requests] == [
        {"tags": {}},
        {"keepLastSnapshots": None},
    ]


@respx.mock
async def test_async_command_kill_after_encodes_seconds_and_timedelta(
    mock_env_clear: None,
) -> None:
    requests: list[httpx.Request] = []

    def command_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.params.get("logs") == "true":
            return _logs_response(
                _command_response(),
                _command_response(exit_code=0),
            )
        return httpx.Response(200, json=_command_response())

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        side_effect=command_handler
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd/cmd_123").mock(
        return_value=httpx.Response(200, json=_command_response(exit_code=0))
    )

    async with session(service_options=_session_options()):
        handle = await sandbox.get_sandbox(name="preview")
        await handle.run_process("sleep", ["60"], kill_after=2.5)
        await handle.create_process("sleep", ["60"], kill_after=timedelta(seconds=3.25))
        assert handle.current_session is not None
        await handle.current_session.create_process("sleep", ["60"], kill_after=4)

    assert [json.loads(request.content) for request in requests] == [
        {
            "command": "sleep",
            "args": ["60"],
            "sudo": False,
            "wait": True,
            "logs": True,
            "timeout": 2500,
        },
        {"command": "sleep", "args": ["60"], "sudo": False, "timeout": 3250},
        {"command": "sleep", "args": ["60"], "sudo": False, "timeout": 4000},
    ]


@respx.mock
def test_sync_command_kill_after_encodes_seconds_and_omits_none(mock_env_clear: None) -> None:
    requests: list[httpx.Request] = []

    def command_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_command_response())

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/cmd").mock(
        side_effect=command_handler
    )

    with session(service_options=_session_options()):
        handle = sandbox_sync.get_sandbox(name="preview")
        handle.create_process("echo", ["hello"])
        assert handle.current_session is not None
        handle.current_session.create_process("sleep", ["60"], kill_after=1.5)

    assert [json.loads(request.content) for request in requests] == [
        {"command": "echo", "args": ["hello"], "sudo": False},
        {"command": "sleep", "args": ["60"], "sudo": False, "timeout": 1500},
    ]


@respx.mock
async def test_session_closure_during_create_polling_is_rejected(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="pending", session_status="pending"),
        )
    )

    async with session(service_options=_session_options()):
        active_session = get_active_session()
        operation = asyncio.create_task(
            get_sandbox_service(active_session).create_sandbox(name="preview")
        )
        await asyncio.sleep(0)
        await active_session.aclose()

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

    async with session(service_options=_session_options()):
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

    async with session(service_options=_session_options()):
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

    async with session(service_options=_session_options()):
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

    async with session(service_options=_session_options()):
        async for handle in sandbox.query_sandboxes(page_size=2):
            handles.append(handle)
            break

    assert [handle.name for handle in handles] == ["preview-1"]
    assert route.call_count == 1


async def test_query_sandboxes_rejects_invalid_page_size(mock_env_clear: None) -> None:
    async with session(service_options=_session_options()):
        with pytest.raises(ValueError, match="page_size"):
            [handle async for handle in sandbox.query_sandboxes(page_size=51)]


@respx.mock
async def test_public_api_error_propagates_status_code_code_and_data(mock_env_clear: None) -> None:
    data = {"error": {"code": "bad_request", "message": "unsupported filter"}}
    respx.get("https://sandbox.test/v2/sandboxes").mock(return_value=httpx.Response(400, json=data))

    async with session(service_options=_session_options()):
        with pytest.raises(SandboxApiError) as exc_info:
            [item async for item in sandbox.query_sandboxes()]

    assert exc_info.value.status_code == 400
    assert exc_info.value.code == "bad_request"
    assert exc_info.value.data == data


@respx.mock
async def test_create_sandbox_operation_invariants(mock_env_clear: None) -> None:
    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    resume_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            200,
            json={
                "session": _sandbox_response(status="stopped", session_status="stopped")["session"]
            },
        )
    )
    destroy_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(status="stopped", session_status="stopped")
        )
    )

    async with session(service_options=_session_options()):
        operation = sandbox.create_sandbox(name="preview")
        created = await operation
        assert created.current_session is not None
        with pytest.raises(RuntimeError, match="can only be used once"):
            await operation

        resume_operation = sandbox.resume_sandbox(name="preview")
        resumed = await resume_operation
        assert resumed.current_session is not None
        with pytest.raises(RuntimeError, match="can only be used once"):
            await resume_operation

    assert create_route.called
    assert dict(resume_route.calls.last.request.url.params)["resume"] == "true"
    assert not stop_route.called
    assert not destroy_route.called

    async with session():
        captured = sandbox.create_sandbox(name="preview")
        captured_resume = sandbox.resume_sandbox(name="preview")

    with pytest.raises(VercelSessionClosedError):
        await captured
    with pytest.raises(VercelSessionClosedError):
        await captured_resume

    with pytest.warns(RuntimeWarning, match="never awaited or entered"):
        unconsumed = sandbox.create_sandbox(name="preview")
        del unconsumed
        gc.collect()
    with pytest.warns(RuntimeWarning, match="never awaited or entered"):
        unconsumed_resume = sandbox.resume_sandbox(name="preview")
        del unconsumed_resume
        gc.collect()


@respx.mock
async def test_async_get_or_create_returns_existing_or_created_sandbox(
    mock_env_clear: None,
) -> None:
    existing_get = respx.get("https://sandbox.test/v2/sandboxes/existing").mock(
        return_value=httpx.Response(
            200,
            json={**_sandbox_response(name="existing", session_id="sbx_existing"), "resumed": True},
        )
    )
    missing_get = respx.get("https://sandbox.test/v2/sandboxes/missing").mock(
        return_value=httpx.Response(
            404,
            json={"error": {"code": "not_found", "message": "not found"}},
        )
    )

    def create_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body == {
            "projectId": "prj_other",
            "name": "missing",
            "persistent": True,
            "tags": {"purpose": "test"},
        }
        return httpx.Response(
            200,
            json=_sandbox_response(name="missing", session_id="sbx_missing"),
        )

    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(side_effect=create_handler)

    async with session(service_options=_session_options()):
        existing, existing_created = await sandbox.get_or_create_sandbox(
            name="existing",
            project_id="prj_other",
            image="existing-repository:latest",
        )
        created, was_created = await sandbox.get_or_create_sandbox(
            name="missing",
            project_id="prj_other",
            persistent=True,
            tags={"purpose": "test"},
        )

    assert existing.name == "existing"
    assert existing_created is False
    assert created.name == "missing"
    assert was_created is True
    assert existing_get.calls.last.request.url.params["resume"] == "true"
    assert missing_get.calls.last.request.url.params["resume"] == "true"
    assert create_route.call_count == 1


@respx.mock
async def test_async_get_or_create_forwards_image_only_when_creating(
    mock_env_clear: None,
) -> None:
    existing_get = respx.get("https://sandbox.test/v2/sandboxes/existing-image").mock(
        return_value=httpx.Response(
            200,
            json={
                **_image_sandbox_response(name="existing-image", session_id="sbx_existing"),
                "resumed": True,
            },
        )
    )
    missing_get = respx.get("https://sandbox.test/v2/sandboxes/missing-image").mock(
        return_value=httpx.Response(
            404,
            json={"error": {"code": "not_found", "message": "not found"}},
        )
    )
    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json=_image_sandbox_response(name="missing-image", session_id="sbx_missing"),
        )
    )

    async with session(service_options=_session_options()):
        existing, existing_created = await sandbox.get_or_create_sandbox(
            name="existing-image",
            image="ignored-repository:latest",
        )
        created, created_flag = await sandbox.get_or_create_sandbox(
            name="missing-image",
            image="requested-repository:latest",
        )

    assert existing_created is False
    assert existing.image == "my-repository@sha256:resolved"
    assert created_flag is True
    assert created.image == "my-repository@sha256:resolved"
    assert existing_get.called
    assert missing_get.called
    assert create_route.call_count == 1
    assert json.loads(create_route.calls.last.request.content) == {
        "projectId": "prj_123",
        "name": "missing-image",
        "image": "requested-repository:latest",
    }


@respx.mock
async def test_async_get_or_create_honors_resume_false(mock_env_clear: None) -> None:
    get_route = respx.get("https://sandbox.test/v2/sandboxes/existing").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(
                name="existing",
                session_id="sbx_existing",
                status="stopped",
                session_status="stopped",
            ),
        )
    )

    async with session(service_options=_session_options()):
        existing, created = await sandbox.get_or_create_sandbox(
            name="existing",
            resume=False,
        )

    assert existing.name == "existing"
    assert created is False
    assert get_route.calls.last.request.url.params["resume"] == "false"


@respx.mock
async def test_async_get_or_create_recreates_stale_snapshot(mock_env_clear: None) -> None:
    get_route = respx.get("https://sandbox.test/v2/sandboxes/stale").mock(
        return_value=httpx.Response(
            410,
            json={
                "error": {
                    "code": "snapshot_not_found",
                    "message": "Cannot resume sandbox: no snapshot available.",
                }
            },
        )
    )
    delete_route = respx.delete("https://sandbox.test/v2/sandboxes/stale").mock(
        return_value=httpx.Response(
            404,
            json={"error": {"code": "not_found", "message": "already deleted"}},
        )
    )

    def create_handler(request: httpx.Request) -> httpx.Response:
        assert json.loads(request.content) == {
            "projectId": "prj_123",
            "name": "stale",
            "image": "stale-repository:latest",
            "__networkId": "network_123",
        }
        return httpx.Response(
            200,
            json=_image_sandbox_response(name="stale", session_id="sbx_recreated"),
        )

    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(side_effect=create_handler)

    async with session(service_options=_session_options()):
        recreated, created = await sandbox.get_or_create_sandbox(
            name="stale",
            image="stale-repository:latest",
            __networkId="network_123",
        )

    assert recreated.name == "stale"
    assert created is True
    assert recreated.image == "my-repository@sha256:resolved"
    assert not hasattr(recreated, "runtime")
    assert get_route.calls.last.request.url.params["resume"] == "true"
    assert get_route.calls.last.request.url.params["__networkId"] == "network_123"
    assert "__networkId" not in delete_route.calls.last.request.url.params
    assert delete_route.call_count == 1
    assert create_route.call_count == 1


@respx.mock
async def test_async_get_or_create_propagates_other_get_errors(mock_env_clear: None) -> None:
    get_route = respx.get("https://sandbox.test/v2/sandboxes/forbidden").mock(
        return_value=httpx.Response(
            403,
            json={"error": {"code": "forbidden", "message": "nope"}},
        )
    )
    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(500)
    )

    async with session(service_options=_session_options()):
        with pytest.raises(SandboxApiError) as exc_info:
            await sandbox.get_or_create_sandbox(name="forbidden")

    assert exc_info.value.status_code == 403
    assert get_route.call_count == 1
    assert create_route.call_count == 0


@respx.mock
def test_sync_get_or_create_defaults_to_resume_and_returns_created_flag(
    mock_env_clear: None,
) -> None:
    requests: list[httpx.Request] = []

    def get_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/existing"):
            return httpx.Response(
                200,
                json=_sandbox_response(name="existing", session_id="sbx_existing"),
            )
        return httpx.Response(
            404,
            json={"error": {"code": "not_found", "message": "not found"}},
        )

    respx.get("https://sandbox.test/v2/sandboxes/existing").mock(side_effect=get_handler)
    respx.get("https://sandbox.test/v2/sandboxes/missing").mock(side_effect=get_handler)

    def create_handler(request: httpx.Request) -> httpx.Response:
        assert json.loads(request.content) == {
            "projectId": "prj_123",
            "name": "missing",
            "image": "missing-repository:latest",
        }
        return httpx.Response(
            200,
            json=_image_sandbox_response(name="missing", session_id="sbx_missing"),
        )

    create_route = respx.post("https://sandbox.test/v3/sandboxes").mock(side_effect=create_handler)

    with session(service_options=_session_options()):
        existing, existing_created = sandbox_sync.get_or_create_sandbox(
            name="existing", image="existing-repository:latest"
        )
        created, was_created = sandbox_sync.get_or_create_sandbox(
            name="missing", image="missing-repository:latest"
        )

    assert existing.name == "existing"
    assert existing_created is False
    assert created.name == "missing"
    assert was_created is True
    assert not hasattr(existing, "__enter__")
    assert not hasattr(created, "__enter__")
    assert created.image == "my-repository@sha256:resolved"
    assert [request.url.params["resume"] for request in requests] == ["true", "true"]
    assert create_route.call_count == 1


@respx.mock
async def test_async_get_fetches_and_resume_ensures_active_session(
    mock_env_clear: None,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.params["resume"] == "true":
            return httpx.Response(200, json=_sandbox_response(session_id="sbx_resumed"))
        return httpx.Response(
            200,
            json=_sandbox_response(status="stopped", session_status="stopped"),
        )

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=handler)

    async with session(service_options=_session_options()):
        fetched = await sandbox.get_sandbox(
            name="preview",
            project_id="prj_other",
            include_system_routes=True,
        )
        resumed = await sandbox.resume_sandbox(
            name="preview",
            project_id="prj_other",
            include_system_routes=True,
        )

    assert fetched.current_session is not None
    assert fetched.current_session.status is SandboxStatus.STOPPED
    assert resumed.current_session is not None
    assert resumed.current_session.id == "sbx_resumed"
    assert [dict(request.url.params) for request in requests] == [
        {
            "teamId": "team_123",
            "projectId": "prj_other",
            "resume": "false",
            "__includeSystemRoutes": "true",
        },
        {
            "teamId": "team_123",
            "projectId": "prj_other",
            "resume": "true",
            "__includeSystemRoutes": "true",
        },
    ]


@respx.mock
def test_sync_get_is_plain_handle_and_create_resume_are_managed(
    mock_env_clear: None,
) -> None:
    requests: list[httpx.Request] = []

    def get_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_sandbox_response())

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=get_handler)
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            200,
            json={
                "session": _sandbox_response(status="stopped", session_status="stopped")["session"]
            },
        )
    )
    destroy_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(status="stopped", session_status="stopped")
        )
    )

    with session(service_options=_session_options()):
        fetched = sandbox_sync.get_sandbox(name="preview")
        created = sandbox_sync.create_sandbox(name="preview")
        resumed = sandbox_sync.resume_sandbox(name="preview")

    assert not hasattr(fetched, "__enter__")
    assert hasattr(created, "__enter__")
    assert hasattr(resumed, "__enter__")
    assert created.image == "vercel/sandbox/universal:latest"
    assert not hasattr(created, "runtime")
    assert created.current_session is not None
    assert not hasattr(created.current_session, "runtime")
    assert [request.url.params["resume"] for request in requests] == ["false", "true"]
    assert not stop_route.called
    assert not destroy_route.called


@respx.mock
async def test_closed_session_rejects_handles_and_lazy_readers(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
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

    async with session(service_options=_session_options()):
        service = get_sandbox_service(get_active_session())
        handle = await sandbox.create_sandbox(name="preview")
        resumed = await sandbox.resume_sandbox(name="preview")
        assert resumed.current_session is not None
        runtime_session = resumed.current_session
        command = await handle.create_process("sleep", ["30"])
        snapshot = await handle.snapshot()
        assert snapshot.region == "iad1"
        assert snapshot.regions == ("iad1", "sfo1", "cle1")

    with pytest.raises(VercelSessionClosedError):
        await handle.create_process("true")
    with pytest.raises(VercelSessionClosedError):
        await runtime_session.create_process("true")
    with pytest.raises(VercelSessionClosedError):
        await command.refresh()
    assert command.stdout is not None
    with pytest.raises(VercelSessionClosedError):
        # The reader opens its log response lazily, so the first read observes
        # the closed session.
        await command.stdout.read()
    with pytest.raises(VercelSessionClosedError):
        await snapshot.delete()
    with pytest.raises(VercelSessionClosedError):
        await service.get_sandbox(name="preview")


@respx.mock
async def test_async_managed_sandbox_cleanup_modes(mock_env_clear: None) -> None:
    events: list[str] = []

    def create_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        name = body["name"]
        return httpx.Response(200, json=_sandbox_response(name=name, session_id=f"sbx_{name}"))

    respx.post("https://sandbox.test/v3/sandboxes").mock(side_effect=create_handler)
    respx.get("https://sandbox.test/v2/sandboxes/resumed").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(name="resumed", session_id="sbx_resumed")
        )
    )

    def stop_handler(request: httpx.Request, *, name: str) -> httpx.Response:
        events.append(f"stop:{name}")
        return httpx.Response(
            200,
            json={
                "session": _sandbox_response(
                    name=name,
                    session_id=f"sbx_{name}",
                    status="stopped",
                    session_status="stopped",
                )["session"]
            },
        )

    for name in ("default", "retained", "resumed"):
        respx.post(f"https://sandbox.test/v2/sandboxes/sessions/sbx_{name}/stop").mock(
            side_effect=lambda request, name=name: stop_handler(request, name=name)
        )

    def destroy_handler(_request: httpx.Request) -> httpx.Response:
        events.append("destroy:default")
        return httpx.Response(
            200,
            json=_sandbox_response(
                name="default",
                session_id="sbx_default",
                status="stopped",
                session_status="stopped",
            ),
        )

    default_destroy = respx.delete("https://sandbox.test/v2/sandboxes/default").mock(
        side_effect=destroy_handler
    )
    retained_destroy = respx.delete("https://sandbox.test/v2/sandboxes/retained").mock(
        return_value=httpx.Response(500)
    )
    resumed_destroy = respx.delete("https://sandbox.test/v2/sandboxes/resumed").mock(
        return_value=httpx.Response(500)
    )

    async with session(service_options=_session_options()):
        async with sandbox.create_sandbox(name="default") as default:
            pass
        async with sandbox.create_sandbox(name="retained", destroy=False) as retained:
            pass
        async with sandbox.resume_sandbox(name="resumed") as resumed:
            pass

    assert events == [
        "stop:default",
        "destroy:default",
        "stop:retained",
        "stop:resumed",
    ]
    assert default.current_session is not None
    assert default.current_session.status is SandboxStatus.STOPPED
    assert retained.current_session is not None
    assert retained.current_session.status is SandboxStatus.STOPPED
    assert resumed.current_session is not None
    assert resumed.current_session.status is SandboxStatus.STOPPED
    assert default_destroy.called
    assert not retained_destroy.called
    assert not resumed_destroy.called


@respx.mock
def test_sync_managed_sandbox_cleanup_modes(mock_env_clear: None) -> None:
    events: list[str] = []

    def create_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        name = body["name"]
        return httpx.Response(200, json=_sandbox_response(name=name, session_id=f"sbx_{name}"))

    respx.post("https://sandbox.test/v3/sandboxes").mock(side_effect=create_handler)
    respx.get("https://sandbox.test/v2/sandboxes/resumed").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(name="resumed", session_id="sbx_resumed")
        )
    )

    def stop_handler(request: httpx.Request, *, name: str) -> httpx.Response:
        events.append(f"stop:{name}")
        return httpx.Response(
            200,
            json={
                "session": _sandbox_response(
                    name=name,
                    session_id=f"sbx_{name}",
                    status="stopped",
                    session_status="stopped",
                )["session"]
            },
        )

    for name in ("default", "retained", "resumed"):
        respx.post(f"https://sandbox.test/v2/sandboxes/sessions/sbx_{name}/stop").mock(
            side_effect=lambda request, name=name: stop_handler(request, name=name)
        )

    def destroy_handler(_request: httpx.Request) -> httpx.Response:
        events.append("destroy:default")
        return httpx.Response(
            200,
            json=_sandbox_response(
                name="default",
                session_id="sbx_default",
                status="stopped",
                session_status="stopped",
            ),
        )

    default_destroy = respx.delete("https://sandbox.test/v2/sandboxes/default").mock(
        side_effect=destroy_handler
    )
    retained_destroy = respx.delete("https://sandbox.test/v2/sandboxes/retained").mock(
        return_value=httpx.Response(500)
    )
    resumed_destroy = respx.delete("https://sandbox.test/v2/sandboxes/resumed").mock(
        return_value=httpx.Response(500)
    )

    with session(service_options=_session_options()):
        with sandbox_sync.create_sandbox(name="default") as default:
            pass
        with sandbox_sync.create_sandbox(name="retained", destroy=False) as retained:
            pass
        with sandbox_sync.resume_sandbox(name="resumed") as resumed:
            pass

    assert events == [
        "stop:default",
        "destroy:default",
        "stop:retained",
        "stop:resumed",
    ]
    assert default.current_session is not None
    assert default.current_session.status is SandboxStatus.STOPPED
    assert retained.current_session is not None
    assert retained.current_session.status is SandboxStatus.STOPPED
    assert resumed.current_session is not None
    assert resumed.current_session.status is SandboxStatus.STOPPED
    assert default_destroy.called
    assert not retained_destroy.called
    assert not resumed_destroy.called


@respx.mock
async def test_async_context_cleanup_wraps_api_failure(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            200,
            json={
                "session": _sandbox_response(status="stopped", session_status="stopped")["session"]
            },
        )
    )
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            500,
            json={"error": {"code": "sandbox_failed", "message": "delete failed"}},
        )
    )

    async with session(service_options=_session_options()):
        with pytest.raises(SandboxCleanupError) as exc_info:
            async with sandbox.create_sandbox(name="preview"):
                pass

    assert exc_info.value.resource_type == "sandbox"
    assert exc_info.value.resource_id == "preview"
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "sandbox_failed"


@respx.mock
def test_sync_context_cleanup_wraps_api_failure(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            200,
            json={
                "session": _sandbox_response(status="stopped", session_status="stopped")["session"]
            },
        )
    )
    respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            500,
            json={"error": {"code": "sandbox_failed", "message": "delete failed"}},
        )
    )

    with session(service_options=_session_options()):
        with pytest.raises(SandboxCleanupError) as exc_info:
            with sandbox_sync.create_sandbox(name="preview"):
                pass

    assert exc_info.value.resource_type == "sandbox"
    assert exc_info.value.resource_id == "preview"
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "sandbox_failed"


@respx.mock
async def test_create_cleanup_attempts_destroy_after_stop_failure(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            500,
            json={"error": {"code": "stop_failed", "message": "stop failed"}},
        )
    )
    destroy_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(status="stopped", session_status="stopped")
        )
    )

    async with session(service_options=_session_options()):
        with pytest.raises(SandboxCleanupError) as exc_info:
            async with sandbox.create_sandbox(name="preview"):
                pass

    assert destroy_route.called
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "stop_failed"


@respx.mock
def test_sync_create_cleanup_attempts_destroy_after_stop_failure(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            500,
            json={"error": {"code": "stop_failed", "message": "stop failed"}},
        )
    )
    destroy_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200, json=_sandbox_response(status="stopped", session_status="stopped")
        )
    )

    with session(service_options=_session_options()):
        with pytest.raises(SandboxCleanupError) as exc_info:
            with sandbox_sync.create_sandbox(name="preview"):
                pass

    assert destroy_route.called
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "stop_failed"


def _install_public_recovery_smoke_routes() -> list[str]:
    events: list[str] = []

    def sandbox_handler(request: httpx.Request) -> httpx.Response:
        resume = request.url.params["resume"] == "true"
        events.append("resume" if resume else "lookup")
        session_id = "sbx_new" if resume else "sbx_old"
        return httpx.Response(200, json=_sandbox_response(session_id=session_id))

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=sandbox_handler)
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            409,
            json={"error": {"code": "sandbox_stopped", "message": "stopped"}},
        )
    )

    def replacement_handler(_request: httpx.Request) -> httpx.Response:
        events.append("replay")
        return httpx.Response(200, json=_command_response(session_id="sbx_new"))

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        side_effect=replacement_handler
    )
    return events


@respx.mock
async def test_stopped_async_sandbox_recovers_one_public_operation(
    mock_env_clear: None,
) -> None:
    events = _install_public_recovery_smoke_routes()

    async with session(service_options=_session_options()):
        handle = await sandbox.get_sandbox(name="preview")
        process = await handle.create_process("python", ["--version"])

    assert process.session_id == handle.current_session_id == "sbx_new"
    assert events == ["lookup", "resume", "replay"]


@respx.mock
def test_stopped_sync_sandbox_recovers_one_public_operation(
    mock_env_clear: None,
) -> None:
    events = _install_public_recovery_smoke_routes()

    with session(service_options=_session_options()):
        handle = sandbox_sync.get_sandbox(name="preview")
        process = handle.create_process("python", ["--version"])

    assert process.session_id == handle.current_session_id == "sbx_new"
    assert events == ["lookup", "resume", "replay"]


_RECOVERABLE_SANDBOX_OPERATIONS = (
    (
        "get_process",
        "get",
        "v2/sandboxes/sessions/sbx_old/cmd/cmd_123",
        "v2/sandboxes/sessions/sbx_new/cmd/cmd_123",
    ),
    (
        "query_processes",
        "get",
        "v2/sandboxes/sessions/sbx_old/cmd",
        "v2/sandboxes/sessions/sbx_new/cmd",
    ),
    (
        "extend_execution_time_limit",
        "post",
        "v2/sandboxes/sessions/sbx_old/extend-timeout",
        "v2/sandboxes/sessions/sbx_new/extend-timeout",
    ),
    (
        "update_network_policy",
        "post",
        "v2/sandboxes/sessions/sbx_old/network-policy",
        "v2/sandboxes/sessions/sbx_new/network-policy",
    ),
    (
        "snapshot",
        "post",
        "v2/sandboxes/sessions/sbx_old/snapshot",
        "v2/sandboxes/sessions/sbx_new/snapshot",
    ),
)


def _recovery_success_response(operation: str, *, session_id: str) -> httpx.Response:
    session = _sandbox_response(session_id=session_id)["session"]
    if operation in {"get_process", "query_processes"}:
        command = _command_response(session_id=session_id)["command"]
        if operation == "get_process":
            return httpx.Response(200, json={"command": command})
        return httpx.Response(200, json={"commands": [command]})
    if operation == "snapshot":
        return httpx.Response(
            201,
            json={
                "snapshot": _snapshot_response(session_id=session_id)["snapshot"],
                "session": session,
            },
        )
    return httpx.Response(200, json={"session": session})


async def _run_async_recoverable_operation(handle: sandbox.Sandbox, operation: str) -> object:
    if operation == "get_process":
        return await handle.get_process("cmd_123")
    if operation == "query_processes":
        return await handle.query_processes()
    if operation == "extend_execution_time_limit":
        return await handle.extend_execution_time_limit(2)
    if operation == "update_network_policy":
        return await handle.update_network_policy(NetworkPolicy.allow_all())
    if operation == "snapshot":
        return await handle.snapshot()
    raise AssertionError(f"Unexpected operation: {operation}")


def _run_sync_recoverable_operation(handle: sandbox_sync.SyncSandbox, operation: str) -> object:
    if operation == "get_process":
        return handle.get_process("cmd_123")
    if operation == "query_processes":
        return handle.query_processes()
    if operation == "extend_execution_time_limit":
        return handle.extend_execution_time_limit(2)
    if operation == "update_network_policy":
        return handle.update_network_policy(NetworkPolicy.allow_all())
    if operation == "snapshot":
        return handle.snapshot()
    raise AssertionError(f"Unexpected operation: {operation}")


@pytest.mark.parametrize(
    ("operation", "method", "old_path", "replacement_path"),
    _RECOVERABLE_SANDBOX_OPERATIONS,
)
@respx.mock
async def test_async_sandbox_replays_each_remaining_covered_operation(
    mock_env_clear: None,
    operation: str,
    method: str,
    old_path: str,
    replacement_path: str,
) -> None:
    events: list[str] = []

    def sandbox_handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["resume"] == "false":
            events.append("lookup")
            return httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
        events.append("resume")
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=sandbox_handler)

    def old_handler(_request: httpx.Request) -> httpx.Response:
        events.append("old")
        return httpx.Response(
            409,
            json={"error": {"code": "sandbox_stopped", "message": "session is stopped"}},
        )

    def replacement_handler(_request: httpx.Request) -> httpx.Response:
        events.append("replacement")
        return _recovery_success_response(operation, session_id="sbx_new")

    getattr(respx, method)(f"https://sandbox.test/{old_path}").mock(side_effect=old_handler)
    getattr(respx, method)(f"https://sandbox.test/{replacement_path}").mock(
        side_effect=replacement_handler
    )

    async with session(service_options=_session_options()):
        handle = await sandbox.get_sandbox(name="preview")
        result = await _run_async_recoverable_operation(handle, operation)

    assert result is not None
    assert handle.current_session_id == "sbx_new"
    assert events == ["lookup", "old", "resume", "replacement"]


@pytest.mark.parametrize(
    ("operation", "method", "old_path", "replacement_path"),
    _RECOVERABLE_SANDBOX_OPERATIONS,
)
@respx.mock
def test_sync_sandbox_replays_each_remaining_covered_operation(
    mock_env_clear: None,
    operation: str,
    method: str,
    old_path: str,
    replacement_path: str,
) -> None:
    events: list[str] = []

    def sandbox_handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["resume"] == "false":
            events.append("lookup")
            return httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
        events.append("resume")
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=sandbox_handler)

    def old_handler(_request: httpx.Request) -> httpx.Response:
        events.append("old")
        return httpx.Response(
            409,
            json={"error": {"code": "sandbox_stopped", "message": "session is stopped"}},
        )

    def replacement_handler(_request: httpx.Request) -> httpx.Response:
        events.append("replacement")
        return _recovery_success_response(operation, session_id="sbx_new")

    getattr(respx, method)(f"https://sandbox.test/{old_path}").mock(side_effect=old_handler)
    getattr(respx, method)(f"https://sandbox.test/{replacement_path}").mock(
        side_effect=replacement_handler
    )

    with session(service_options=_session_options()):
        handle = sandbox_sync.get_sandbox(name="preview")
        result = _run_sync_recoverable_operation(handle, operation)

    assert result is not None
    assert handle.current_session_id == "sbx_new"
    assert events == ["lookup", "old", "resume", "replacement"]


@respx.mock
async def test_async_sparse_sandbox_lookup_targets_current_session_without_resume(
    mock_env_clear: None,
) -> None:
    events: list[str] = []
    sparse_response = _sandbox_response(session_id="sbx_sparse")
    del sparse_response["session"]

    def lookup_handler(request: httpx.Request) -> httpx.Response:
        events.append("lookup")
        assert request.url.params["resume"] == "false"
        return httpx.Response(200, json=sparse_response)

    lookup_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=lookup_handler
    )

    def operation_handler(_request: httpx.Request) -> httpx.Response:
        events.append("operation")
        return httpx.Response(200, json=_command_response(session_id="sbx_sparse"))

    operation_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_sparse/cmd").mock(
        side_effect=operation_handler
    )

    async with session(service_options=_session_options()):
        handle = await sandbox.get_sandbox(name="preview")
        assert handle.current_session_id == "sbx_sparse"
        assert handle.current_session is None
        process = await handle.create_process("python")

    assert process.session_id == "sbx_sparse"
    assert lookup_route.call_count == operation_route.call_count == 1
    assert events == ["lookup", "operation"]


@pytest.mark.parametrize(
    ("include_system_routes", "expected_projection"),
    [(True, "true"), (False, "false"), (None, None)],
)
@respx.mock
async def test_async_sandbox_recovery_preserves_route_projection(
    mock_env_clear: None,
    include_system_routes: bool | None,
    expected_projection: str | None,
) -> None:
    requests: list[httpx.Request] = []

    def sandbox_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        session_id = "sbx_old" if request.url.params["resume"] == "false" else "sbx_new"
        return httpx.Response(
            200,
            json=_sandbox_response(session_id=session_id, project_id="prj_bound"),
        )

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=sandbox_handler)
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            409,
            json={"error": {"code": "sandbox_stopped", "message": "session is stopped"}},
        )
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(session_id="sbx_new"))
    )

    async with session(service_options=_session_options()):
        handle = await sandbox.get_sandbox(
            name="preview",
            project_id="prj_bound",
            include_system_routes=include_system_routes,
        )
        await handle.create_process("python")

    assert [request.url.params["resume"] for request in requests] == ["false", "true"]
    for request in requests:
        assert request.url.params["projectId"] == "prj_bound"
        assert request.url.params.get("__includeSystemRoutes") == expected_projection


async def _run_async_unrecovered_operation(handle: sandbox.Sandbox, operation: str) -> object:
    if operation == "update":
        return await handle.update(ports=[3001])
    if operation == "stop":
        return await handle.stop()
    if operation == "destroy":
        return await handle.destroy()
    if operation == "list_sessions":
        return await handle.list_sessions()
    if operation == "list_snapshots":
        return await handle.list_snapshots()
    raise AssertionError(f"Unexpected operation: {operation}")


@respx.mock
@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_transition_polling_is_cancellable(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
    anyio_backend: str,
) -> None:
    poll_waiting = anyio.Event()
    never_release = anyio.Event()

    async def blocked_delay(_delay: float) -> None:
        poll_waiting.set()
        await never_release.wait()

    monkeypatch.setattr(sandbox_async_runtime.anyio, "sleep", blocked_delay)
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            409,
            json={"error": {"code": "sandbox_stopping", "message": "transitioning"}},
        )
    )
    poll_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old").mock(
        return_value=httpx.Response(500)
    )

    async with session(service_options=_session_options()):
        handle = await sandbox.get_sandbox(name="preview")
        cancel_scope = anyio.CancelScope()

        async def query() -> None:
            with cancel_scope:
                await handle.query_processes()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(query)
            await poll_waiting.wait()
            cancel_scope.cancel()

    assert poll_route.call_count == 0


@respx.mock
@pytest.mark.parametrize("sync", [False, True])
async def test_transition_polling_has_deadline(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
    sync: bool,
) -> None:
    runtime = sandbox_sync_runtime if sync else sandbox_async_runtime
    monkeypatch.setattr(runtime, "TRANSITION_TIMEOUT", -1.0)
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            409,
            json={"error": {"code": "sandbox_stopping", "message": "transitioning"}},
        )
    )
    poll_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old").mock(
        return_value=httpx.Response(500)
    )

    if sync:
        with session(service_options=_session_options(sync=True)):
            sync_handle = sandbox_sync.get_sandbox(name="preview")
            with pytest.raises(SandboxTimeoutError, match="within -1.0s"):
                sync_handle.query_processes()
    else:
        async with session(service_options=_session_options()):
            async_handle = await sandbox.get_sandbox(name="preview")
            with pytest.raises(SandboxTimeoutError, match="within -1.0s"):
                await async_handle.query_processes()

    assert poll_route.call_count == 0


@respx.mock
async def test_async_transition_poll_failure_propagates_without_resuming(
    mock_env_clear: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_delay(_delay: float) -> None:
        return None

    monkeypatch.setattr(sandbox_async_runtime.anyio, "sleep", no_delay)
    sandbox_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            409,
            json={"error": {"code": "sandbox_stopping", "message": "transitioning"}},
        )
    )
    poll_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old").mock(
        return_value=httpx.Response(
            503,
            json={"error": {"code": "poll_failed", "message": "poll failed"}},
        )
    )

    async with session(service_options=_session_options()):
        handle = await sandbox.get_sandbox(name="preview")
        with pytest.raises(SandboxApiError) as exc_info:
            await handle.query_processes()

    assert exc_info.value.code == "poll_failed"
    assert poll_route.call_count == 1
    assert sandbox_route.call_count == 1


def _run_sync_unrecovered_operation(handle: sandbox_sync.SyncSandbox, operation: str) -> object:
    if operation == "update":
        return handle.update(ports=[3001])
    if operation == "stop":
        return handle.stop()
    if operation == "destroy":
        return handle.destroy()
    if operation == "list_sessions":
        return handle.list_sessions()
    if operation == "list_snapshots":
        return handle.list_snapshots()
    raise AssertionError(f"Unexpected operation: {operation}")


@respx.mock
async def test_mutating_handles_reject_mismatched_response_identity(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
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

    async with session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview")
        with pytest.raises(SandboxResponseError):
            await handle.update(ports=[3001])

        command = await handle.create_process("python", ["--version"])
        with pytest.raises(SandboxResponseError):
            await command.refresh()

        snapshot = await handle.snapshot()
        with pytest.raises(SandboxResponseError):
            await snapshot.delete()


def test_sync_query_sandboxes_binds_session_at_iterator_creation(mock_env_clear: None) -> None:
    with session(service_options=_session_options()):
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

    with session(service_options=_session_options()):
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


def _stop_response(
    session_id: str,
    *,
    sandbox_session_id: str | None = None,
) -> dict[str, object]:
    response: dict[str, object] = {
        "session": _sandbox_response(
            session_id=session_id,
            status="stopped",
            session_status="stopped",
        )["session"]
    }
    if sandbox_session_id is not None:
        response["sandbox"] = {
            "name": "preview",
            "currentSessionId": sandbox_session_id,
            "status": "stopped",
            "updatedAt": 99,
        }
    return response


@respx.mock
async def test_async_box_session_acquires_authoritatively_and_canonicalizes(
    mock_env_clear: None,
) -> None:
    calls = iter(
        [
            _sandbox_response(session_id="sbx_old"),
            _sandbox_response(session_id="sbx_old"),
            _sandbox_response(session_id="sbx_new"),
        ]
    )
    route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=lambda _request: httpx.Response(200, json=next(calls))
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview", include_system_routes=True)
        old = box.current_session
        assert old is not None
        acquired = await box.session()
        assert acquired is old is box.current_session
        replacement = await box.session()
        assert replacement is box.current_session
        assert replacement is not old

    assert route.call_count == 3
    assert [call.request.url.params["resume"] for call in route.calls] == [
        "false",
        "true",
        "true",
    ]
    assert all(call.request.url.params["__includeSystemRoutes"] == "true" for call in route.calls)


@respx.mock
def test_sync_box_session_direct_and_managed_identity(mock_env_clear: None) -> None:
    calls = iter(
        [
            _sandbox_response(session_id="sbx_old"),
            _sandbox_response(session_id="sbx_old"),
            _sandbox_response(session_id="sbx_new"),
        ]
    )
    resume_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=lambda _request: httpx.Response(200, json=next(calls))
    )
    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_old"))
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.get_sandbox(name="preview")
        old = box.current_session
        assert old is not None
        acquired = box.session()
        assert acquired is old is box.current_session
        with acquired as entered:
            assert entered is acquired
            replacement = box.session()
            assert replacement is box.current_session
            assert replacement is not acquired

    assert resume_route.call_count == 3
    assert stop_route.call_count == 1
    assert stop_route.calls[0].request.url.path.endswith("/sbx_old/stop")
    assert replacement.status is SandboxStatus.RUNNING


@respx.mock
async def test_async_managed_session_applies_matching_stop_metadata(
    mock_env_clear: None,
) -> None:
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            200,
            json=_stop_response("sbx_123", sandbox_session_id="sbx_123"),
        )
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        routes = box.routes
        raw = box.raw
        project_id = box.project_id
        async with box.session() as acquired:
            assert acquired is box.current_session
        assert acquired.status is SandboxStatus.STOPPED
        assert box.status is SandboxStatus.STOPPED
        assert box.updated_at == 99
        assert box.routes == routes
        assert box.raw is not None
        assert raw is not None
        assert box.project_id == project_id == "prj_123"
        assert box.current_session is acquired

    assert stop_route.call_count == 1


@respx.mock
async def test_async_managed_session_cleanup_never_rolls_back_replacement(
    mock_env_clear: None,
) -> None:
    responses = iter(
        [
            _sandbox_response(session_id="sbx_old"),
            _sandbox_response(session_id="sbx_old"),
            _sandbox_response(session_id="sbx_new"),
        ]
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=lambda _request: httpx.Response(200, json=next(responses))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "stopped"}},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        return_value=httpx.Response(200, json={"commands": []})
    )
    old_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/stop").mock(
        return_value=httpx.Response(
            200,
            json=_stop_response("sbx_old", sandbox_session_id="sbx_old"),
        )
    )
    new_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_new"))
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        async with box.session() as acquired:
            assert await box.query_processes() == []
            replacement = box.current_session
            assert replacement is not None and replacement.id == "sbx_new"
        assert acquired.status is SandboxStatus.STOPPED
        assert box.current_session is replacement
        assert replacement.status is SandboxStatus.RUNNING

    assert old_stop.call_count == 1
    assert new_stop.call_count == 0


@respx.mock
@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize(
    ("status_code", "data"),
    [
        (409, {"error": {"code": "sandbox_stopped", "message": "stopped"}}),
        (410, {"error": {"message": "gone"}}),
    ],
)
async def test_session_cleanup_suppresses_already_stopped(
    mock_env_clear: None,
    sync: bool,
    status_code: int,
    data: dict[str, object],
) -> None:
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            status_code,
            json=data,
        )
    )

    acquired_status: SandboxStatus | None
    if sync:
        with session(service_options=_session_options(sync=True)):
            sync_box = sandbox_sync.get_sandbox(name="preview")
            with sync_box.session() as sync_acquired:
                pass
            with sync_acquired:
                pass
            acquired_status = sync_acquired.status
    else:
        async with session(service_options=_session_options()):
            async_box = await sandbox.get_sandbox(name="preview")
            async with async_box.session() as async_acquired:
                pass
            acquired_status = async_acquired.status

    assert acquired_status is SandboxStatus.STOPPED
    assert stop_route.call_count == 1


@respx.mock
async def test_async_session_cleanup_preserves_block_error_and_warns(
    mock_env_clear: None,
) -> None:
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            500,
            json={"error": {"code": "stop_failed", "message": "failed"}},
        )
    )
    original = ValueError("block failed")

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        with pytest.warns(RuntimeWarning) as warnings_info:
            with pytest.raises(ValueError) as exc_info:
                async with box.session():
                    raise original

    assert exc_info.value is original
    cleanup_error = warnings_info[0].source
    assert isinstance(cleanup_error, SandboxCleanupError)
    assert cleanup_error.resource_type == "sandbox_runtime_session"
    assert cleanup_error.resource_id == "sbx_123"
    assert isinstance(cleanup_error.cause, SandboxApiError)


@respx.mock
async def test_async_session_operation_is_single_use_and_warns_unconsumed(
    mock_env_clear: None,
) -> None:
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        operation = box.session()
        await operation
        with pytest.raises(RuntimeError, match="can only be used once"):
            await operation
        with pytest.warns(RuntimeWarning, match=r"await box\.session"):
            unconsumed = box.session()
            del unconsumed
            gc.collect()


@respx.mock
@pytest.mark.parametrize("error_code", ["sandbox_stopping", "sandbox_snapshotting"])
async def test_async_session_acquisition_polls_transition_then_retries_once(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
    error_code: str,
) -> None:
    async def no_delay(_delay: float) -> None:
        return None

    monkeypatch.setattr(sandbox_async_runtime.anyio, "sleep", no_delay)
    attempts = 0

    def sandbox_handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.params["resume"] == "false":
            return httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
        attempts += 1
        if attempts == 1:
            return httpx.Response(
                409,
                json={
                    "error": {
                        "code": error_code,
                        "message": "transitioning",
                    }
                },
            )
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    sandbox_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=sandbox_handler
    )
    poll_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old").mock(
        return_value=httpx.Response(
            200,
            json={
                "session": _sandbox_response(
                    session_id="sbx_old", status="stopped", session_status="stopped"
                )["session"]
            },
        )
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        acquired = await box.session()

    assert acquired.id == "sbx_new"
    assert sandbox_route.call_count == 3
    assert poll_route.call_count == 1


@respx.mock
@pytest.mark.parametrize("error_code", ["sandbox_stopping", "sandbox_snapshotting"])
def test_sync_session_acquisition_polls_transition_then_retries_once(
    mock_env_clear: None,
    monkeypatch: pytest.MonkeyPatch,
    error_code: str,
) -> None:
    monkeypatch.setattr(sandbox_sync_runtime.time, "sleep", lambda _delay: None)
    attempts = 0

    def sandbox_handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.params["resume"] == "false":
            return httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
        attempts += 1
        if attempts == 1:
            return httpx.Response(
                409,
                json={
                    "error": {
                        "code": error_code,
                        "message": "transitioning",
                    }
                },
            )
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    sandbox_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=sandbox_handler
    )
    poll_route = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old").mock(
        return_value=httpx.Response(
            200,
            json={
                "session": _sandbox_response(
                    session_id="sbx_old", status="stopped", session_status="stopped"
                )["session"]
            },
        )
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.get_sandbox(name="preview")
        acquired = box.session()

    assert acquired.id == "sbx_new"
    assert sandbox_route.call_count == 3
    assert poll_route.call_count == 1


@respx.mock
@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_session_entry_cancellation_abandons_resume_and_owns_no_cleanup(
    mock_env_clear: None,
    anyio_backend: str,
) -> None:
    resume_started = anyio.Event()
    release_resume = anyio.Event()

    respx.get("https://sandbox.test/v2/sandboxes/preview", params={"resume": "false"}).mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
    )

    async def resume_handler(_request: httpx.Request) -> httpx.Response:
        resume_started.set()
        await release_resume.wait()
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    respx.get("https://sandbox.test/v2/sandboxes/preview", params={"resume": "true"}).mock(
        side_effect=resume_handler
    )
    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_new"))
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        cancel_scope = anyio.CancelScope()
        entry_done = anyio.Event()

        async def enter() -> None:
            with cancel_scope:
                try:
                    async with box.session():
                        raise AssertionError("cancelled entry must not yield")
                finally:
                    entry_done.set()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(enter)
            await resume_started.wait()
            cancel_scope.cancel()
            await entry_done.wait()
            release_resume.set()
        assert box.current_session_id == "sbx_old"

    assert stop_route.call_count == 0


@respx.mock
@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_session_cleanup_finishes_before_cancellation_propagates(
    mock_env_clear: None,
    anyio_backend: str,
) -> None:
    entered = anyio.Event()
    stop_started = anyio.Event()
    release_stop = anyio.Event()
    block_forever = anyio.Event()
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    async def stop_handler(_request: httpx.Request) -> httpx.Response:
        stop_started.set()
        await release_stop.wait()
        return httpx.Response(200, json=_stop_response("sbx_123"))

    stop_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        side_effect=stop_handler
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        cancel_scope = anyio.CancelScope()
        managed_done = anyio.Event()

        async def managed() -> None:
            with cancel_scope:
                try:
                    async with box.session():
                        entered.set()
                        await block_forever.wait()
                finally:
                    managed_done.set()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(managed)
            await entered.wait()
            cancel_scope.cancel()
            await stop_started.wait()
            assert not managed_done.is_set()
            release_stop.set()

    assert stop_route.call_count == 1


@respx.mock
def test_listed_sync_session_cleanup_has_no_parent_linkage(mock_env_clear: None) -> None:
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions").mock(
        return_value=httpx.Response(
            200,
            json={
                "sessions": [_sandbox_response(session_id="sbx_old")["session"]],
                "pagination": {"count": 1, "next": None, "prev": None},
            },
        )
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/stop").mock(
        return_value=httpx.Response(
            200,
            json=_stop_response("sbx_old", sandbox_session_id="sbx_old"),
        )
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.get_sandbox(name="preview")
        historical = box.list_sessions()[0]
        with historical:
            pass

    assert historical.status is SandboxStatus.STOPPED
    assert box.status is SandboxStatus.RUNNING
    assert box.current_session_id == "sbx_123"


@respx.mock
@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_session_acquisition_shares_implicit_resume(
    mock_env_clear: None,
    anyio_backend: str,
) -> None:
    resume_started = anyio.Event()
    release_resume = anyio.Event()
    respx.get("https://sandbox.test/v2/sandboxes/preview", params={"resume": "false"}).mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
    )

    async def resume_handler(_request: httpx.Request) -> httpx.Response:
        resume_started.set()
        await release_resume.wait()
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    resume_route = respx.get(
        "https://sandbox.test/v2/sandboxes/preview", params={"resume": "true"}
    ).mock(side_effect=resume_handler)
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "stopped"}},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        return_value=httpx.Response(200, json={"commands": []})
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        processes: list[sandbox.Process] | None = None
        acquired: sandbox.SandboxRuntimeSession | None = None

        async def query() -> None:
            nonlocal processes
            processes = await box.query_processes()

        async def acquire() -> None:
            nonlocal acquired
            acquired = await box.session()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(query)
            await resume_started.wait()
            task_group.start_soon(acquire)
            await anyio.sleep(0)
            release_resume.set()

    assert processes == []
    assert acquired is not None
    assert acquired is box.current_session
    assert acquired.id == "sbx_new"
    assert resume_route.call_count == 1


@respx.mock
def test_sync_managed_session_cleanup_never_rolls_back_replacement(
    mock_env_clear: None,
) -> None:
    responses = iter(
        [
            _sandbox_response(session_id="sbx_old"),
            _sandbox_response(session_id="sbx_old"),
            _sandbox_response(session_id="sbx_new"),
        ]
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=lambda _request: httpx.Response(200, json=next(responses))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "stopped"}},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        return_value=httpx.Response(200, json={"commands": []})
    )
    old_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/stop").mock(
        return_value=httpx.Response(
            200,
            json=_stop_response("sbx_old", sandbox_session_id="sbx_old"),
        )
    )
    new_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_new"))
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.get_sandbox(name="preview")
        with box.session() as acquired:
            assert box.query_processes() == []
            replacement = box.current_session
            assert replacement is not None and replacement.id == "sbx_new"

    assert acquired.status is SandboxStatus.STOPPED
    assert box.current_session is replacement
    assert replacement.status is SandboxStatus.RUNNING
    assert old_stop.call_count == 1
    assert new_stop.call_count == 0


@pytest.mark.parametrize("sandbox_session_id", [None, "sbx_other"])
@respx.mock
def test_sync_session_cleanup_ignores_sparse_nonmatching_metadata(
    mock_env_clear: None,
    sandbox_session_id: str | None,
) -> None:
    get_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            200,
            json=_stop_response("sbx_123", sandbox_session_id=sandbox_session_id),
        )
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.get_sandbox(name="preview")
        with box.session() as acquired:
            pass

    assert acquired.status is SandboxStatus.STOPPED
    assert box.status is SandboxStatus.RUNNING
    assert box.current_session_id == "sbx_123"
    assert get_route.call_count == 2


def _incoherent_stop_response() -> dict[str, object]:
    response = _stop_response("sbx_123", sandbox_session_id="sbx_123")
    sandbox_payload = response["sandbox"]
    assert isinstance(sandbox_payload, dict)
    sandbox_payload["name"] = "other"
    return response


@pytest.mark.parametrize("block_fails", [False, True])
@respx.mock
async def test_async_cleanup_wraps_stop_result_application_failure(
    mock_env_clear: None,
    block_fails: bool,
) -> None:
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(200, json=_incoherent_stop_response())
    )
    original = ValueError("block failed")

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        if block_fails:
            with pytest.warns(RuntimeWarning) as warnings_info:
                with pytest.raises(ValueError) as exc_info:
                    async with box.session():
                        raise original
            assert exc_info.value is original
            cleanup_error = warnings_info[0].source
        else:
            with pytest.raises(SandboxCleanupError) as cleanup_info:
                async with box.session():
                    pass
            cleanup_error = cleanup_info.value

    assert isinstance(cleanup_error, SandboxCleanupError)
    assert cleanup_error.resource_type == "sandbox_runtime_session"
    assert cleanup_error.resource_id == "sbx_123"
    assert isinstance(cleanup_error.cause, SandboxResponseError)


@respx.mock
def test_sync_successful_block_wraps_unrelated_cleanup_failure(
    mock_env_clear: None,
) -> None:
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            503,
            json={"error": {"code": "unavailable", "message": "unavailable"}},
        )
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.get_sandbox(name="preview")
        with pytest.raises(SandboxCleanupError) as exc_info:
            with box.session():
                pass

    assert exc_info.value.resource_type == "sandbox_runtime_session"
    assert exc_info.value.resource_id == "sbx_123"
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "unavailable"


@respx.mock
@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_cancellation_with_cleanup_failure_warns_structured_error(
    mock_env_clear: None,
    anyio_backend: str,
) -> None:
    entered = anyio.Event()
    block_forever = anyio.Event()
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_123/stop").mock(
        return_value=httpx.Response(
            503,
            json={"error": {"code": "stop_failed", "message": "failed"}},
        )
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        cancel_scope = anyio.CancelScope()

        async def managed() -> None:
            with cancel_scope:
                async with box.session():
                    entered.set()
                    await block_forever.wait()

        with pytest.warns(RuntimeWarning) as warnings_info:
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(managed)
                await entered.wait()
                cancel_scope.cancel()

    cleanup_error = warnings_info[0].source
    assert isinstance(cleanup_error, SandboxCleanupError)
    assert cleanup_error.resource_id == "sbx_123"
    assert isinstance(cleanup_error.cause, SandboxApiError)
    assert cleanup_error.cause.code == "stop_failed"


@respx.mock
def test_sync_session_acquisition_shares_implicit_resume(
    mock_env_clear: None,
) -> None:
    resume_started = Event()
    acquire_joined = Event()
    release_resume = Event()

    class ObservedCondition(Condition):
        def wait(self, timeout: float | None = None) -> bool:
            acquire_joined.set()
            return super().wait(timeout)

    respx.get("https://sandbox.test/v2/sandboxes/preview", params={"resume": "false"}).mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
    )

    def resume_handler(_request: httpx.Request) -> httpx.Response:
        resume_started.set()
        assert release_resume.wait(timeout=5)
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    resume_route = respx.get(
        "https://sandbox.test/v2/sandboxes/preview", params={"resume": "true"}
    ).mock(side_effect=resume_handler)
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "stopped"}},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        return_value=httpx.Response(200, json={"commands": []})
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.get_sandbox(name="preview")
        box._recovery_condition = ObservedCondition()
        with ThreadPoolExecutor(max_workers=2) as executor:
            implicit = executor.submit(box.query_processes)
            assert resume_started.wait(timeout=5)
            explicit = executor.submit(box.session)
            assert acquire_joined.wait(timeout=5)
            release_resume.set()
            assert implicit.result(timeout=5) == []
            acquired = explicit.result(timeout=5)

    assert acquired is box.current_session
    assert acquired.id == "sbx_new"
    assert resume_route.call_count == 1


@respx.mock
def test_sync_managed_resume_sandbox_stops_adopted_replacement(
    mock_env_clear: None,
) -> None:
    responses = iter(
        [
            _sandbox_response(session_id="sbx_old"),
            _sandbox_response(session_id="sbx_new"),
        ]
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        side_effect=lambda _request: httpx.Response(200, json=next(responses))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "stopped"}},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        return_value=httpx.Response(200, json={"commands": []})
    )
    old_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_old"))
    )
    new_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_new"))
    )

    with session(service_options=_session_options()):
        with sandbox_sync.resume_sandbox(name="preview") as box:
            assert box.query_processes() == []
            assert box.current_session_id == "sbx_new"

    assert old_stop.call_count == 0
    assert new_stop.call_count == 1


@respx.mock
async def test_managed_create_sandbox_stops_replacement_before_destroy(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
    )
    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "stopped"}},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        return_value=httpx.Response(200, json={"commands": []})
    )
    old_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_old"))
    )
    new_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_new"))
    )
    destroy_route = respx.delete("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(
                session_id="sbx_new", status="stopped", session_status="stopped"
            ),
        )
    )

    async with session(service_options=_session_options()):
        async with sandbox.create_sandbox(name="preview") as box:
            assert await box.query_processes() == []

    assert old_stop.call_count == 0
    assert new_stop.call_count == 1
    assert destroy_route.call_count == 1


@respx.mock
async def test_async_managed_exit_does_not_wait_for_racing_recovery(
    mock_env_clear: None,
) -> None:
    resume_started = asyncio.Event()
    release_resume = asyncio.Event()
    resume_count = 0

    async def sandbox_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal resume_count
        resume_count += 1
        if resume_count == 1:
            return httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
        resume_started.set()
        await release_resume.wait()
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=sandbox_handler)
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "stopped"}},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        return_value=httpx.Response(200, json={"commands": []})
    )
    old_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_old"))
    )
    new_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_new"))
    )

    async with session(service_options=_session_options()):
        async with sandbox.resume_sandbox(name="preview") as box:
            operation = asyncio.create_task(box.query_processes())
            await resume_started.wait()
        assert old_stop.call_count == 1
        release_resume.set()
        assert await operation == []

    assert box.current_session_id == "sbx_new"
    assert box.current_session is not None
    assert box.current_session.status is SandboxStatus.RUNNING
    assert new_stop.call_count == 0


@respx.mock
async def test_async_sparse_stop_ignores_concurrent_recovery(
    mock_env_clear: None,
) -> None:
    resume_started = asyncio.Event()
    release_resume = asyncio.Event()
    stop_started = asyncio.Event()
    replacement_used = asyncio.Event()
    resume_count = 0

    async def sandbox_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal resume_count
        resume_count += 1
        response = _sandbox_response(session_id="sbx_old")
        if resume_count == 1:
            del response["session"]
            return httpx.Response(200, json=response)
        resume_started.set()
        await release_resume.wait()
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    async def stop_handler(_request: httpx.Request) -> httpx.Response:
        stop_started.set()
        await replacement_used.wait()
        return httpx.Response(200, json=_stop_response("sbx_old"))

    def replacement_handler(_request: httpx.Request) -> httpx.Response:
        replacement_used.set()
        return httpx.Response(200, json={"commands": []})

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=sandbox_handler)
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "stopped"}},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        side_effect=replacement_handler
    )
    old_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/stop").mock(
        side_effect=stop_handler
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        assert box.current_session is None
        operation = asyncio.create_task(box.query_processes())
        await resume_started.wait()
        stopping = asyncio.create_task(box.stop())
        await stop_started.wait()
        release_resume.set()
        assert await operation == []
        assert await stopping is box

    assert old_stop.call_count == 1
    assert box.current_session_id == "sbx_new"
    assert box.current_session is not None
    assert box.current_session.status is SandboxStatus.RUNNING


@respx.mock
def test_sync_managed_exit_does_not_wait_for_racing_recovery(
    mock_env_clear: None,
) -> None:
    resume_started = Event()
    release_resume = Event()
    resume_count = 0

    def sandbox_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal resume_count
        resume_count += 1
        if resume_count == 1:
            return httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
        resume_started.set()
        assert release_resume.wait(timeout=5)
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=sandbox_handler)
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        return_value=httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "stopped"}},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        return_value=httpx.Response(200, json={"commands": []})
    )
    old_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_old"))
    )
    new_stop = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/stop").mock(
        return_value=httpx.Response(200, json=_stop_response("sbx_new"))
    )

    with session(service_options=_session_options()):
        with ThreadPoolExecutor(max_workers=1) as executor:
            with sandbox_sync.resume_sandbox(name="preview") as box:
                operation = executor.submit(box.query_processes)
                assert resume_started.wait(timeout=5)
            assert old_stop.call_count == 1
            release_resume.set()
            assert operation.result(timeout=5) == []

    assert box.current_session_id == "sbx_new"
    assert box.current_session is not None
    assert box.current_session.status is SandboxStatus.RUNNING
    assert new_stop.call_count == 0
