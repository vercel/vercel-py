import asyncio
import gc
import json
from collections.abc import AsyncIterator
from datetime import timedelta
from itertools import islice
from typing import Any

import httpx
import pytest
import respx
from pydantic import BaseModel, ValidationError

from vercel import unstable as vercel
from vercel._internal.unstable.errors import VercelSessionClosedError
from vercel._internal.unstable.sandbox.options import SandboxCredentials
from vercel._internal.unstable.sandbox.service import get_sandbox_service
from vercel._internal.unstable.sandbox.state import SandboxRuntimeSessionState, SandboxState
from vercel._internal.unstable.session import get_active_session
from vercel.unstable import sandbox
from vercel.unstable.sandbox import (
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
    SandboxServiceOptions,
    SandboxSource,
    SandboxStatus,
    SandboxTerminalStateError,
    SnapshotExpiration,
    SnapshotRetention,
    SnapshotRetentionState,
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
    project_id: str = "prj_123",
) -> dict[str, Any]:
    return {
        "sandbox": {
            "name": name,
            "currentSessionId": session_id,
            "status": status,
            "persistent": True,
            "runtime": "python3.13",
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
        assert request.url.path == "/v2/sandboxes"
        assert dict(request.url.params) == {"teamId": "team_123"}
        assert request.headers["authorization"] == "Bearer token"
        assert request.headers["user-agent"].startswith("vercel/unstable/sandbox/")
        assert "Python/" in request.headers["user-agent"]
        assert json.loads(request.content) == {
            "projectId": "prj_other",
            "name": "preview",
            "runtime": "python3.13",
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
        }
        response = _sandbox_response(project_id="prj_other")
        payload = response["sandbox"]
        assert isinstance(payload, dict)
        payload["tags"] = {"env": "test"}
        return httpx.Response(200, json=response)

    route = respx.post("https://sandbox.test/v2/sandboxes").mock(side_effect=handler)
    update_responses = iter(
        [
            {
                "sandbox": {
                    "name": "preview",
                    "currentSessionId": "sbx_123",
                    "tags": {"env": "updated"},
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

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(
            project_id="prj_other",
            name="preview",
            runtime="python3.13",
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
        )

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
        )
        assert handle.tags == {"env": "updated"}
        assert handle.routes[0].url == "https://preview.sandbox.test"
        assert handle.project_id == "prj_other"
        assert handle.current_session is retained_session

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
async def test_network_policy_async_public_flow(mock_env_clear: None) -> None:
    create_route = respx.post("https://sandbox.test/v2/sandboxes").mock(
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

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(
            name="preview",
            runtime="python3.13",
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

        session = await handle.update_network_policy(NetworkPolicy.deny_all())
        assert session.network_policy == NetworkPolicy.deny_all()
        assert handle.current_session is session

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
    route = respx.post("https://sandbox.test/v2/sandboxes").mock(
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

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(
            name="preview",
            runtime="python3.13",
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
    create_route = respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    async with vercel.session(service_options=_session_options()):
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
async def test_public_snapshot_expiration_validation_happens_before_requests(
    mock_env_clear: None,
) -> None:
    create_route = respx.post("https://sandbox.test/v2/sandboxes").mock(
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

    async with vercel.session(service_options=_session_options()):
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

    async with vercel.session(service_options=_session_options()):
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
        session = await handle.extend_execution_time_limit(2.5)
        assert isinstance(session, sandbox.SandboxRuntimeSession)
        assert session.execution_time_limit == timedelta(minutes=5)
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

    with vercel.session(service_options=_session_options()):
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

    async with vercel.session(service_options=_session_options()):
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

    with vercel.session(service_options=_session_options()):
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
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(
            200,
            json=_sandbox_response(status="pending", session_status="pending"),
        )
    )

    async with vercel.session(service_options=_session_options()):
        session = get_active_session()
        operation = asyncio.create_task(
            get_sandbox_service(session).create_sandbox(name="preview", runtime="python3.13")
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
async def test_closed_session_rejects_handles_and_lazy_readers(mock_env_clear: None) -> None:
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
        service = get_sandbox_service(get_active_session())
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = await handle.session()
        command = await handle.create_process("sleep", ["30"])
        snapshot = await handle.snapshot()

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
        assert (await handle.create_process("python", ["--version"])).id == "cmd_123"

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
            json={
                "session": _sandbox_response(
                    session_id="sbx_runtime", status="stopped", session_status="stopped"
                )["session"]
            },
        )
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(session_id="sbx_runtime"))
    )

    async with vercel.session(service_options=_session_options()):
        handle = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = await handle.session()
        assert await runtime_session.stop() is runtime_session
        assert (await runtime_session.create_process("python", ["--version"])).id == "cmd_123"

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
            json={
                "session": _sandbox_response(
                    session_id="sbx_runtime", status="stopped", session_status="stopped"
                )["session"]
            },
        )
    )
    command_route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_runtime/cmd").mock(
        return_value=httpx.Response(200, json=_command_response(session_id="sbx_runtime"))
    )

    with vercel.session(service_options=_session_options()):
        handle = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        runtime_session = handle.session()
        assert runtime_session.stop() is runtime_session
        assert runtime_session.create_process("python", ["--version"]).id == "cmd_123"

    assert command_route.called


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

        command = await handle.create_process("python", ["--version"])
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
