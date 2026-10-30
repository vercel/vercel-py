"""Retry and timeout contracts through both public Sandbox APIs."""

import inspect
import json
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, TypeVar

import anyio
import httpx2 as httpx
import pytest

import vendor.respx as respx
from vercel._internal.core.session import SdkSession, SyncSdkSession
from vercel.sandbox import (
    SandboxApiError,
    SandboxClient,
    SandboxCredentials,
    SandboxFilesystemWriteError,
    SandboxResponseError,
    SandboxServiceOptions,
)
from vercel.sandbox.sync import (
    SandboxServiceOptions as SyncSandboxServiceOptions,
    SyncSandboxClient,
)

_ResultT = TypeVar("_ResultT")
_BASE = "https://sandbox.test/v2/sandboxes"
_SNAPSHOT = {
    "snapshot": {
        "id": "snap_1",
        "sourceSessionId": "sbx_1",
        "status": "created",
        "region": "iad1",
        "sizeBytes": 12,
        "createdAt": 1,
        "updatedAt": 2,
    }
}
_SANDBOX = {
    "sandbox": {"name": "preview", "currentSessionId": "sbx_1", "status": "running"},
    "session": {"id": "sbx_1", "sourceSandboxName": "preview", "status": "running"},
}


async def _resolve(value: _ResultT | Awaitable[_ResultT]) -> _ResultT:
    return await value if inspect.isawaitable(value) else value


@asynccontextmanager
async def _client(sync: bool, *, request_timeout: timedelta | None = None) -> AsyncIterator[Any]:
    credentials = SandboxCredentials("token", "team_1", "prj_1")
    if sync:
        client = SyncSandboxClient.create(
            options=SyncSandboxServiceOptions(
                base_url="https://sandbox.test",
                credentials_factory=lambda: credentials,
                request_timeout=request_timeout,
                file_transfer_timeout=timedelta(seconds=12),
            )
        )
        try:
            yield client
        finally:
            client.close()
    else:

        async def factory() -> SandboxCredentials:
            return credentials

        async_client = SandboxClient.create(
            options=SandboxServiceOptions(
                base_url="https://sandbox.test",
                credentials_factory=factory,
                request_timeout=request_timeout,
                file_transfer_timeout=timedelta(seconds=12),
            )
        )
        try:
            yield async_client
        finally:
            await async_client.aclose()


@pytest.fixture
def delays(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    delays: list[float] = []

    async def sleep(self: object, seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr(SdkSession, "sleep", sleep)
    monkeypatch.setattr(SyncSdkSession, "sleep", sleep)
    return delays


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize(
    "status", [400, 401, 404, 408, 409, 422, 429, 500, 502, 503, 504, 599, 600]
)
@respx.mock
async def test_requests_share_status_policy(sync: bool, status: int, delays: list[float]) -> None:
    route = respx.get(f"{_BASE}/snapshots/snap_1").mock(
        side_effect=[
            httpx.Response(status, json={"error": {"message": "temporary"}}),
            httpx.Response(200, json=_SNAPSHOT),
        ]
    )
    retryable = status == 429 or 500 <= status < 600
    async with _client(sync) as client:
        if retryable:
            snapshot = await _resolve(client.get_snapshot(snapshot_id="snap_1"))
            assert snapshot.id == "snap_1"
        else:
            with pytest.raises(SandboxApiError) as failure:
                await _resolve(client.get_snapshot(snapshot_id="snap_1"))
            assert failure.value.status_code == status
    assert route.call_count == (2 if retryable else 1)
    assert len(delays) == (1 if retryable else 0)


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize(
    "retry_after",
    [None, "", "0", "-1", "invalid", "Wed, 21 Oct 2026 07:28:00 GMT", "0.25", "20", "20.1"],
)
@respx.mock
async def test_rate_limits_include_server_wait_and_jitter(
    sync: bool, retry_after: str | None, delays: list[float]
) -> None:
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    route = respx.get(f"{_BASE}/snapshots/snap_1").mock(
        side_effect=[
            httpx.Response(429, headers=headers),
            httpx.Response(429, headers=headers),
            httpx.Response(200, json=_SNAPSHOT),
        ]
    )
    async with _client(sync) as client:
        if retry_after == "20.1":
            with pytest.raises(SandboxApiError) as failure:
                await _resolve(client.get_snapshot(snapshot_id="snap_1"))
            assert failure.value.status_code == 429
            assert route.call_count == 1
            assert delays == []
            return
        assert (await _resolve(client.get_snapshot(snapshot_id="snap_1"))).id == "snap_1"
    server_wait = float(retry_after) if retry_after in ("0.25", "20") else 0
    assert route.call_count == 3
    assert server_wait + 0.4 <= delays[0] <= server_wait + 0.8
    assert server_wait + 0.8 <= delays[1] <= server_wait + 1.6


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize(
    "failure",
    [
        httpx.ConnectError("offline"),
        httpx.ReadTimeout("read stalled"),
        httpx.RemoteProtocolError("reset"),
    ],
)
@respx.mock
async def test_network_failures_exhaust_one_retry_budget(
    sync: bool, failure: Exception, delays: list[float]
) -> None:
    route = respx.get(f"{_BASE}/snapshots/snap_1").mock(side_effect=failure)
    async with _client(sync) as client:
        with pytest.raises(type(failure), match=str(failure)):
            await _resolve(client.get_snapshot(snapshot_id="snap_1"))
    assert route.call_count == 3
    assert 0.4 <= delays[0] <= 0.8
    assert 0.8 <= delays[1] <= 1.6


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("status", [429, 503])
@respx.mock
async def test_final_api_error_keeps_last_response(
    sync: bool, status: int, delays: list[float]
) -> None:
    last = httpx.Response(
        status,
        headers={"Retry-After": "20"},
        json={"error": {"code": "unavailable", "message": "try later"}},
    )
    route = respx.get(f"{_BASE}/snapshots/snap_1").mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(502),
            last,
        ]
    )
    async with _client(sync) as client:
        with pytest.raises(SandboxApiError, match="try later") as failure:
            await _resolve(client.get_snapshot(snapshot_id="snap_1"))
    assert route.call_count == 3
    assert len(delays) == 2
    assert failure.value.status_code == status
    assert failure.value.code == "unavailable"
    assert failure.value.response.json() == last.json()


@pytest.mark.parametrize("sync", [False, True])
@respx.mock
async def test_creation_retries_the_same_body_and_credentials(
    sync: bool, delays: list[float]
) -> None:
    route = respx.post("https://sandbox.test/v3/sandboxes").mock(
        side_effect=[
            httpx.ConnectError("reset"),
            httpx.Response(503),
            httpx.Response(200, json=_SANDBOX),
        ]
    )
    async with _client(sync) as client:
        box = await _resolve(client.create_sandbox(name="preview", destroy=False))
        assert box.name == "preview"
    assert route.call_count == 3
    assert len({call.request.content for call in route.calls}) == 1
    assert json.loads(route.calls.last.request.content)["name"] == "preview"
    assert all(call.request.headers["authorization"] == "Bearer token" for call in route.calls)
    assert all(call.request.url.params["teamId"] == "team_1" for call in route.calls)


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("request_timeout", [None, timedelta(seconds=9)])
@respx.mock
async def test_timeout_policy_preserves_transfer_and_wait_overrides(
    sync: bool, request_timeout: timedelta | None
) -> None:
    lookup = respx.get(f"{_BASE}/preview").respond(200, json=_SANDBOX)
    read = respx.post(f"{_BASE}/sessions/sbx_1/fs/read").respond(200, content=b"hello")
    command = {
        "command": {
            "id": "cmd_1",
            "sessionId": "sbx_1",
            "name": "echo",
            "args": [],
            "cwd": "/vercel/sandbox",
            "startedAt": 1,
            "exitCode": 0,
        }
    }
    create = respx.post(f"{_BASE}/sessions/sbx_1/cmd").respond(
        200, json={"command": {**command["command"], "exitCode": None}}
    )
    wait = respx.get(f"{_BASE}/sessions/sbx_1/cmd/cmd_1").respond(200, json=command)
    async with _client(sync, request_timeout=request_timeout) as client:
        box = await _resolve(client.get_sandbox(name="preview"))
        assert await _resolve(box.fs.read_bytes("hello.txt")) == b"hello"
        command_handle = await _resolve(box.create_command("echo"))
        assert await _resolve(command_handle.wait()) == 0
    timeout = lookup.calls.last.request.extensions["timeout"]
    assert timeout["read"] == (None if request_timeout is None else 9.0)
    assert timeout["connect"] == (60.0 if request_timeout is None else 9.0)
    assert create.calls.last.request.extensions["timeout"] == timeout
    assert all(value == 12.0 for value in read.calls.last.request.extensions["timeout"].values())
    assert all(value is None for value in wait.calls.last.request.extensions["timeout"].values())


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("operation", ["run", "upload"])
@respx.mock
async def test_streaming_side_effects_are_not_replayed(
    sync: bool, operation: str, delays: list[float]
) -> None:
    respx.get(f"{_BASE}/preview").respond(200, json=_SANDBOX)
    endpoint = "cmd" if operation == "run" else "fs/write"
    route = respx.post(f"{_BASE}/sessions/sbx_1/{endpoint}").respond(503)
    async with _client(sync) as client:
        box = await _resolve(client.get_sandbox(name="preview"))
        with pytest.raises(SandboxApiError if operation == "run" else SandboxFilesystemWriteError):
            if operation == "run":
                await _resolve(box.run_command("echo"))
            else:
                await _resolve(box.fs.write_bytes("hello.txt", b"hello"))
    assert route.call_count == 1
    assert delays == []


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["request", "backoff", "retry-after"])
@respx.mock
async def test_cancellation_never_retries(phase: str, monkeypatch: pytest.MonkeyPatch) -> None:
    scope = anyio.CancelScope()
    waits: list[float] = []
    attempts: list[httpx.Request] = []

    async def sleep(self: object, seconds: float) -> None:
        waits.append(seconds)
        scope.cancel()
        await anyio.lowlevel.checkpoint()

    async def respond(request: httpx.Request) -> httpx.Response:
        attempts.append(request)
        if phase == "request":
            scope.cancel()
            await anyio.lowlevel.checkpoint()
        return (
            httpx.Response(429, headers={"Retry-After": "20"})
            if phase == "retry-after"
            else httpx.Response(503)
        )

    monkeypatch.setattr(SdkSession, "sleep", sleep)
    respx.get(f"{_BASE}/snapshots/snap_1").mock(side_effect=respond)
    async with _client(False) as client:
        with scope:
            await client.get_snapshot(snapshot_id="snap_1")
    assert scope.cancelled_caught
    assert len(attempts) == 1
    assert len(waits) == (0 if phase == "request" else 1)


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("content", [b"not-json", b'{"snapshot": {}}'])
@respx.mock
async def test_invalid_success_responses_are_not_retried(
    sync: bool, content: bytes, delays: list[float]
) -> None:
    route = respx.get(f"{_BASE}/snapshots/snap_1").respond(200, content=content)
    async with _client(sync) as client:
        with pytest.raises(SandboxResponseError):
            await _resolve(client.get_snapshot(snapshot_id="snap_1"))
    assert route.call_count == 1
    assert delays == []


class _BrokenBody(httpx.SyncByteStream, httpx.AsyncByteStream):
    def __init__(self, *, broken: bool = True) -> None:
        self.broken = broken
        self.closed = False

    def close(self) -> None:
        self.closed = True

    async def aclose(self) -> None:
        self.close()

    def __iter__(self):  # type: ignore[no-untyped-def]
        yield b'{"command":'
        if self.broken:
            raise httpx.ReadError("response interrupted")

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        for chunk in self:
            yield chunk


@pytest.mark.parametrize("sync", [False, True])
@respx.mock
async def test_command_wait_retries_entire_response_once_per_attempt(
    sync: bool, delays: list[float]
) -> None:
    respx.get(f"{_BASE}/preview").respond(200, json=_SANDBOX)
    command = {
        "id": "cmd_1",
        "sessionId": "sbx_1",
        "name": "echo",
        "args": [],
        "cwd": "/vercel/sandbox",
        "startedAt": 1,
        "exitCode": None,
    }
    respx.post(f"{_BASE}/sessions/sbx_1/cmd").respond(200, json={"command": command})
    wait = respx.get(f"{_BASE}/sessions/sbx_1/cmd/cmd_1").mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(200, stream=_BrokenBody()),
            httpx.Response(200, json={"command": {**command, "exitCode": 7}}),
        ]
    )
    async with _client(sync) as client:
        box = await _resolve(client.get_sandbox(name="preview"))
        command_handle = await _resolve(box.create_command("echo"))
        assert await _resolve(command_handle.wait()) == 7
    assert wait.call_count == 3
    assert len(delays) == 2


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("retry_after", ["20", "21"])
@respx.mock
async def test_log_opening_has_one_rate_limit_budget(
    sync: bool, retry_after: str, delays: list[float]
) -> None:
    respx.get(f"{_BASE}/preview").respond(200, json=_SANDBOX)
    command = {
        "id": "cmd_1",
        "sessionId": "sbx_1",
        "name": "echo",
        "args": [],
        "cwd": "/vercel/sandbox",
        "startedAt": 1,
        "exitCode": None,
    }
    respx.post(f"{_BASE}/sessions/sbx_1/cmd").respond(200, json={"command": command})
    logs = respx.get(f"{_BASE}/sessions/sbx_1/cmd/cmd_1/logs").respond(
        429, headers={"Retry-After": retry_after}
    )
    async with _client(sync) as client:
        box = await _resolve(client.get_sandbox(name="preview"))
        command_handle = await _resolve(box.create_command("echo"))
        assert command_handle.stdout is not None
        with pytest.raises(SandboxApiError):
            await _resolve(command_handle.stdout.read())
    assert logs.call_count == (3 if retry_after == "20" else 1)
    assert len(delays) == (2 if retry_after == "20" else 0)


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("phase", ["open", "body"])
@respx.mock
async def test_file_reads_retry_opening_and_close_failed_responses(
    sync: bool, phase: str, delays: list[float]
) -> None:
    respx.get(f"{_BASE}/preview").respond(200, json=_SANDBOX)
    body = _BrokenBody(broken=phase == "body")
    read = respx.post(f"{_BASE}/sessions/sbx_1/fs/read").mock(
        side_effect=[
            httpx.Response(503 if phase == "open" else 200, stream=body),
            httpx.Response(200, content=b"hello"),
        ]
    )
    async with _client(sync) as client:
        box = await _resolve(client.get_sandbox(name="preview"))
        if phase == "open":
            assert await _resolve(box.fs.read_bytes("hello.txt")) == b"hello"
        else:
            with pytest.raises(httpx.ReadError, match="response interrupted"):
                await _resolve(box.fs.read_bytes("hello.txt"))
    assert body.closed
    assert read.call_count == (2 if phase == "open" else 1)
    assert len(delays) == (1 if phase == "open" else 0)
