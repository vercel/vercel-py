import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import anyio
import httpx2 as httpx
import pytest
from sandbox_fixtures import sandbox_service_options

import vendor.respx as respx
from vercel import sandbox
from vercel._internal.core.options import ServiceOptions
from vercel.api import session
from vercel.sandbox import sync as sandbox_sync
from vercel.sandbox._internal import async_runtime, sync_runtime
from vercel.sandbox._internal.interactive_session import (
    Frame,
    decode_frame,
    health_url,
    resize_message,
    start_message,
)
from vercel.sandbox._internal.state import InteractiveSessionState


def _session_options() -> list[ServiceOptions]:
    return sandbox_service_options()


def _sandbox_response(*, session_id: str = "sbx_1", status: str = "running") -> dict[str, object]:
    return {
        "sandbox": {"name": "preview", "currentSessionId": session_id, "status": status},
        "session": {
            "id": session_id,
            "sourceSandboxName": "preview",
            "projectId": "prj_1",
            "status": status,
            "cwd": "/vercel/sandbox",
        },
    }


def _interactive_response() -> dict[str, object]:
    return {"url": "wss://controller.test/ws", "token": "tok_1"}


class _TextEvent:
    def __init__(self, data: str) -> None:
        self.data = data


class _BytesEvent:
    def __init__(self, data: bytes) -> None:
        self.data = data


class _FakeTransport:
    """Stand-in for the WebSocket transport, scripted with received frames."""

    instances: list["_FakeTransport"] = []

    def __init__(self, state: InteractiveSessionState) -> None:
        self.state = state
        self.sent_bytes: list[bytes] = []
        self.sent_json: list[dict[str, object]] = []
        self.closed = False
        self.frames: list[Frame] = [Frame(data=b"hello"), Frame(returncode=0, end=True)]
        _FakeTransport.instances.append(self)

    async def connect(self) -> None:
        return None

    async def send_bytes(self, data: bytes) -> None:
        self.sent_bytes.append(data)

    async def send_json(self, message: dict[str, object]) -> None:
        self.sent_json.append(message)

    async def receive(self) -> Frame:
        if not self.frames:
            return Frame(end=True)
        return self.frames.pop(0)

    async def close(self) -> None:
        self.closed = True


@asynccontextmanager
async def _fake_open_async_transport(
    state: InteractiveSessionState,
) -> AsyncIterator[_FakeTransport]:
    transport = _FakeTransport(state)
    try:
        yield transport
    finally:
        await transport.close()


@pytest.fixture(autouse=True)
def fake_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeTransport.instances.clear()
    monkeypatch.setattr(async_runtime, "open_async_transport", _fake_open_async_transport)
    monkeypatch.setattr(sync_runtime, "SyncInteractiveTransport", _FakeTransport)


def test_public_interactive_exports() -> None:
    assert sandbox.InteractiveSession is async_runtime.InteractiveSession
    assert sandbox_sync.SyncInteractiveSession is sync_runtime.SyncInteractiveSession
    assert issubclass(sandbox.SandboxInteractiveError, sandbox.SandboxError)


def test_start_message_builds_protocol_payload() -> None:
    message = start_message(
        command="python",
        args=["-i"],
        cwd="/vercel/sandbox",
        env={"FOO": "bar"},
        sudo=False,
        cols=100,
        rows=30,
    )
    assert message == {
        "type": "start",
        "command": "python",
        "args": ["-i"],
        "env": ["TERM=xterm-256color", "FOO=bar"],
        "cwd": "/vercel/sandbox",
        "cols": 100,
        "rows": 30,
    }


def test_start_message_prepends_sudo_and_allows_term_override() -> None:
    message = start_message(
        command="bash",
        args=None,
        cwd=None,
        env={"TERM": "dumb"},
        sudo=True,
        cols=80,
        rows=24,
    )
    assert message["command"] == "sudo"
    assert message["args"] == ["bash"]
    assert message["env"] == ["TERM=dumb"]


def test_resize_message_and_health_url() -> None:
    assert resize_message(120, 40) == {"type": "resize", "cols": 120, "rows": 40}
    assert health_url("wss://controller.test/ws?x=1") == "https://controller.test/health"


def test_decode_frame_maps_events() -> None:
    assert decode_frame(_BytesEvent(b"out")) == Frame(data=b"out")
    assert decode_frame(_TextEvent(json.dumps({"type": "exit", "code": 3}))) == Frame(
        returncode=3, end=True
    )
    # A successful exit omits "code"; it must not surface as terminal output.
    assert decode_frame(_TextEvent(json.dumps({"type": "exit"}))) == Frame(returncode=0, end=True)
    assert decode_frame(_TextEvent("plain")) == Frame(data=b"plain")
    assert decode_frame(_TextEvent(json.dumps({"type": "other"}))) == Frame(
        data=b'{"type": "other"}'
    )
    assert decode_frame(object()) == Frame(end=True)


@respx.mock
async def test_async_open_interactive_streams_and_waits(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        async with box.open_interactive("bash", ["-l"], cols=100, rows=30) as pty:
            transport = _FakeTransport.instances[-1]
            assert transport.state.url == "wss://controller.test/ws"
            assert transport.state.token == "tok_1"
            assert transport.sent_json[0] == start_message(
                command="bash",
                args=["-l"],
                cwd="/vercel/sandbox",
                env=None,
                sudo=False,
                cols=100,
                rows=30,
            )
            await pty.send(b"echo hi\n")
            assert transport.sent_bytes == [b"echo hi\n"]
            await pty.resize(120, 40)
            assert transport.sent_json[-1] == resize_message(120, 40)
            assert await pty.receive() == b"hello"
            assert await pty.wait() == 0
            assert pty.returncode == 0

    assert route.called
    assert _FakeTransport.instances[-1].closed


@respx.mock
async def test_async_interactive_iteration_stops_at_exit(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        async with box.open_interactive() as pty:
            _FakeTransport.instances[-1].frames = [
                Frame(data=b"a"),
                Frame(data=b"b"),
                Frame(returncode=2, end=True),
            ]
            chunks = [chunk async for chunk in pty]
            assert chunks == [b"a", b"b"]
            assert pty.returncode == 2
            with pytest.raises(anyio.EndOfStream):
                await pty.receive()


@respx.mock
async def test_async_interactive_exit_without_code_reports_success(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        async with box.open_interactive() as pty:
            _FakeTransport.instances[-1].frames = [
                Frame(data=b"bye"),
                decode_frame(_TextEvent(json.dumps({"type": "exit"}))),
            ]
            assert [chunk async for chunk in pty] == [b"bye"]
            assert await pty.wait() == 0


@respx.mock
async def test_async_interactive_reads_after_stream_end_do_not_block(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        async with box.open_interactive() as pty:
            _FakeTransport.instances[-1].frames = [Frame(returncode=4, end=True)]
            assert await pty.wait() == 4
            for _ in range(3):
                with pytest.raises(anyio.EndOfStream):
                    await pty.receive()
            assert await pty.wait() == 4


@respx.mock
async def test_async_interactive_wait_without_exit_frame(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        async with box.open_interactive() as pty:
            _FakeTransport.instances[-1].frames = [Frame(data=b"a")]
            assert await pty.wait() is None


@respx.mock
async def test_async_open_interactive_resumes_stopped_sandbox(mock_env_clear: None) -> None:
    respx.get("https://sandbox.test/v2/sandboxes/preview", params={"resume": "false"}).mock(
        return_value=httpx.Response(200, json=_sandbox_response(status="stopped"))
    )
    resume = respx.get("https://sandbox.test/v2/sandboxes/preview", params={"resume": "true"}).mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_2"))
    )
    stopped = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(410, json={"error": {"code": "session_stopped"}})
    )
    resumed = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_2/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.get_sandbox(name="preview")
        async with box.open_interactive():
            pass

    assert stopped.called
    assert resume.called
    assert resumed.called


@respx.mock
def test_sync_open_interactive_streams_and_waits(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with box.open_interactive("bash") as pty:
            transport = _FakeTransport.instances[-1]
            assert transport.sent_json[0]["command"] == "bash"
            pty.send(b"echo hi\n")
            assert transport.sent_bytes == [b"echo hi\n"]
            pty.resize(120, 40)
            assert transport.sent_json[-1] == resize_message(120, 40)
            assert pty.receive() == b"hello"
            assert pty.wait() == 0

    assert _FakeTransport.instances[-1].closed


@respx.mock
def test_sync_interactive_iteration_stops_at_exit(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with box.open_interactive() as pty:
            _FakeTransport.instances[-1].frames = [Frame(data=b"a"), Frame(returncode=5, end=True)]
            assert list(pty) == [b"a"]
            assert pty.returncode == 5
