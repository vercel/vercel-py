import io
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
    open_async_transport,
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
        self.receive_timeouts: list[float | None] = []
        self.running = False
        self.frames: list[Frame] = [Frame(data=b"hello"), Frame(returncode=0, end=True)]
        _FakeTransport.instances.append(self)

    async def connect(self) -> None:
        return None

    async def send_bytes(self, data: bytes) -> None:
        self.sent_bytes.append(data)

    async def send_json(self, message: dict[str, object]) -> None:
        self.sent_json.append(message)

    async def receive(self, timeout: float | None = None) -> Frame:
        self.receive_timeouts.append(timeout)
        if self.frames:
            return self.frames.pop(0)
        if self.running:
            # A live terminal with nothing to say yet.
            raise TimeoutError
        return Frame(end=True)

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


def test_empty_frames_are_not_end_of_stream() -> None:
    # A zero-length frame would otherwise read as EOF and cut the session
    # short.
    assert decode_frame(_BytesEvent(b"")) is None
    assert decode_frame(_TextEvent("")) is None


async def test_connect_failure_is_catchable_by_type() -> None:
    # The connection task group would otherwise wrap this in an
    # ExceptionGroup, making the public error type impossible to catch.
    state = InteractiveSessionState(url="wss://127.0.0.1:1/ws", token="t")
    with pytest.raises(sandbox.SandboxInteractiveError):
        async with open_async_transport(state):
            pass


async def test_caller_errors_are_not_wrapped_by_the_connection() -> None:
    class Boom(Exception):
        pass

    async with session(service_options=_session_options()):
        with pytest.raises(Boom):
            async with _fake_open_async_transport(
                InteractiveSessionState(url="wss://h.test/ws", token="t")
            ):
                raise Boom


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
    # Unknown control frames are protocol chatter and must never be shown as
    # terminal output.
    assert decode_frame(_TextEvent(json.dumps({"type": "other"}))) is None
    # Text that merely looks like JSON is still terminal output.
    assert decode_frame(_TextEvent(json.dumps({"no": "type"}))) == Frame(data=b'{"no": "type"}')
    assert decode_frame(object()) == Frame(end=True)


@respx.mock
async def test_async_stream_follows_the_bytestream_contract(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        async with box.open_interactive() as pty:
            assert isinstance(pty.stream, anyio.abc.ByteStream)
            _FakeTransport.instances[-1].frames = [Frame(data=b"abcdef")]
            # receive() must respect max_bytes and keep the remainder.
            assert await pty.stream.receive(2) == b"ab"
            assert await pty.stream.receive(10) == b"cdef"
            with pytest.raises(ValueError):
                await pty.stream.receive(0)
            with pytest.raises(NotImplementedError):
                await pty.stream.send_eof()

            await pty.stream.aclose()
            with pytest.raises(anyio.ClosedResourceError):
                await pty.stream.receive()
            with pytest.raises(anyio.ClosedResourceError):
                await pty.stream.send(b"x")


@respx.mock
def test_sync_stream_follows_the_rawio_contract(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with box.open_interactive() as pty:
            transport = _FakeTransport.instances[-1]
            assert isinstance(pty.stream, io.RawIOBase)
            assert pty.stream.readable() and pty.stream.writable()

            transport.frames = [Frame(data=b"abcdef")]
            # A zero-length read must not consume input or block.
            assert pty.stream.readinto(bytearray(0)) == 0
            assert not transport.receive_timeouts

            target = bytearray(2)
            assert pty.stream.readinto(target) == 2
            assert bytes(target) == b"ab"
            assert pty.stream.read(10) == b"cdef"
            assert pty.stream.write(b"hi") == 2

        with pytest.raises(ValueError):
            pty.stream.write(b"x")
        with pytest.raises(ValueError):
            pty.stream.readinto(bytearray(4))


@respx.mock
async def test_async_controls_after_close_report_closed(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        async with box.open_interactive() as pty:
            await pty.aclose()
            with pytest.raises(anyio.ClosedResourceError):
                await pty.resize(80, 24)


@respx.mock
def test_sync_controls_after_close_report_closed(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with box.open_interactive() as pty:
            pty.close()
            with pytest.raises(ValueError):
                pty.resize(80, 24)


@respx.mock
async def test_async_control_frames_never_reach_the_terminal(mock_env_clear: None) -> None:
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
                Frame(data=b"real output"),
                Frame(returncode=0, end=True),
            ]
            assert [chunk async for chunk in pty.stream] == [b"real output"]


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
            await pty.stream.send(b"echo hi\n")
            assert transport.sent_bytes == [b"echo hi\n"]
            await pty.resize(120, 40)
            assert transport.sent_json[-1] == resize_message(120, 40)
            assert await pty.stream.receive() == b"hello"
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
            chunks = [chunk async for chunk in pty.stream]
            assert chunks == [b"a", b"b"]
            assert pty.returncode == 2
            with pytest.raises(anyio.EndOfStream):
                await pty.stream.receive()


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
            exit_frame = decode_frame(_TextEvent(json.dumps({"type": "exit"})))
            assert exit_frame is not None
            _FakeTransport.instances[-1].frames = [Frame(data=b"bye"), exit_frame]
            assert [chunk async for chunk in pty.stream] == [b"bye"]
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
                    await pty.stream.receive()
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
            pty.stream.write(b"echo hi\n")
            assert transport.sent_bytes == [b"echo hi\n"]
            pty.resize(120, 40)
            assert transport.sent_json[-1] == resize_message(120, 40)
            assert pty.stream.read(5) == b"hello"
            assert pty.wait() == 0

    assert _FakeTransport.instances[-1].closed


@respx.mock
def test_sync_interactive_wait_honours_timeout(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with box.open_interactive() as pty:
            transport = _FakeTransport.instances[-1]
            transport.frames = [Frame(data=b"still running")]
            transport.running = True
            with pytest.raises(TimeoutError):
                pty.wait(timeout=5)
            # The deadline is passed down so the read cannot outlive it.
            assert transport.receive_timeouts and all(
                value is not None and value <= 5 for value in transport.receive_timeouts
            )


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
            assert pty.stream.read() == b"a"
            assert pty.returncode == 5
