import io
import json
import queue
import threading
import time
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import anyio
import httpx2 as httpx
import pytest
from sandbox_fixtures import sandbox_service_options

import vendor.respx as respx
from vercel import sandbox
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.core.options import ServiceOptions
from vercel.api import session
from vercel.sandbox import sync as sandbox_sync
from vercel.sandbox._internal import async_runtime, sync_runtime
from vercel.sandbox._internal.interactive_session import (
    AsyncInteractiveTransport,
    Frame,
    SyncInteractiveTransport,
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
        self.failure: BaseException | None = None
        self.frames: list[Frame] = [Frame(data=b"hello"), Frame(returncode=0, end=True)]
        self._returncode: int | None = None
        self._finished = False
        _FakeTransport.instances.append(self)

    @property
    def returncode(self) -> int | None:
        return self._returncode

    @property
    def finished(self) -> bool:
        return self._finished

    async def connect(self) -> None:
        return None

    async def send_bytes(self, data: bytes) -> None:
        self.sent_bytes.append(data)

    async def send_json(self, message: dict[str, object]) -> None:
        self.sent_json.append(message)

    async def receive(self, timeout: float | None = None) -> Frame:
        self.receive_timeouts.append(timeout)
        if self._finished:
            return Frame(end=True)
        if self.frames:
            frame = self.frames.pop(0)
            if frame.end:
                self._returncode = frame.returncode
                self._finished = True
            return frame
        if self.running:
            # A live terminal with nothing to say yet.
            raise TimeoutError
        self._finished = True
        return Frame(end=True)

    async def wait(self, timeout: float | None = None) -> int | None:
        if not self._finished:
            # Mirror the real transports: the exit frame is tracked as it
            # passes the connection and never reaches a reader.
            for index, frame in enumerate(self.frames):
                if frame.end:
                    self._returncode = frame.returncode
                    del self.frames[index]
                    self._finished = True
                    break
            else:
                if self.running:
                    raise TimeoutError
                self._finished = True
        if self.failure is not None:
            raise ConnectionError from self.failure
        return self._returncode

    async def close(self) -> None:
        self.closed = True
        self._finished = True


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
    assert sandbox.InteractiveStream is async_runtime.InteractiveStream
    assert sandbox.InteractiveSessionOperation is async_runtime.InteractiveSessionOperation
    assert sandbox_sync.SyncInteractiveSession is sync_runtime.SyncInteractiveSession
    assert sandbox_sync.SyncInteractiveStream is sync_runtime.SyncInteractiveStream
    assert (
        sandbox_sync.SyncInteractiveSessionOperation is sync_runtime.SyncInteractiveSessionOperation
    )
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


def test_exit_code_is_only_trusted_when_it_is_an_integer() -> None:
    def exit_frame(payload: dict[str, object]) -> Frame | None:
        return decode_frame(_TextEvent(json.dumps(payload)))

    assert exit_frame({"type": "exit"}) == Frame(returncode=0, end=True)
    assert exit_frame({"type": "exit", "code": 3}) == Frame(returncode=3, end=True)
    assert exit_frame({"type": "exit", "code": -9}) == Frame(returncode=-9, end=True)
    # An unexpected shape must never be reported as a successful exit.
    assert exit_frame({"type": "exit", "code": "3"}) == Frame(returncode=None, end=True)
    assert exit_frame({"type": "exit", "code": True}) == Frame(returncode=None, end=True)


async def test_waiting_does_not_consume_terminal_output() -> None:
    # Waiting is lifecycle, not I/O: it must not take frames from a reader or
    # block one that is already running.
    transport = _FakeTransport(InteractiveSessionState(url="wss://h.test/ws", token="t"))
    transport.frames = [Frame(data=b"one"), Frame(data=b"two"), Frame(returncode=5, end=True)]
    pty = async_runtime.InteractiveSession(transport=cast(AsyncInteractiveTransport, transport))

    seen: list[bytes] = []
    codes: list[int | None] = []
    async with anyio.create_task_group() as task_group:

        async def reader() -> None:
            async for chunk in pty.stream:
                seen.append(chunk)

        async def waiter() -> None:
            codes.append(await pty.wait())

        task_group.start_soon(reader)
        task_group.start_soon(waiter)

    assert seen == [b"one", b"two"]
    assert codes == [5]


async def test_reader_reports_a_broken_connection_instead_of_a_clean_end() -> None:
    # A dropped connection must not look like the process exiting normally.
    transport = _FakeTransport(InteractiveSessionState(url="wss://h.test/ws", token="t"))
    transport.frames = [Frame(data=b"partial")]
    transport.failure = OSError("connection reset")
    pty = async_runtime.InteractiveSession(transport=cast(AsyncInteractiveTransport, transport))

    assert await pty.stream.receive() == b"partial"
    with pytest.raises(anyio.BrokenResourceError):
        await pty.stream.receive()


async def test_waiting_reports_a_broken_connection() -> None:
    # A failure has to reach the wait as well as the reader, or a caller that
    # only waits sees a clean exit that never happened.
    transport = _FakeTransport(InteractiveSessionState(url="wss://h.test/ws", token="t"))
    transport.frames = []
    transport.failure = OSError("connection reset")
    pty = async_runtime.InteractiveSession(transport=cast(AsyncInteractiveTransport, transport))

    with pytest.raises((anyio.BrokenResourceError, ConnectionError)):
        await pty.wait()


def test_sync_waiting_reports_a_broken_connection() -> None:
    transport = _FakeTransport(InteractiveSessionState(url="wss://h.test/ws", token="t"))
    transport.frames = []
    transport.failure = OSError("connection reset")
    pty = sync_runtime.SyncInteractiveSession(transport=cast(SyncInteractiveTransport, transport))

    with pytest.raises(ConnectionError):
        pty.wait()


def test_sync_reader_reports_a_broken_connection() -> None:
    transport = _FakeTransport(InteractiveSessionState(url="wss://h.test/ws", token="t"))
    transport.frames = [Frame(data=b"partial")]
    transport.failure = OSError("connection reset")
    pty = sync_runtime.SyncInteractiveSession(transport=cast(SyncInteractiveTransport, transport))

    assert pty.stream.read(7) == b"partial"
    with pytest.raises(ConnectionError):
        pty.stream.read(1)


class _ScriptedSocket:
    """Stands in for httpx2's sync websocket: scripted events behind receive()."""

    def __init__(self, events: list[object]) -> None:
        self.events = deque(events)
        self.closed = False

    def receive(self, timeout: float | None = None) -> object:
        if self.events:
            event = self.events.popleft()
            if isinstance(event, BaseException):
                raise event
            return event
        time.sleep(min(timeout or 0.01, 0.01))
        raise TimeoutError

    def close(self) -> None:
        self.closed = True


def _sync_transport(socket: object) -> SyncInteractiveTransport:
    transport = SyncInteractiveTransport(InteractiveSessionState(url="wss://h.test/ws", token="t"))
    transport._session = cast(Any, socket)
    return transport


def test_sync_transport_waiting_buffers_output_for_readers() -> None:
    # Waiting drives the connection but must not lose what it dispatches.
    transport = _sync_transport(
        _ScriptedSocket(
            [
                _BytesEvent(b"one"),
                _BytesEvent(b"two"),
                _TextEvent(json.dumps({"type": "exit", "code": 5})),
            ]
        )
    )

    assert iter_coroutine(transport.wait()) == 5
    assert iter_coroutine(transport.receive()).data == b"one"
    assert iter_coroutine(transport.receive()).data == b"two"
    assert iter_coroutine(transport.receive()).end


def test_sync_transport_backpressure_blocks_rather_than_dropping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vercel.sandbox._internal import interactive_session

    monkeypatch.setattr(interactive_session, "_BUFFER_BYTES", 8)
    transport = _sync_transport(
        _ScriptedSocket(
            [
                _BytesEvent(b"abcd"),
                _BytesEvent(b"efgh"),
                _BytesEvent(b"ijkl"),
                _TextEvent(json.dumps({"type": "exit"})),
            ]
        )
    )

    # The buffer fills before the exit frame is reached, so waiting must stop
    # pulling and time out instead of discarding output.
    with pytest.raises(TimeoutError):
        iter_coroutine(transport.wait(timeout=0.3))

    assert iter_coroutine(transport.receive()).data == b"abcd"
    assert iter_coroutine(transport.receive()).data == b"efgh"
    assert iter_coroutine(transport.receive()).data == b"ijkl"
    assert iter_coroutine(transport.wait()) == 0


def test_sync_transport_reports_disconnect_after_buffered_output() -> None:
    from httpx2.websockets import WebSocketNetworkError

    transport = _sync_transport(_ScriptedSocket([_BytesEvent(b"tail"), WebSocketNetworkError()]))

    assert iter_coroutine(transport.receive()).data == b"tail"
    assert iter_coroutine(transport.receive()).end
    with pytest.raises(ConnectionError):
        iter_coroutine(transport.wait())


def test_sync_transport_close_releases_a_parked_websocket_reader() -> None:
    # httpx2's reader blocks on a bounded queue with no way to be woken, and
    # its shutdown joins that thread. Closing must drain enough for it to
    # finish, or exiting the session hangs.
    events: queue.Queue[object] = queue.Queue(maxsize=2)
    events.put(_BytesEvent(b"a"))
    events.put(_BytesEvent(b"b"))

    class QueueSocket:
        def receive(self, timeout: float | None = None) -> object:
            try:
                return events.get(timeout=timeout)
            except queue.Empty:
                raise TimeoutError from None

        def close(self) -> None:
            pass

    producer = threading.Thread(target=lambda: events.put(_BytesEvent(b"c")), daemon=True)
    producer.start()
    time.sleep(0.05)
    assert producer.is_alive()  # parked on the full queue

    iter_coroutine(_sync_transport(QueueSocket()).close())

    producer.join(2)
    assert not producer.is_alive()


def test_sync_reads_wake_when_closed_from_another_thread() -> None:
    # The websocket parks a reader in a queue with no deadline, so closing on
    # another thread has to be observable from inside a pending read.
    class QuietSession:
        def receive(self, timeout: float | None = None) -> object:
            time.sleep(timeout or 0.01)
            raise TimeoutError

    transport = SyncInteractiveTransport(InteractiveSessionState(url="wss://h.test/ws", token="t"))
    transport._session = cast(Any, QuietSession())

    def close_soon() -> None:
        time.sleep(0.3)
        transport._closed = True

    closer = threading.Thread(target=close_soon)
    closer.start()
    try:
        started = time.monotonic()
        frame = iter_coroutine(transport.receive())
        assert frame.end
        assert time.monotonic() - started < 5
    finally:
        closer.join()


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
async def test_interactive_operations_are_single_use(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        operation = box.open_interactive()
        async with operation:
            pass
        with pytest.raises(RuntimeError, match="can only be used once"):
            async with operation:
                pass


@respx.mock
def test_sync_interactive_operations_are_single_use(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v3/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/interactive").mock(
        return_value=httpx.Response(200, json=_interactive_response())
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        operation = box.open_interactive()
        with operation:
            pass
        with pytest.raises(RuntimeError, match="can only be used once"):
            with operation:
                pass


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

            # Waiting is lifecycle, not I/O: the output it timed out on must
            # still be there for the reader.
            assert pty.stream.read(13) == b"still running"


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
