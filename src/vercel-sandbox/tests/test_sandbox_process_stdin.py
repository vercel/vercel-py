"""Command stdin through the public sync and async APIs.

The fake service applies offsets the way the real one does: a request whose
offset is behind the bytes already written only appends the new suffix, so a
resent request can never duplicate input.
"""

import base64
import io
import json
import subprocess
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from typing import Any

import anyio
import httpx2 as httpx
import pytest
from anyio.from_thread import BlockingPortal, start_blocking_portal
from anyio.streams.text import TextSendStream
from hypothesis import given, settings, strategies as st
from sandbox_fixtures import sandbox_service_options

import vendor.respx as respx
from vercel import sandbox
from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.api import session
from vercel.sandbox import sync as sandbox_sync
from vercel.sandbox._internal.errors import SandboxApiError
from vercel.sandbox._internal.process_stdin import STDIN_CHUNK_SIZE, StdinCore, _UnsharedLock
from vercel.sandbox._internal.service import SandboxService

_BASE = "https://sandbox.test/v2/sandboxes/sessions/sbx_1"
_COMMAND: dict[str, object] = {
    "id": "cmd_1",
    "name": "cat",
    "args": [],
    "cwd": "/vercel/sandbox",
    "sessionId": "sbx_1",
    "exitCode": None,
    "startedAt": 1,
}


def _json(body: object, status: int = 200, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status, json=body, headers=headers)


def _failure(status: int, code: str = "failed", **extra: object) -> httpx.Response:
    return _json({"error": {"code": code, "message": code, **extra}}, status)


Fault = Callable[["_StdinServer", httpx.Request, dict[str, Any]], httpx.Response]


class _StdinServer:
    """Stands in for the stdin endpoint of one running ``cat`` process."""

    def __init__(self) -> None:
        self.data = bytearray()
        self.closed = False
        self.exited = False
        self.attached = True
        self.reading = True
        self.bodies: list[dict[str, Any]] = []
        self.faults: deque[Fault] = deque()

    def apply(self, body: dict[str, Any]) -> httpx.Response:
        chunk = base64.b64decode(body.get("data", ""))
        close = bool(body.get("close"))
        offset = body.get("offset")
        if self.exited:
            return _failure(404, "command_not_found_or_exited")
        if not self.attached:
            return _failure(400, "command_stdin_unavailable")
        if offset is None:
            new, end = chunk, None
        elif offset > len(self.data):
            return _failure(409, "command_stdin_offset_mismatch")
        else:
            new, end = chunk[len(self.data) - offset :], offset + len(chunk)
        # A replay that adds nothing succeeds even after stdin is closed, as
        # long as it ends where the stream does.
        replay = end is not None and not new
        if (self.closed or not self.reading) and (new or (close and not replay)):
            return _failure(400, "command_stdin_unavailable")
        if close and end is not None and end < len(self.data):
            return _failure(409, "command_stdin_offset_mismatch")
        self.data += new
        if close:
            self.closed = True
        return _json({"command": _COMMAND, "bytesWritten": len(self.data)})

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.bodies.append(body)
        if self.faults:
            return self.faults.popleft()(self, request, body)
        return self.apply(body)

    def writes(self) -> list[dict[str, Any]]:
        """Requests that carried data or a close, as opposed to offset probes."""
        return [body for body in self.bodies if body]


def _applied_then(status: int) -> Fault:
    def fault(server: _StdinServer, request: httpx.Request, body: dict[str, Any]) -> httpx.Response:
        server.apply(body)
        return _failure(status)

    return fault


def _applied_then_disconnect(
    server: _StdinServer, request: httpx.Request, body: dict[str, Any]
) -> httpx.Response:
    server.apply(body)
    raise httpx.RemoteProtocolError("Server disconnected", request=request)


def _rate_limited(
    server: _StdinServer, request: httpx.Request, body: dict[str, Any]
) -> httpx.Response:
    return _failure(429, "rate_limited", retryAfter=0)


def _rejected(status: int) -> Fault:
    def fault(server: _StdinServer, request: httpx.Request, body: dict[str, Any]) -> httpx.Response:
        return _failure(status)

    return fault


class _LazyStream(httpx.SyncByteStream, httpx.AsyncByteStream):
    """Yield records one at a time so later output can depend on stdin."""

    def __init__(self, records: Callable[[], Iterator[object]]) -> None:
        self._records = records

    def __iter__(self) -> Iterator[bytes]:
        for record in self._records():
            yield (json.dumps(record) + "\n").encode()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self:
            yield chunk


class _Api:
    def __init__(self, router: respx.MockRouter, server: _StdinServer) -> None:
        self.server = server
        self.created: list[dict[str, Any]] = []
        self.signals: list[int] = []
        self.interrupt_run = False
        router.post("https://sandbox.test/v3/sandboxes").mock(
            return_value=_json(
                {
                    "sandbox": {"name": "box", "currentSessionId": "sbx_1", "status": "running"},
                    "session": {
                        "id": "sbx_1",
                        "sourceSandboxName": "box",
                        "projectId": "prj_1",
                        "status": "running",
                        "cwd": "/vercel/sandbox",
                    },
                }
            )
        )
        router.post(f"{_BASE}/cmd").mock(side_effect=self._create)
        router.post(f"{_BASE}/cmd/cmd_1/stdin").mock(side_effect=server)
        router.get(f"{_BASE}/cmd/cmd_1/logs").mock(
            side_effect=lambda _: httpx.Response(200, text=self._stdout_record() + "\n")
        )
        router.get(f"{_BASE}/cmd").mock(return_value=_json({"commands": [_COMMAND]}))
        router.get(f"{_BASE}/cmd/cmd_1").mock(
            return_value=_json({"command": {**_COMMAND, "exitCode": 0}})
        )
        router.post(f"{_BASE}/cmd/cmd_1/kill").mock(side_effect=self._kill)

    def _stdout_record(self) -> str:
        return json.dumps({"stream": "stdout", "data": self.server.data.decode(errors="replace")})

    def _create(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.created.append(body)
        if not body.get("wait"):
            return _json({"command": _COMMAND})

        def records() -> Iterator[object]:
            yield {"command": _COMMAND}
            if self.interrupt_run:
                raise httpx.ReadError("connection reset by peer")
            yield {"stream": "stdout", "data": self.server.data.decode(errors="replace")}
            yield {"command": {**_COMMAND, "exitCode": 0}}

        return httpx.Response(200, stream=_LazyStream(records))

    def _kill(self, request: httpx.Request) -> httpx.Response:
        self.signals.append(json.loads(request.content)["signal"])
        self.server.exited = True
        return _json({"command": _COMMAND})


class _Handle:
    """One process driven through either the sync or the async API."""

    def __init__(self, process: Any, portal: BlockingPortal | None) -> None:
        self.process = process
        self._portal = portal

    @property
    def broken(self) -> type[Exception]:
        return BrokenPipeError if self._portal is None else anyio.BrokenResourceError

    def _call(self, function: Callable[..., Any], *args: Any) -> Any:
        return function(*args) if self._portal is None else self._portal.call(function, *args)

    def write(self, data: bytes) -> None:
        stdin = self.process.stdin
        self._call(stdin.write if self._portal is None else stdin.send, data)

    def close(self) -> None:
        stdin = self.process.stdin
        self._call(stdin.close if self._portal is None else stdin.aclose)

    def communicate(self, input: object = None) -> tuple[str | None, str | None]:
        return self._call(self.process.communicate, input)  # type: ignore[no-any-return]


class _Driver:
    def __init__(self, api: _Api, box: Any, portal: BlockingPortal | None) -> None:
        self.api = api
        self.server = api.server
        self.box = box
        self.portal = portal

    def _call(self, function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if self.portal is None:
            return function(*args, **kwargs)
        return self.portal.call(lambda: function(*args, **kwargs))

    def create_command(self, **kwargs: Any) -> _Handle:
        return _Handle(self._call(self.box.create_command, "cat", **kwargs), self.portal)

    def get_command(self) -> _Handle:
        return _Handle(self._call(self.box.get_command, "cmd_1"), self.portal)

    def query_commands(self) -> list[_Handle]:
        return [_Handle(process, self.portal) for process in self._call(self.box.query_commands)]

    def run_command(self, **kwargs: Any) -> sandbox.CompletedCommand:
        return self._call(self.box.run_command, "cat", **kwargs)  # type: ignore[no-any-return]


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    delays: list[float] = []

    async def sleep(self: SandboxService, seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr(SandboxService, "sleep", sleep)
    return delays


@contextmanager
def _open_driver(mode: str, server: _StdinServer) -> Iterator[_Driver]:
    with respx.mock(assert_all_called=False) as router:
        api = _Api(router, server)
        if mode == "sync":
            with session(service_options=sandbox_service_options(team_id="team_1", sync=True)):
                yield _Driver(api, sandbox_sync.create_sandbox(name="box"), None)
            return
        # Portal calls run in fresh tasks that can't see an active session, so
        # the async API is driven through a standalone client.
        (options,) = sandbox_service_options(team_id="team_1", sync=False)
        client = sandbox.SandboxClient.create(options=options)  # type: ignore[arg-type]

        async def create_box() -> sandbox.Sandbox:
            return await client.create_sandbox(name="box")

        with start_blocking_portal(backend=mode) as portal:
            try:
                yield _Driver(api, portal.call(create_box), portal)
            finally:
                portal.call(client.aclose)


_MODES = ["sync", "asyncio", "trio"]


@pytest.fixture(params=_MODES)
def driver(
    request: pytest.FixtureRequest, mock_env_clear: None, sleeps: list[float]
) -> Iterator[_Driver]:
    with _open_driver(request.param, _StdinServer()) as opened:
        yield opened


def test_writes_reach_the_process_in_order_and_close_sends_eof(driver: _Driver) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)

    process.write(b"hello ")
    process.write(bytearray(b"wor"))
    process.write(memoryview(b"ld\n"))
    process.close()
    process.close()

    assert driver.api.created == [
        {"command": "cat", "args": [], "sudo": False, "attachStdin": True}
    ]
    assert driver.server.data == b"hello world\n"
    assert driver.server.closed
    assert driver.server.bodies[0] == {}
    assert [body.get("offset") for body in driver.server.writes()] == [0, 6, 9, 12]
    assert process.communicate() == ("hello world\n", "")


@pytest.mark.parametrize("lookup", ["get_command", "query_commands"])
def test_a_looked_up_process_continues_after_input_already_written(
    driver: _Driver, lookup: str
) -> None:
    started = driver.create_command(stdin=subprocess.PIPE)
    started.write(b"first ")

    if lookup == "get_command":
        fetched = driver.get_command()
    else:
        (fetched,) = driver.query_commands()
    fetched.write(b"second\n")
    fetched.close()

    assert driver.server.data == b"first second\n"
    assert driver.server.closed
    assert [body.get("offset") for body in driver.server.writes()] == [0, 6, 13]


@pytest.mark.parametrize(
    ("written", "input", "data", "closed"),
    [
        (b"", None, b"", False),
        (b"", b"", b"", True),
        (b"", "x", b"x", True),
        (b"later", None, b"later", True),
    ],
    ids=["no-input", "empty-input", "input", "written-then-no-input"],
)
def test_communicate_closes_a_looked_up_process_stdin_only_when_it_owns_it(
    driver: _Driver, written: bytes, input: object, data: bytes, closed: bool
) -> None:
    fetched = driver.get_command()
    if written:
        fetched.write(written)

    fetched.communicate(input)

    assert driver.server.data == data
    assert driver.server.closed is closed
    if not closed:
        assert driver.server.bodies == []


def test_a_looked_up_process_without_stdin_refuses_input(driver: _Driver) -> None:
    driver.server.attached = False
    fetched = driver.get_command()

    for _ in range(2):
        with pytest.raises(sandbox.SandboxStdinError, match="stdin=subprocess.PIPE"):
            fetched.write(b"data")
    with pytest.raises(sandbox.SandboxStdinError, match="stdin=subprocess.PIPE"):
        driver.get_command().communicate("data")

    # Nothing was attached, so there's nothing to close.
    fetched.close()


@pytest.mark.parametrize("stopped", ["reading", "exited"])
def test_a_looked_up_process_that_stops_taking_input_breaks_the_pipe(
    driver: _Driver, stopped: str
) -> None:
    fetched = driver.get_command()
    fetched.write(b"a")
    if stopped == "reading":
        driver.server.reading = False
    else:
        driver.server.exited = True

    with pytest.raises(fetched.broken):
        fetched.write(b"b")
    assert fetched.communicate(b"more") == ("a", "")


def test_stdin_is_not_attached_unless_requested(driver: _Driver) -> None:
    for stdin in (None, subprocess.DEVNULL):
        process = driver.create_command(stdin=stdin)
        assert process.process.stdin is None
        with pytest.raises(ValueError, match="stdin=subprocess.PIPE"):
            process.communicate("input")

    assert all("attachStdin" not in body for body in driver.api.created)
    assert driver.server.bodies == []


@pytest.mark.parametrize(
    ("input", "expected"),
    [("héllo\n", b"h\xc3\xa9llo\n"), (b"\x00\xff", b"\x00\xff"), ("", b""), (None, b"")],
)
def test_communicate_writes_input_and_closes_stdin(
    driver: _Driver, input: object, expected: bytes
) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)

    stdout, _ = process.communicate(input)

    assert driver.server.data == expected
    assert driver.server.closed
    assert stdout == expected.decode(errors="replace")


def test_large_writes_are_split_into_bounded_requests(driver: _Driver) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)
    data = bytes(range(256)) * (2 * STDIN_CHUNK_SIZE // 256) + b"!"

    process.write(data)

    sizes = [len(base64.b64decode(body["data"])) for body in driver.server.writes()]
    assert sizes == [STDIN_CHUNK_SIZE, STDIN_CHUNK_SIZE, 1]
    assert driver.server.data == data


@pytest.mark.parametrize(
    "fault",
    [_applied_then_disconnect, _applied_then(502), _applied_then(503), _rate_limited],
    ids=["network", "502", "503", "429"],
)
def test_transient_failures_resend_the_same_request(
    driver: _Driver, sleeps: list[float], fault: Fault
) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)
    process.write(b"a")
    driver.server.faults.append(fault)

    process.write(b"bc")
    process.write(b"d")

    assert driver.server.data == b"abcd"
    first, resent = driver.server.writes()[1:3]
    assert first == resent == {"data": base64.b64encode(b"bc").decode(), "offset": 1}
    assert len(sleeps) == 1


def test_a_write_that_may_have_landed_breaks_the_stream_until_closed(
    driver: _Driver, sleeps: list[float]
) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)
    process.write(b"a")
    driver.server.faults.extend([_applied_then(503)] * 6)

    with pytest.raises(sandbox.SandboxStdinError) as failure:
        process.write(b"bc")
    assert isinstance(failure.value.cause, SandboxApiError)
    assert failure.value.cause.status_code == 503
    assert len(sleeps) == 5

    sent = len(driver.server.bodies)
    with pytest.raises(process.broken):
        process.write(b"d")
    assert len(driver.server.bodies) == sent

    process.close()
    assert driver.server.data == b"abc"
    assert driver.server.closed
    assert driver.server.bodies[-2:] == [{}, {"offset": 3, "close": True}]


@pytest.mark.parametrize("status", [400, 401, 409, 500])
def test_rejected_requests_are_not_resent(driver: _Driver, status: int) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)
    process.write(b"a")
    driver.server.faults.append(_rejected(status))

    with pytest.raises(sandbox.SandboxStdinError):
        process.write(b"b")

    assert len(driver.server.writes()) == 2
    if status < 500:
        # Rejected before applying anything, so the stream is still usable.
        process.write(b"c")
        assert driver.server.data == b"ac"
    else:
        with pytest.raises(process.broken):
            process.write(b"c")


def test_exited_process_breaks_the_pipe_but_not_communicate_or_close(driver: _Driver) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)
    process.write(b"a")
    driver.server.exited = True

    with pytest.raises(process.broken):
        process.write(b"b")
    with pytest.raises(process.broken):
        process.write(b"c")
    assert process.communicate(b"more") == ("a", "")


def test_timeouts_are_resent_while_the_process_keeps_reading(driver: _Driver) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)
    process.write(b"a")

    def slow_reader(written: int) -> Fault:
        def fault(
            server: _StdinServer, request: httpx.Request, body: dict[str, Any]
        ) -> httpx.Response:
            chunk = base64.b64decode(body["data"])
            server.apply({**body, "data": base64.b64encode(chunk[: written - 1]).decode()})
            return _failure(504, "command_stdin_timeout", bytesWritten=written)

        return fault

    driver.server.faults.extend([slow_reader(3), slow_reader(5)])
    process.write(b"bcdef")
    assert driver.server.data == b"abcdef"

    def stalled(
        server: _StdinServer, request: httpx.Request, body: dict[str, Any]
    ) -> httpx.Response:
        return _failure(504, "command_stdin_timeout", bytesWritten=len(server.data))

    # A timeout that reports no progress, or no bytes written at all, is
    # stalled, and the request is given up after a second one.
    for fault in (stalled, _rejected(504)):
        process = driver.create_command(stdin=subprocess.PIPE)
        process.write(b"g")
        sent, offset = len(driver.server.bodies), len(driver.server.data)
        driver.server.faults.extend([fault, fault])
        with pytest.raises(sandbox.SandboxStdinError) as failure:
            process.write(b"h")
        assert isinstance(failure.value.cause, SandboxApiError)
        assert failure.value.cause.status_code == 504
        resent = driver.server.bodies[sent:]
        assert resent == [{"data": base64.b64encode(b"h").decode(), "offset": offset}] * 2


@pytest.mark.parametrize("exits", [False, True], ids=["still-running", "exited"])
def test_a_close_whose_response_is_lost_is_resent(driver: _Driver, exits: bool) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)
    process.write(b"a")

    def close_then_disconnect(
        server: _StdinServer, request: httpx.Request, body: dict[str, Any]
    ) -> httpx.Response:
        server.apply(body)
        server.exited = exits
        raise httpx.ReadError("connection reset", request=request)

    driver.server.faults.append(close_then_disconnect)
    process.close()

    assert driver.server.closed
    assert driver.server.bodies[-2:] == [{"offset": 1, "close": True}] * 2


def test_a_failed_close_can_be_retried(driver: _Driver) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)
    process.write(b"a")
    driver.server.faults.append(_rejected(500))

    with pytest.raises(sandbox.SandboxStdinError):
        process.close()
    assert not driver.server.closed
    process.write(b"b")

    process.close()
    assert driver.server.closed
    assert driver.server.data == b"ab"
    assert driver.server.writes() == [
        {"data": base64.b64encode(b"a").decode(), "offset": 0},
        {"offset": 1, "close": True},
        {"data": base64.b64encode(b"b").decode(), "offset": 1},
        {"offset": 2, "close": True},
    ]


def test_writes_fail_before_writing_when_the_offset_is_unknown(driver: _Driver) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)
    driver.server.faults.append(lambda server, request, body: _json({"command": _COMMAND}))

    with pytest.raises(sandbox.SandboxStdinError) as failure:
        process.write(b"a")

    assert isinstance(failure.value.cause, sandbox.SandboxResponseError)
    assert driver.server.writes() == []
    # Nothing was written, so a later write asks again and succeeds.
    process.write(b"b")
    assert driver.server.data == b"b"


def test_run_command_feeds_input_while_streaming_output(driver: _Driver) -> None:
    result = driver.run_command(input="hello\n", capture_output=True)

    assert result.stdout == "hello\n"
    assert result.returncode == 0
    assert driver.api.created[0]["attachStdin"] is True
    assert driver.server.data == b"hello\n"
    assert driver.server.closed


def test_run_command_feeds_input_once_when_its_output_stream_drops(driver: _Driver) -> None:
    driver.api.interrupt_run = True

    result = driver.run_command(input="hello\n", capture_output=True)

    # The command keeps running, so its output is followed from the logs
    # endpoint instead of starting it, and writing its input, again.
    assert result.stdout == "hello\n"
    assert len(driver.api.created) == 1
    assert driver.server.writes() == [
        {"data": base64.b64encode(b"hello\n").decode(), "offset": 0},
        {"offset": 6, "close": True},
    ]


def test_run_command_without_input_does_not_attach_stdin(driver: _Driver) -> None:
    driver.run_command(capture_output=True)

    assert "attachStdin" not in driver.api.created[0]
    assert driver.server.bodies == []


def test_run_command_kills_the_process_when_input_fails(driver: _Driver) -> None:
    driver.server.faults.extend([lambda server, _, body: server.apply(body), _rejected(500)])

    with pytest.raises(sandbox.SandboxStdinError):
        driver.run_command(input=b"data", capture_output=True)

    assert driver.api.signals == [sandbox.CommandSignal.SIGKILL]
    assert len(driver.api.created) == 1


@pytest.mark.parametrize("raises", [False, True], ids=["returns", "raises"])
def test_run_command_on_start_runs_before_input_is_written(driver: _Driver, raises: bool) -> None:
    error = RuntimeError("hook failed")
    seen: list[tuple[str, bytes, bool]] = []

    def on_start(process: Any) -> None:
        seen.append((process.id, bytes(driver.server.data), driver.server.closed))
        if raises:
            raise error

    if raises:
        with pytest.raises(RuntimeError) as raised:
            driver.run_command(input="hello\n", capture_output=True, on_start=on_start)
        assert raised.value is error
    else:
        result = driver.run_command(input="hello\n", capture_output=True, on_start=on_start)
        assert result.stdout == "hello\n"

    assert seen == [("cmd_1", b"", False)]
    # The input is written and stdin closed even when on_start raises, so the
    # process is not left waiting for input.
    assert (driver.server.data, driver.server.closed) == (b"hello\n", True)
    assert driver.api.signals == []


def test_run_command_on_start_error_is_raised_when_input_also_fails(driver: _Driver) -> None:
    driver.server.faults.extend([lambda server, _, body: server.apply(body), _rejected(500)])
    error = RuntimeError("hook failed")

    def on_start(_process: Any) -> None:
        raise error

    with pytest.raises(RuntimeError) as raised:
        driver.run_command(input=b"data", capture_output=True, on_start=on_start)

    assert raised.value is error
    assert raised.value.__context__ is None
    assert driver.api.signals == [sandbox.CommandSignal.SIGKILL]


@pytest.mark.parametrize("stdin", [subprocess.STDOUT, 42, True, io.BytesIO(), "pipe"])
def test_create_command_rejects_stdin_options_before_request(
    driver: _Driver, stdin: object
) -> None:
    with pytest.raises((TypeError, ValueError)):
        driver.create_command(stdin=stdin)

    assert driver.api.created == []


def test_stdin_takes_bytes_not_text(driver: _Driver) -> None:
    process = driver.create_command(stdin=subprocess.PIPE)

    with pytest.raises(TypeError, match="bytes"):
        process.write("text")  # type: ignore[arg-type]

    assert driver.server.writes() == []


def test_sync_stdin_composes_with_text_wrappers(mock_env_clear: None) -> None:
    with _open_driver("sync", _StdinServer()) as driver:
        process = driver.box.create_command("cat", stdin=subprocess.PIPE)
        with io.TextIOWrapper(
            process.stdin, encoding="utf-8", newline="\n", write_through=True
        ) as text:
            text.write("héllo\n")
            text.write("wörld\n")
        assert process.stdin.closed
        assert driver.server.data == "héllo\nwörld\n".encode()
        assert driver.server.closed

        process = driver.box.create_command("cat", stdin=subprocess.PIPE)
        process.stdin.close()
        with pytest.raises(ValueError, match="closed"):
            process.stdin.write(b"late")


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
@pytest.mark.anyio
async def test_async_stdin_orders_concurrent_sends_and_composes_with_anyio(
    anyio_backend: str, mock_env_clear: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = _StdinServer()
    release = anyio.Event()

    async def held_sleep(self: SandboxService, seconds: float) -> None:
        await release.wait()

    monkeypatch.setattr(SandboxService, "sleep", held_sleep)
    with respx.mock(assert_all_called=False) as router:
        _Api(router, server)
        options = sandbox_service_options(team_id="team_1", sync=False)
        async with session(service_options=options):
            box = await sandbox.create_sandbox(name="box")
            process = await box.create_command("cat", stdin=subprocess.PIPE)
            assert isinstance(process.stdin, anyio.abc.ByteSendStream)
            # The first send backs off after a failure and holds the stream
            # until released, so every later send queues up behind it.
            server.faults.append(_applied_then(503))

            async with anyio.create_task_group() as tasks:
                for index in range(10):
                    tasks.start_soon(process.stdin.send, f"{index};".encode())
                    await anyio.wait_all_tasks_blocked()
                release.set()

            async with TextSendStream(process.stdin) as text:
                await text.send("done\n")
            with pytest.raises(anyio.ClosedResourceError):
                await process.stdin.send(b"late")

    assert server.data == b"".join(f"{index};".encode() for index in range(10)) + b"done\n"
    assert server.closed


def test_sync_stdin_keeps_writes_from_threads_whole(
    mock_env_clear: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = _StdinServer()
    holding, release = threading.Event(), threading.Event()

    async def held_sleep(self: SandboxService, seconds: float) -> None:
        holding.set()
        release.wait(timeout=5)

    monkeypatch.setattr(SandboxService, "sleep", held_sleep)
    # Every write spans several requests, so one that interleaved with another
    # thread's would garble both.
    monkeypatch.setattr("vercel.sandbox._internal.process_stdin.STDIN_CHUNK_SIZE", 2)
    payloads = [f"{index}" * 6 + ";" for index in range(5)]
    with _open_driver("sync", server) as driver:
        process = driver.box.create_command("cat", stdin=subprocess.PIPE)
        # The first write backs off after a failure and holds the stream until
        # released, while the other threads queue up behind it.
        server.faults.append(_applied_then(503))
        first = threading.Thread(target=process.stdin.write, args=(payloads[0].encode(),))
        first.start()
        assert holding.wait(timeout=5)
        held = len(server.bodies)
        rest = [
            threading.Thread(target=process.stdin.write, args=(payload.encode(),))
            for payload in payloads[1:]
        ]
        for thread in rest:
            thread.start()
        time.sleep(0.05)
        assert len(server.bodies) == held
        release.set()
        for thread in [first, *rest]:
            thread.join(timeout=5)
        process.stdin.close()

    delivered = server.data.decode().split(";")[:-1]
    assert delivered[0] == payloads[0][:-1]
    assert sorted(delivered) == [payload[:-1] for payload in payloads]
    assert server.closed


class _FakeStdinService:
    """In-memory stdin service that may fail each request after applying it."""

    def __init__(self, failures: list[str]) -> None:
        self.server = _StdinServer()
        self.failures = deque(failures)

    async def write_process_stdin(
        self, *, session_id: str, process_id: str, data: bytes, offset: int | None, close: bool
    ) -> int | None:
        body: dict[str, Any] = {}
        if data:
            body["data"] = base64.b64encode(data).decode()
        if offset is not None:
            body["offset"] = offset
        if close:
            body["close"] = True
        failure = self.failures.popleft() if self.failures else "ok"
        if failure == "rate_limited":
            raise SandboxApiError(_failure(429), "rate limited")
        response = self.server.apply(body)
        if failure == "disconnect":
            raise httpx.RemoteProtocolError("Server disconnected")
        if failure == "unavailable":
            raise SandboxApiError(_failure(503), "unavailable")
        assert response.is_success
        return response.json()["bytesWritten"]  # type: ignore[no-any-return]

    async def sleep(self, seconds: float) -> None:
        pass


@settings(deadline=None, max_examples=200)
@given(
    writes=st.lists(st.binary(max_size=24), max_size=8),
    failures=st.lists(st.sampled_from(["ok", "disconnect", "unavailable", "rate_limited"])),
    chunk_size=st.integers(min_value=1, max_value=8),
)
def test_transient_failures_never_lose_or_duplicate_input(
    writes: list[bytes], failures: list[str], chunk_size: int
) -> None:
    """Delivered bytes equal the concatenated writes, or the stream is broken.

    Retrying stops after a bounded budget, so a long run of failures may fail
    a write. Until then, and for every write that succeeded, the service holds
    exactly the bytes that were written, in order.
    """
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("vercel.sandbox._internal.process_stdin.STDIN_CHUNK_SIZE", chunk_size)
        _check_delivery(writes, failures)


def _check_delivery(writes: list[bytes], failures: list[str]) -> None:
    service = _FakeStdinService(failures)
    core = StdinCore(
        service=service,  # type: ignore[arg-type]
        session_id="sbx_1",
        process_id="cmd_1",
        lock=_UnsharedLock(),
    )
    accepted = b""
    for data in writes:
        try:
            iter_coroutine(core.write(data))
        except sandbox.SandboxStdinError:
            delivered = bytes(service.server.data)
            assert delivered.startswith(accepted)
            assert (accepted + data).startswith(delivered)
            return
        accepted += data
        assert service.server.data == accepted
