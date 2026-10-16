"""Behavioral tests for ``open_process`` live output routing and block exit."""

import io
import json
import signal
import subprocess
import threading
from collections.abc import AsyncIterator, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TextIO

import anyio
import httpx2 as httpx
import pytest
from hypothesis import HealthCheck, given, settings, strategies as st
from sandbox_fixtures import sandbox_service_options

import vendor.respx as respx
from vercel import sandbox
from vercel._internal.core.options import ServiceOptions
from vercel.api import session
from vercel.sandbox import sync as sandbox_sync

CREATE_URL = "https://sandbox.test/v3/sandboxes"
COMMAND_URL = "https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd"
SIGNAL_URL = "https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/kill"
DESTINATIONS = ("capture", "merged", "sinks")
TARGETS = ("sandbox", "session")


def _sandbox_response(*, session_id: str = "sbx_1") -> dict[str, object]:
    return {
        "sandbox": {"name": "preview", "currentSessionId": session_id, "status": "running"},
        "session": {
            "id": session_id,
            "sourceSandboxName": "preview",
            "projectId": "prj_1",
            "status": "running",
            "cwd": "/vercel/sandbox",
        },
    }


def _process(returncode: int | None = None, *, session_id: str = "sbx_1") -> dict[str, object]:
    return {
        "command": {
            "id": "cmd_1",
            "name": "python",
            "args": [],
            "cwd": "/vercel/sandbox",
            "sessionId": session_id,
            "exitCode": returncode,
            "startedAt": 1,
        }
    }


def _output(stream: str, data: str) -> dict[str, object]:
    return {"stream": stream, "data": data}


def _ndjson(*records: object) -> bytes:
    return "".join(json.dumps(record) + "\n" for record in records).encode()


def _session_options() -> list[ServiceOptions]:
    return sandbox_service_options(team_id="team_1", project_id="prj_1")


def _open_target(
    box: sandbox.Sandbox, target: str
) -> sandbox.Sandbox | sandbox.SandboxRuntimeSession:
    if target == "sandbox":
        return box
    current = box.current_session
    assert current is not None
    return current


def _sync_open_target(
    box: sandbox_sync.SyncSandbox, target: str
) -> sandbox_sync.SyncSandbox | sandbox_sync.SyncSandboxRuntimeSession:
    if target == "sandbox":
        return box
    current = box.current_session
    assert current is not None
    return current


class _ObservedTextIO(io.StringIO):
    """A text sink that reports each write as it happens."""

    def __init__(self, on_write: Callable[[str], None]) -> None:
        super().__init__()
        self._on_write = on_write

    def write(self, value: str) -> int:
        written = super().write(value)
        self._on_write(value)
        return written


class _FailingTextIO(io.StringIO):
    def write(self, value: str) -> int:
        raise OSError("sink write failed")


class _GatedAsyncStream(httpx.AsyncByteStream):
    """Yield ``before``, block until ``gate`` is set, then yield ``after``."""

    def __init__(self, *, before: bytes, after: bytes, gate: anyio.Event) -> None:
        self._before = before
        self._after = after
        self._gate = gate
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._before
        with anyio.fail_after(5):
            await self._gate.wait()
        yield self._after

    async def aclose(self) -> None:
        self.closed = True


class _GatedSyncStream(httpx.SyncByteStream):
    """Yield ``before``, block until ``gate`` is set, then yield ``after``."""

    def __init__(self, *, before: bytes, after: bytes, gate: threading.Event) -> None:
        self._before = before
        self._after = after
        self._gate = gate
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield self._before
        if not self._gate.wait(timeout=5):
            raise TimeoutError("test never released the gated stream")
        yield self._after

    def close(self) -> None:
        self.closed = True


class _TrackingAsyncStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes) -> None:
        self._content = content
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._content

    async def aclose(self) -> None:
        self.closed = True


class _TrackingSyncStream(httpx.SyncByteStream):
    def __init__(self, content: bytes) -> None:
        self._content = content
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield self._content

    def close(self) -> None:
        self.closed = True


def _stopped_error(status_code: int, code: str) -> sandbox.SandboxApiError:
    data = {"error": {"code": code, "message": "session stopped"}}
    return sandbox.SandboxApiError(httpx.Response(status_code), "session stopped", data=data)


# -- Live routing while the block runs ---------------------------------------


@pytest.mark.parametrize("target", TARGETS)
@respx.mock
async def test_async_open_process_routes_output_while_block_runs(
    mock_env_clear: None, target: str
) -> None:
    gate = anyio.Event()
    output_seen = anyio.Event()

    def on_write(text: str) -> None:
        if "out-1" in text:
            output_seen.set()

    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(
            200,
            stream=_GatedAsyncStream(
                before=_ndjson(_process(), _output("stdout", "out-1\n")),
                after=_ndjson(_output("stdout", "out-2\n"), _process(0)),
                gate=gate,
            ),
        )
    )
    sink = _ObservedTextIO(on_write)

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        async with _open_target(box, target).open_process("python", stdout=sink) as proc:
            with anyio.fail_after(5):
                await output_seen.wait()
            assert proc.id == "cmd_1"
            assert proc.session_id == "sbx_1"
            assert proc.returncode is None
            assert sink.getvalue() == "out-1\n"
            with pytest.raises(RuntimeError, match="has not completed"):
                _ = proc.result
            gate.set()
            waited = await proc.wait()

        assert waited is proc.result
        assert waited.returncode == 0
        assert waited.stdout is None
        assert sink.getvalue() == "out-1\nout-2\n"


@pytest.mark.parametrize("target", TARGETS)
@respx.mock
def test_sync_open_process_output_is_read_by_the_caller(mock_env_clear: None, target: str) -> None:
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(
            200,
            stream=_TrackingSyncStream(
                _ndjson(
                    _process(),
                    _output("stdout", "out-1\n"),
                    _output("stdout", "out-2\n"),
                    _output("stdout", "out-3\n"),
                    _process(0),
                )
            ),
        )
    )
    sink = io.StringIO()

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with _sync_open_target(box, target).open_process("python", stdout=sink) as proc:
            assert proc.id == "cmd_1"
            assert proc.session_id == "sbx_1"
            assert proc.returncode is None
            assert sink.getvalue() == ""

            assert next(proc).data == "out-1\n"
            assert sink.getvalue() == "out-1\n"

            for event in proc:
                assert event.data == "out-2\n"
                break
            assert sink.getvalue() == "out-1\nout-2\n"

            assert [event.data for event in proc] == ["out-3\n"]
            waited = proc.wait()
            assert waited is proc.result
            assert list(proc) == []

    assert waited.returncode == 0
    assert waited.stdout is None
    assert sink.getvalue() == "out-1\nout-2\nout-3\n"


# -- Parity with run_process -------------------------------------------------

_output_records = st.lists(
    st.tuples(st.sampled_from(["stdout", "stderr"]), st.text(max_size=8)),
    max_size=6,
)


def _destination_kwargs(
    destinations: str,
) -> tuple[dict[str, Any], io.StringIO | None, io.StringIO | None]:
    if destinations == "capture":
        return {"capture_output": True}, None, None
    if destinations == "merged":
        return {"stdout": subprocess.PIPE, "stderr": subprocess.STDOUT}, None, None
    stdout, stderr = io.StringIO(), io.StringIO()
    return {"stdout": stdout, "stderr": stderr}, stdout, stderr


def _observe(
    result: sandbox.CompletedProcess,
    stdout_sink: io.StringIO | None,
    stderr_sink: io.StringIO | None,
) -> tuple[object, ...]:
    return (
        result.returncode,
        result.stdout,
        result.stderr,
        None if stdout_sink is None else stdout_sink.getvalue(),
        None if stderr_sink is None else stderr_sink.getvalue(),
    )


def _parity_body(outputs: list[tuple[str, str]], returncode: int) -> bytes:
    records = [
        _process(),
        *(_output(stream, data) for stream, data in outputs),
        _process(returncode),
    ]
    return _ndjson(*records)


def _parity_sync(
    outputs: list[tuple[str, str]], returncode: int, destinations: str
) -> tuple[tuple[object, ...], tuple[object, ...], list[tuple[str, str]]]:
    body = _parity_body(outputs, returncode)
    with respx.mock:
        respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
        respx.post(COMMAND_URL).mock(
            side_effect=[httpx.Response(200, content=body), httpx.Response(200, content=body)]
        )
        with session(service_options=_session_options()):
            box = sandbox_sync.create_sandbox(name="preview")
            run_kwargs, run_out, run_err = _destination_kwargs(destinations)
            ran = box.run_process("python", **run_kwargs)
            open_kwargs, open_out, open_err = _destination_kwargs(destinations)
            with box.open_process("python", **open_kwargs) as proc:
                events = [(event.stream.value, event.data) for event in proc]
            opened = proc.result
    return (
        _observe(ran, run_out, run_err),
        _observe(opened, open_out, open_err),
        events,
    )


async def _parity_async(
    outputs: list[tuple[str, str]], returncode: int, destinations: str
) -> tuple[tuple[object, ...], tuple[object, ...]]:
    body = _parity_body(outputs, returncode)
    with respx.mock:
        respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
        respx.post(COMMAND_URL).mock(
            side_effect=[httpx.Response(200, content=body), httpx.Response(200, content=body)]
        )
        async with session(service_options=_session_options()):
            box = await sandbox.create_sandbox(name="preview")
            run_kwargs, run_out, run_err = _destination_kwargs(destinations)
            ran = await box.run_process("python", **run_kwargs)
            open_kwargs, open_out, open_err = _destination_kwargs(destinations)
            async with box.open_process("python", **open_kwargs) as proc:
                pass
            opened = proc.result
    return _observe(ran, run_out, run_err), _observe(opened, open_out, open_err)


@pytest.mark.usefixtures("mock_env_clear")
@pytest.mark.parametrize("destinations", DESTINATIONS)
@pytest.mark.parametrize("execution", ["async", "sync"])
@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    outputs=_output_records,
    returncode=st.integers(min_value=-128, max_value=255),
)
def test_open_process_matches_run_process(
    execution: str,
    destinations: str,
    outputs: list[tuple[str, str]],
    returncode: int,
) -> None:
    if execution == "async":
        expected, actual = anyio.run(_parity_async, outputs, returncode, destinations)
    else:
        expected, actual, events = _parity_sync(outputs, returncode, destinations)
        assert events == outputs
    assert actual == expected


# -- Block exit semantics ------------------------------------------------------


@pytest.mark.parametrize("target", TARGETS)
@respx.mock
async def test_async_body_exception_propagates_and_closes_without_killing(
    mock_env_clear: None, target: str
) -> None:
    gate = anyio.Event()
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    stream = _GatedAsyncStream(
        before=_ndjson(_process(), _output("stdout", "out\n")),
        after=_ndjson(_process(0)),
        gate=gate,
    )
    respx.post(COMMAND_URL).mock(return_value=httpx.Response(200, stream=stream))
    kill = respx.post(SIGNAL_URL)
    error = ValueError("body failed")

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        with pytest.raises(ValueError) as exc_info:
            async with _open_target(box, target).open_process("python") as proc:
                raise error

        assert exc_info.value is error
        assert stream.closed
        assert not kill.called
        with pytest.raises(RuntimeError, match="has not completed"):
            _ = proc.result


@pytest.mark.parametrize("target", TARGETS)
@respx.mock
def test_sync_body_exception_propagates_and_closes_without_killing(
    mock_env_clear: None, target: str
) -> None:
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    stream = _TrackingSyncStream(_ndjson(_process(), _output("stdout", "out\n"), _process(0)))
    respx.post(COMMAND_URL).mock(return_value=httpx.Response(200, stream=stream))
    kill = respx.post(SIGNAL_URL)
    error = ValueError("body failed")

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with pytest.raises(ValueError) as exc_info:
            with _sync_open_target(box, target).open_process("python") as proc:
                raise error

        assert exc_info.value is error
        assert stream.closed
        assert not kill.called
        with pytest.raises(RuntimeError, match="has not completed"):
            _ = proc.result
        with pytest.raises(RuntimeError, match="exited before"):
            proc.wait()


@respx.mock
async def test_async_cancelled_block_closes_stream_and_propagates_cancellation(
    mock_env_clear: None,
) -> None:
    gate = anyio.Event()
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    stream = _GatedAsyncStream(
        before=_ndjson(_process(), _output("stdout", "out\n")),
        after=_ndjson(_process(0)),
        gate=gate,
    )
    respx.post(COMMAND_URL).mock(return_value=httpx.Response(200, stream=stream))
    kill = respx.post(SIGNAL_URL)
    body_finished = False

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        with anyio.move_on_after(0.05) as scope:
            async with box.open_process("python") as proc:
                await anyio.sleep(10)
                body_finished = True

        assert scope.cancelled_caught
        assert not body_finished
        assert stream.closed
        assert not kill.called
        assert proc.returncode is None
        with pytest.raises(RuntimeError, match="has not completed"):
            _ = proc.result


@respx.mock
async def test_async_kill_inside_block_signals_the_process(mock_env_clear: None) -> None:
    gate = anyio.Event()
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(
            200,
            stream=_GatedAsyncStream(
                before=_ndjson(_process()),
                after=_ndjson(_process(-9)),
                gate=gate,
            ),
        )
    )
    signals: list[int] = []

    def signal_handler(request: httpx.Request) -> httpx.Response:
        signals.append(json.loads(request.content)["signal"])
        gate.set()
        return httpx.Response(200, json=_process())

    respx.post(SIGNAL_URL).mock(side_effect=signal_handler)

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        async with box.open_process("python") as proc:
            await proc.kill()
            waited = await proc.wait()

    assert signals == [9]
    assert waited.returncode == -9


@respx.mock
def test_sync_kill_from_another_thread_while_wait_is_reading(mock_env_clear: None) -> None:
    gate = threading.Event()
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(
            200,
            stream=_GatedSyncStream(
                before=_ndjson(_process()),
                after=_ndjson(_process(-9)),
                gate=gate,
            ),
        )
    )
    signals: list[int] = []

    def signal_handler(request: httpx.Request) -> httpx.Response:
        signals.append(json.loads(request.content)["signal"])
        gate.set()
        return httpx.Response(200, json=_process())

    respx.post(SIGNAL_URL).mock(side_effect=signal_handler)

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with box.open_process("python") as proc, ThreadPoolExecutor(max_workers=1) as pool:
            finished = pool.submit(proc.wait)
            proc.kill()
            waited = finished.result(timeout=5)

    assert signals == [9]
    assert waited.returncode == -9


# -- check, failures, and lifecycle recovery ----------------------------------


@pytest.mark.parametrize(("check", "raises"), [(True, True), (False, False)])
@respx.mock
async def test_async_open_process_check_applies_at_normal_block_exit(
    mock_env_clear: None, check: bool, raises: bool
) -> None:
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(
            200,
            content=_ndjson(_process(), _output("stderr", "boom\n"), _process(3)),
        )
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        if raises:
            with pytest.raises(subprocess.CalledProcessError) as exc_info:
                async with box.open_process("python", check=check, capture_output=True) as proc:
                    pass
            assert exc_info.value.returncode == 3
            assert exc_info.value.stderr == "boom\n"
        else:
            async with box.open_process("python", check=check, capture_output=True) as proc:
                pass
        assert proc.result.returncode == 3


@pytest.mark.parametrize(("check", "raises"), [(True, True), (False, False)])
@respx.mock
def test_sync_open_process_check_applies_at_normal_block_exit(
    mock_env_clear: None, check: bool, raises: bool
) -> None:
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(
            200,
            content=_ndjson(_process(), _output("stderr", "boom\n"), _process(3)),
        )
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        if raises:
            with pytest.raises(subprocess.CalledProcessError) as exc_info:
                with box.open_process("python", check=check, capture_output=True) as proc:
                    pass
            assert exc_info.value.returncode == 3
            assert exc_info.value.stderr == "boom\n"
        else:
            with box.open_process("python", check=check, capture_output=True) as proc:
                pass
        assert proc.result.returncode == 3


_PUMP_FAILURES: dict[str, tuple[list[object], type[Exception], str]] = {
    "sink-write-fails": (
        [_process(), _output("stdout", "out\n"), _process(0)],
        OSError,
        "sink write failed",
    ),
    "stream-error-record": (
        [_process(), {"stream": "error", "data": {"code": "failed", "message": "stream failed"}}],
        sandbox.SandboxStreamError,
        "stream failed",
    ),
}


@pytest.mark.parametrize("failure", list(_PUMP_FAILURES))
@pytest.mark.parametrize("body", ["idle", "wait"])
@respx.mock
async def test_async_pump_failure_surfaces_as_plain_exception(
    mock_env_clear: None, failure: str, body: str
) -> None:
    records, error_type, match = _PUMP_FAILURES[failure]
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    stream = _TrackingAsyncStream(_ndjson(*records))
    respx.post(COMMAND_URL).mock(return_value=httpx.Response(200, stream=stream))
    sink: TextIO = _FailingTextIO() if failure == "sink-write-fails" else io.StringIO()

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        with pytest.raises(error_type, match=match) as exc_info:
            async with box.open_process("python", stdout=sink) as proc:
                if body == "idle":
                    await anyio.sleep(5)
                else:
                    await proc.wait()

    assert type(exc_info.value) is error_type
    assert stream.closed


@pytest.mark.parametrize("failure", list(_PUMP_FAILURES))
@pytest.mark.parametrize("hit_at", ["next", "wait", "exit"])
@respx.mock
def test_sync_pump_failure_is_raised_where_it_is_hit(
    mock_env_clear: None, failure: str, hit_at: str
) -> None:
    records, error_type, match = _PUMP_FAILURES[failure]
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    stream = _TrackingSyncStream(_ndjson(*records))
    respx.post(COMMAND_URL).mock(return_value=httpx.Response(200, stream=stream))
    sink: TextIO = _FailingTextIO() if failure == "sink-write-fails" else io.StringIO()

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with pytest.raises(error_type, match=match) as exc_info:
            with box.open_process("python", stdout=sink) as proc:
                if hit_at == "next":
                    next(proc)
                elif hit_at == "wait":
                    proc.wait()

        assert type(exc_info.value) is error_type
        assert stream.closed
        with pytest.raises(error_type) as later:
            proc.wait()
        assert later.value is exc_info.value


@pytest.mark.parametrize(
    ("status_code", "code"), [(410, "sandbox_stopped"), (409, "sandbox_stopped")]
)
@respx.mock
async def test_async_lifecycle_error_in_block_is_not_replayed(
    mock_env_clear: None, status_code: int, code: str
) -> None:
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    command = respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(200, content=_ndjson(_process(), _process(0)))
    )
    resume = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        with pytest.raises(sandbox.SandboxApiError) as exc_info:
            async with box.open_process("python"):
                raise _stopped_error(status_code, code)

    assert exc_info.value.status_code == status_code
    assert command.call_count == 1
    assert not resume.called


@respx.mock
def test_sync_lifecycle_error_in_block_is_not_replayed(mock_env_clear: None) -> None:
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    command = respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(200, content=_ndjson(_process(), _process(0)))
    )
    resume = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with pytest.raises(sandbox.SandboxApiError):
            with box.open_process("python"):
                raise _stopped_error(410, "sandbox_stopped")

    assert command.call_count == 1
    assert not resume.called


@respx.mock
async def test_async_pre_start_lifecycle_error_replays_once_into_replacement(
    mock_env_clear: None,
) -> None:
    events: list[str] = []
    respx.post(CREATE_URL).mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
    )

    def old_command_handler(_request: httpx.Request) -> httpx.Response:
        events.append("old-command")
        return httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "session is stopped"}},
        )

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        side_effect=old_command_handler
    )

    def resume_handler(_request: httpx.Request) -> httpx.Response:
        events.append("resume")
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=resume_handler)

    def replacement_handler(_request: httpx.Request) -> httpx.Response:
        events.append("replacement-command")
        return httpx.Response(
            200,
            content=_ndjson(
                _process(session_id="sbx_new"),
                _output("stdout", "out\n"),
                _process(0, session_id="sbx_new"),
            ),
        )

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        side_effect=replacement_handler
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        async with box.open_process("python", capture_output=True) as proc:
            assert proc.session_id == "sbx_new"

    assert proc.session_id == "sbx_new"
    assert proc.result.stdout == "out\n"
    assert events == ["old-command", "resume", "replacement-command"]


@respx.mock
def test_sync_pre_start_lifecycle_error_replays_once_into_replacement(
    mock_env_clear: None,
) -> None:
    events: list[str] = []
    respx.post(CREATE_URL).mock(
        return_value=httpx.Response(200, json=_sandbox_response(session_id="sbx_old"))
    )

    def old_command_handler(_request: httpx.Request) -> httpx.Response:
        events.append("old-command")
        return httpx.Response(
            410,
            json={"error": {"code": "sandbox_stopped", "message": "session is stopped"}},
        )

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_old/cmd").mock(
        side_effect=old_command_handler
    )

    def resume_handler(_request: httpx.Request) -> httpx.Response:
        events.append("resume")
        return httpx.Response(200, json=_sandbox_response(session_id="sbx_new"))

    respx.get("https://sandbox.test/v2/sandboxes/preview").mock(side_effect=resume_handler)

    def replacement_handler(_request: httpx.Request) -> httpx.Response:
        events.append("replacement-command")
        return httpx.Response(
            200,
            content=_ndjson(
                _process(session_id="sbx_new"),
                _output("stdout", "out\n"),
                _process(0, session_id="sbx_new"),
            ),
        )

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        side_effect=replacement_handler
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with box.open_process("python", capture_output=True) as proc:
            assert proc.session_id == "sbx_new"

    assert proc.session_id == "sbx_new"
    assert proc.result.stdout == "out\n"
    assert events == ["old-command", "resume", "replacement-command"]


# -- Validation and single use -------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"capture_output": True, "stdout": subprocess.PIPE},
        {"capture_output": True, "stderr": subprocess.PIPE},
        {"stdout": subprocess.STDOUT},
        {"stdout": 42},
        {"stderr": 42},
        {"stdout": object()},
        {"stderr": io.BytesIO()},
    ],
)
@respx.mock
async def test_async_open_process_rejects_output_options_before_request(
    mock_env_clear: None, kwargs: dict[str, object]
) -> None:
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    command = respx.post(COMMAND_URL)

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        with pytest.raises((TypeError, ValueError)):
            box.open_process("python", **kwargs)  # type: ignore[arg-type]

    assert command.call_count == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"capture_output": True, "stdout": subprocess.PIPE},
        {"stdout": subprocess.STDOUT},
        {"stderr": io.BytesIO()},
    ],
)
@respx.mock
def test_sync_open_process_rejects_output_options_before_request(
    mock_env_clear: None, kwargs: dict[str, object]
) -> None:
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    command = respx.post(COMMAND_URL)

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with pytest.raises((TypeError, ValueError)):
            box.open_process("python", **kwargs)  # type: ignore[arg-type]

    assert command.call_count == 0


@respx.mock
async def test_async_open_process_operation_is_single_use_and_warns_when_unused(
    mock_env_clear: None,
) -> None:
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(200, content=_ndjson(_process(), _process(0)))
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        operation = box.open_process("python")
        async with operation:
            pass
        with pytest.raises(RuntimeError, match="only be used once"):
            async with operation:
                pass

        with pytest.warns(RuntimeWarning, match="never entered"):
            unused = box.open_process("python")
            del unused


@respx.mock
def test_sync_open_process_operation_is_single_use_and_warns_when_unused(
    mock_env_clear: None,
) -> None:
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(200, content=_ndjson(_process(), _process(0)))
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        operation = box.open_process("python")
        with operation:
            pass
        with pytest.raises(RuntimeError, match="only be used once"):
            with operation:
                pass

        with pytest.warns(RuntimeWarning, match="never entered"):
            unused = box.open_process("python")
            del unused


@respx.mock
async def test_async_outer_cancel_during_normal_exit_settles_the_handle(
    mock_env_clear: None,
) -> None:
    gate = anyio.Event()
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    respx.post(COMMAND_URL).mock(
        return_value=httpx.Response(
            200,
            stream=_GatedAsyncStream(
                before=_ndjson(_process()),
                after=_ndjson(_process(0)),
                gate=gate,
            ),
        )
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        with anyio.move_on_after(0.05) as scope:
            async with box.open_process("python") as proc:
                pass

        assert scope.cancelled_caught
        with anyio.fail_after(5), pytest.raises(RuntimeError, match="exited before"):
            await proc.wait()
        with pytest.raises(RuntimeError, match="has not completed"):
            _ = proc.result


@pytest.mark.skipif(not hasattr(signal, "pthread_kill"), reason="requires signal.pthread_kill")
@respx.mock
def test_sync_interrupt_while_draining_at_exit_closes_stream_and_stops_reading(
    mock_env_clear: None,
) -> None:
    gate = threading.Event()
    respx.post(CREATE_URL).mock(return_value=httpx.Response(200, json=_sandbox_response()))
    stream = _GatedSyncStream(
        before=_ndjson(_process()),
        after=_ndjson(_output("stdout", "late\n"), _process(0)),
        gate=gate,
    )
    respx.post(COMMAND_URL).mock(return_value=httpx.Response(200, stream=stream))
    sink = io.StringIO()
    main_thread = threading.get_ident()
    interrupt = threading.Timer(0.2, lambda: signal.pthread_kill(main_thread, signal.SIGINT))

    try:
        with session(service_options=_session_options()):
            box = sandbox_sync.create_sandbox(name="preview")
            with pytest.raises(KeyboardInterrupt):
                with box.open_process("python", stdout=sink) as proc:
                    interrupt.start()

            assert stream.closed
            gate.set()
            with pytest.raises(RuntimeError, match="exited before"):
                proc.wait()
            assert "late" not in sink.getvalue()
    finally:
        interrupt.cancel()
        gate.set()
