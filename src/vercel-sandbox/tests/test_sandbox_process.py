import io
import json
import signal
import subprocess
from collections.abc import AsyncIterator, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event, Lock

import anyio
import httpx
import pytest
import respx
from sandbox_fixtures import sandbox_service_options

from vercel import sandbox
from vercel._internal.core.options import ServiceOptions
from vercel.api import session
from vercel.sandbox import sync as sandbox_sync
from vercel.sandbox._internal.state import (
    SandboxRuntimeSessionState,
    SandboxState,
)


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


def _process_response(
    returncode: int | None = None,
    *,
    args: list[str] | None = None,
    command_id: str = "cmd_1",
    session_id: str = "sbx_1",
) -> dict[str, object]:
    return {
        "command": {
            "id": command_id,
            "name": "python",
            "args": args or [],
            "cwd": "/vercel/sandbox",
            "sessionId": session_id,
            "exitCode": returncode,
            "startedAt": 1,
        }
    }


def _logs_response() -> httpx.Response:
    records = [
        {"stream": "stdout", "data": "out-1\nout-2"},
        {"stream": "stderr", "data": "err\n"},
    ]
    return httpx.Response(200, text="".join(json.dumps(record) + "\n" for record in records))


def _completed_response(
    returncode: int = 0,
    *,
    args: list[str] | None = None,
    session_id: str = "sbx_1",
) -> httpx.Response:
    records = [
        _process_response(args=args, session_id=session_id),
        {"stream": "stdout", "data": "out\n"},
        {"stream": "stderr", "data": "err\n"},
        _process_response(returncode, args=args, session_id=session_id),
    ]
    return httpx.Response(200, text="".join(json.dumps(record) + "\n" for record in records))


def _interleaved_completed_response(returncode: int = 0) -> httpx.Response:
    records = [
        _process_response(),
        {"stream": "stdout", "data": "out-1\n"},
        {"stream": "stderr", "data": "err\n"},
        {"stream": "stdout", "data": "out-2\n"},
        _process_response(returncode),
    ]
    return httpx.Response(200, text="".join(json.dumps(record) + "\n" for record in records))


class _RecordingTextIO(io.StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.flush_count = 0

    def flush(self) -> None:
        self.flush_count += 1
        super().flush()


class _FailingTextIO(io.StringIO):
    def __init__(self, *, fail_on: str) -> None:
        super().__init__()
        self._fail_on = fail_on

    def write(self, value: str) -> int:
        if self._fail_on == "write":
            raise OSError("sink write failed")
        return super().write(value)

    def flush(self) -> None:
        if self._fail_on == "flush":
            raise OSError("sink flush failed")
        super().flush()


class _TrackingAsyncStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes | list[bytes]) -> None:
        self.chunks = content if isinstance(content, list) else [content]
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class _TrackingSyncStream(httpx.SyncByteStream):
    def __init__(self, content: bytes | list[bytes]) -> None:
        self.chunks = content if isinstance(content, list) else [content]
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield from self.chunks

    def close(self) -> None:
        self.closed = True


def _completed_body() -> bytes:
    records = [
        _process_response(),
        {"stream": "stdout", "data": "out\n"},
        _process_response(0),
    ]
    return "".join(json.dumps(record) + "\n" for record in records).encode()


def _chunked_ndjson(*records: object) -> list[bytes]:
    content = "\r\n\r\n".join(json.dumps(record, ensure_ascii=False) for record in records).encode()
    return [content[offset : offset + 1] for offset in range(len(content))]


def _session_options() -> list[ServiceOptions]:
    return sandbox_service_options(
        team_id="team_1",
        project_id="prj_1",
    )


def test_public_process_exports() -> None:
    for name in (
        "CompletedProcess",
        "Process",
        "ProcessStatus",
        "SandboxCredentials",
        "SandboxCredentialsFactory",
        "TextReader",
    ):
        assert name in sandbox.__all__
    for name in (
        "CompletedProcess",
        "ProcessStatus",
        "SandboxCredentials",
        "SyncProcess",
        "SyncSandboxCredentialsFactory",
        "SyncTextReader",
    ):
        assert name in sandbox_sync.__all__


@respx.mock
async def test_async_process_readers_wait_and_signals(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_process_response())
    )
    get_process = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1").mock(
        side_effect=[
            httpx.Response(200, json=_process_response()),
            httpx.Response(200, json=_process_response(7)),
        ]
    )
    logs = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs").mock(
        side_effect=lambda _request: _logs_response()
    )
    signals: list[int] = []

    def signal_handler(request: httpx.Request) -> httpx.Response:
        signals.append(json.loads(request.content)["signal"])
        return httpx.Response(200, json=_process_response())

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/kill").mock(
        side_effect=signal_handler
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        process = await box.create_process("python")
        assert process.name == "python"
        assert process.args == []
        assert process.cwd == "/vercel/sandbox"
        assert process.session_id == "sbx_1"
        assert process.started_at == 1
        assert process.status is sandbox.ProcessStatus.RUNNING
        assert process.stdin is None
        assert process.returncode is None
        assert process.stdout is not None
        assert process.stderr is not None
        assert await process.stdout.readline() == "out-1\n"
        assert await process.stdout.read() == "out-2"
        assert await process.stderr.read() == "err\n"
        assert await process.refresh() is process
        assert await process.wait() == 7
        assert process.returncode == 7
        assert process.status is sandbox.ProcessStatus.EXITED
        await process.terminate()
        await process.kill()
        await process.send_signal(signal.SIGINT)

    assert get_process.calls[0].request.url.params["wait"] == "false"
    assert get_process.calls[1].request.url.params["wait"] == "true"
    assert logs.call_count == 1
    assert all(call.request.headers["connection"] == "close" for call in logs.calls)
    assert signals == [signal.SIGTERM, signal.SIGKILL, signal.SIGINT]


@respx.mock
def test_sync_process_readers_wait_and_signals(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_process_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1").mock(
        return_value=httpx.Response(200, json=_process_response(0))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs").mock(
        side_effect=lambda _request: _logs_response()
    )
    signals: list[int] = []

    def signal_handler(request: httpx.Request) -> httpx.Response:
        signals.append(json.loads(request.content)["signal"])
        return httpx.Response(200, json=_process_response())

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/kill").mock(
        side_effect=signal_handler
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        process = box.create_process("python")
        assert process.communicate() == ("out-1\nout-2", "err\n")
        assert process.returncode == 0
        process.terminate()
        process.kill()

    assert signals == [signal.SIGTERM, signal.SIGKILL]


@respx.mock
async def test_async_create_process_merges_stderr_into_stdout_reader(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_process_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1").mock(
        return_value=httpx.Response(200, json=_process_response(0))
    )
    logs = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs").mock(
        side_effect=lambda _request: _logs_response()
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        process = await box.create_process("python", stderr=subprocess.STDOUT)
        assert process.stderr is None
        assert process.stdout is not None
        assert await process.communicate() == ("out-1\nout-2err\n", None)
        assert process.returncode == 0

    assert logs.call_count == 1


@respx.mock
async def test_async_create_process_devnull_drops_reader(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_process_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1").mock(
        return_value=httpx.Response(200, json=_process_response(0))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs").mock(
        side_effect=lambda _request: _logs_response()
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        process = await box.create_process("python", stderr=subprocess.DEVNULL)
        assert process.stderr is None
        assert await process.communicate() == ("out-1\nout-2", None)


@pytest.mark.parametrize("stderr", [subprocess.DEVNULL, subprocess.STDOUT])
@respx.mock
async def test_async_create_process_with_no_readers_never_requests_logs(
    mock_env_clear: None, stderr: int
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_process_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1").mock(
        return_value=httpx.Response(200, json=_process_response(0))
    )
    logs = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs")

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        process = await box.create_process("python", stdout=subprocess.DEVNULL, stderr=stderr)
        assert process.stdout is None
        assert process.stderr is None
        assert await process.communicate() == (None, None)
        assert process.returncode == 0

    assert logs.call_count == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"stdout": subprocess.STDOUT},
        {"stdout": None},
        {"stderr": None},
        {"stdout": 42},
        {"stderr": 42},
        {"stdout": io.StringIO()},
        {"stderr": io.BytesIO()},
    ],
)
@respx.mock
async def test_create_process_rejects_output_options_before_request(
    mock_env_clear: None, kwargs: dict[str, object]
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    create = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd")

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises((TypeError, ValueError)):
            await box.create_process("python", **kwargs)  # type: ignore[arg-type]

    assert create.call_count == 0


@respx.mock
def test_sync_create_process_merges_stderr_into_stdout_reader(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_process_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1").mock(
        return_value=httpx.Response(200, json=_process_response(0))
    )
    logs = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs").mock(
        side_effect=lambda _request: _logs_response()
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        process = box.create_process("python", stderr=subprocess.STDOUT)
        assert process.stderr is None
        assert process.stdout is not None
        assert process.communicate() == ("out-1\nout-2err\n", None)
        assert process.returncode == 0

    assert logs.call_count == 1


@pytest.mark.parametrize("stderr", [subprocess.DEVNULL, subprocess.STDOUT])
@respx.mock
def test_sync_create_process_with_no_readers_never_requests_logs(
    mock_env_clear: None, stderr: int
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_process_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1").mock(
        return_value=httpx.Response(200, json=_process_response(0))
    )
    logs = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs")

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        process = box.create_process("python", stdout=subprocess.DEVNULL, stderr=stderr)
        assert process.stdout is None
        assert process.stderr is None
        assert process.communicate() == (None, None)
        assert process.returncode == 0

    assert logs.call_count == 0


@respx.mock
def test_sync_create_process_rejects_output_options_before_request(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    create = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd")

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(ValueError, match="STDOUT is only supported for stderr"):
            box.create_process("python", stdout=subprocess.STDOUT)

    assert create.call_count == 0


@respx.mock
async def test_run_process_routes_output_checks_and_uses_one_request(
    mock_env_clear: None, capsys: pytest.CaptureFixture[str]
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    run = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        side_effect=[
            _completed_response(args=["-c", "print('out')"]),
            _completed_response(),
            _interleaved_completed_response(),
            _completed_response(9),
            _completed_response(9),
        ]
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        result = await box.run_process("python", ("-c", "print('out')"))
        assert isinstance(result, sandbox.CompletedProcess)
        assert result.args == ("python", "-c", "print('out')")
        assert result.returncode == 0
        assert result.stdout is None
        assert result.stderr is None
        assert capsys.readouterr() == ("out\n", "err\n")

        captured = await box.run_process("python", capture_output=True)
        assert captured.stdout == "out\n"
        assert captured.stderr == "err\n"

        merged = await box.run_process("python", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert merged.stdout == "out-1\nerr\nout-2\n"
        assert merged.stderr is None

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            await box.run_process("python", check=True)
        assert exc_info.value.returncode == 9
        assert exc_info.value.stdout is None
        assert exc_info.value.stderr is None

        with pytest.raises(subprocess.CalledProcessError) as captured_error:
            await box.run_process("python", check=True, capture_output=True)
        assert captured_error.value.stdout == "out\n"
        assert captured_error.value.stderr == "err\n"

    assert run.call_count == 5
    assert all(call.request.url.params["wait"] == "true" for call in run.calls)
    assert all(call.request.url.params["logs"] == "true" for call in run.calls)


@respx.mock
async def test_async_run_process_explicit_and_discarded_destinations(
    mock_env_clear: None, capsys: pytest.CaptureFixture[str]
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    run = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        side_effect=[
            _interleaved_completed_response(),
            _interleaved_completed_response(),
            _interleaved_completed_response(),
            _completed_response(),
        ]
    )
    sink = _RecordingTextIO()

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        result = await box.run_process("python", stdout=sink, stderr=subprocess.STDOUT)
        assert result.stdout is None
        assert result.stderr is None
        assert sink.getvalue() == "out-1\nerr\nout-2\n"
        assert sink.flush_count == 3

        discarded = await box.run_process(
            "python", stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )
        assert discarded.stdout is None
        assert discarded.stderr is None

        inherited = await box.run_process("python", stderr=subprocess.STDOUT)
        assert inherited.stdout is None
        assert inherited.stderr is None
        assert capsys.readouterr() == ("out-1\nerr\nout-2\n", "")

        stdout_only = await box.run_process(
            "python", stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        assert stdout_only.stdout == "out\n"
        assert stdout_only.stderr is None

    assert run.call_count == 4


@respx.mock
def test_sync_run_process_routes_and_captures(
    mock_env_clear: None, capsys: pytest.CaptureFixture[str]
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    run = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        side_effect=[_completed_response(), _interleaved_completed_response()]
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        inherited = box.run_process("python")
        assert inherited.stdout is None
        assert inherited.stderr is None
        assert capsys.readouterr() == ("out\n", "err\n")

        captured = box.run_process("python", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert captured.stdout == "out-1\nerr\nout-2\n"
        assert captured.stderr is None

    assert run.call_count == 2


@respx.mock
async def test_async_run_process_reads_chunked_ndjson(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    stream = _TrackingAsyncStream(
        _chunked_ndjson(
            _process_response(),
            {"stream": "stdout", "data": "café\n"},
            {"stream": "stderr", "data": "雪\n"},
            _process_response(0),
        )
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        result = await box.run_process("python", capture_output=True)

    assert result.stdout == "café\n"
    assert result.stderr == "雪\n"
    assert stream.closed


@respx.mock
async def test_async_run_process_replays_pre_stream_stopped_session_error(
    mock_env_clear: None,
) -> None:
    events: list[str] = []
    respx.post("https://sandbox.test/v2/sandboxes").mock(
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

    def replacement_command_handler(_request: httpx.Request) -> httpx.Response:
        events.append("replacement-command")
        return _completed_response(session_id="sbx_new")

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        side_effect=replacement_command_handler
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        result = await box.run_process("python", capture_output=True)

    assert result.session_id == "sbx_new"
    assert result.stdout == "out\n"
    assert events == ["old-command", "resume", "replacement-command"]


@respx.mock
def test_sync_run_process_replays_pre_stream_stopped_session_error(
    mock_env_clear: None,
) -> None:
    events: list[str] = []
    respx.post("https://sandbox.test/v2/sandboxes").mock(
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

    def replacement_command_handler(_request: httpx.Request) -> httpx.Response:
        events.append("replacement-command")
        return _completed_response(session_id="sbx_new")

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_new/cmd").mock(
        side_effect=replacement_command_handler
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        result = box.run_process("python", capture_output=True)

    assert result.session_id == "sbx_new"
    assert result.stdout == "out\n"
    assert events == ["old-command", "resume", "replacement-command"]


@respx.mock
def test_sync_run_process_reads_chunked_ndjson(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    stream = _TrackingSyncStream(
        _chunked_ndjson(
            _process_response(),
            {"stream": "stdout", "data": "café\n"},
            {"stream": "stderr", "data": "雪\n"},
            _process_response(0),
        )
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        result = box.run_process("python", capture_output=True)

    assert result.stdout == "café\n"
    assert result.stderr == "雪\n"
    assert stream.closed


@respx.mock
async def test_async_process_readers_read_chunked_ndjson(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_process_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1").mock(
        return_value=httpx.Response(200, json=_process_response(0))
    )
    stream = _TrackingAsyncStream(
        _chunked_ndjson(
            {"stream": "stdout", "data": "café\n"},
            {"stream": "stderr", "data": "雪\n"},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        process = await box.create_process("python")
        output = await process.communicate()

    assert output == ("café\n", "雪\n")
    assert stream.closed


@respx.mock
def test_sync_process_readers_read_chunked_ndjson(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_process_response())
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1").mock(
        return_value=httpx.Response(200, json=_process_response(0))
    )
    stream = _TrackingSyncStream(
        _chunked_ndjson(
            {"stream": "stdout", "data": "café\n"},
            {"stream": "stderr", "data": "雪\n"},
        )
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        process = box.create_process("python")
        output = process.communicate()

    assert output == ("café\n", "雪\n")
    assert stream.closed


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
async def test_run_process_rejects_output_options_before_request(
    mock_env_clear: None, kwargs: dict[str, object]
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    run = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd")

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises((TypeError, ValueError)):
            await box.run_process("python", **kwargs)  # type: ignore[arg-type]

    assert run.call_count == 0


@respx.mock
async def test_async_run_process_closes_response_when_sink_write_fails(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    stream = _TrackingAsyncStream(_completed_body())
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(OSError, match="sink write failed"):
            await box.run_process("python", stdout=_FailingTextIO(fail_on="write"))

    assert stream.closed


@respx.mock
def test_sync_run_process_closes_response_when_sink_flush_fails(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    stream = _TrackingSyncStream(_completed_body())
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(OSError, match="sink flush failed"):
            box.run_process("python", stdout=_FailingTextIO(fail_on="flush"))

    assert stream.closed


@pytest.mark.parametrize(
    ("records", "error", "match"),
    [
        (["not-json"], sandbox.SandboxResponseError, "malformed NDJSON"),
        ([_process_response()], sandbox.SandboxResponseError, "missing final"),
        (
            [_process_response(), _process_response(0, command_id="cmd_other")],
            sandbox.SandboxResponseError,
            "different final process identity",
        ),
        (
            [
                _process_response(),
                {"stream": "error", "data": {"code": "failed", "message": "stream failed"}},
            ],
            sandbox.SandboxStreamError,
            "stream failed",
        ),
    ],
)
@respx.mock
async def test_run_process_rejects_invalid_streams(
    mock_env_clear: None,
    records: list[object],
    error: type[Exception],
    match: str,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(
            200,
            text="".join(
                (record if isinstance(record, str) else json.dumps(record)) + "\n"
                for record in records
            ),
        )
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(error, match=match):
            await box.run_process("python")


@respx.mock
async def test_async_run_process_does_not_replay_lifecycle_stream_errors(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(
            200,
            text="".join(
                json.dumps(record) + "\n"
                for record in (
                    _process_response(),
                    {
                        "stream": "error",
                        "data": {"code": "sandbox_stopped", "message": "stream stopped"},
                    },
                )
            ),
        )
    )
    resume_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(sandbox.SandboxStreamError, match="stream stopped") as exc_info:
            await box.run_process("python")

    assert exc_info.value.code == "sandbox_stopped"
    assert not resume_route.called


@respx.mock
def test_sync_run_process_does_not_replay_lifecycle_stream_errors(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(
            200,
            text="".join(
                json.dumps(record) + "\n"
                for record in (
                    _process_response(),
                    {
                        "stream": "error",
                        "data": {"code": "sandbox_stopped", "message": "stream stopped"},
                    },
                )
            ),
        )
    )
    resume_route = respx.get("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(sandbox.SandboxStreamError, match="stream stopped") as exc_info:
            box.run_process("python")

    assert exc_info.value.code == "sandbox_stopped"
    assert not resume_route.called


def _stopped_error() -> sandbox.SandboxApiError:
    data = {"error": {"code": "sandbox_stopped", "message": "session stopped"}}
    return sandbox.SandboxApiError(httpx.Response(409), "session stopped", data=data)


def _transition_error() -> sandbox.SandboxApiError:
    data = {"error": {"code": "sandbox_stopping", "message": "sandbox stopping"}}
    return sandbox.SandboxApiError(httpx.Response(409), "sandbox stopping", data=data)


class _AsyncCoordinatedRecoveryService:
    def __init__(self, *, resume_error: BaseException | None = None) -> None:
        self.allow_resume = anyio.Event()
        self.resume_started = anyio.Event()
        self.resume_finished = anyio.Event()
        self.second_failure = anyio.Event()
        self.operation_count = 0
        self.resume_count = 0
        self.resume_error = resume_error

    async def query_processes(self, *, session_id: str) -> list[object]:
        self.operation_count += 1
        if session_id == "sbx_old":
            if self.operation_count >= 2:
                self.second_failure.set()
            raise _stopped_error()
        return []

    async def resume_sandbox(self, **_kwargs: object) -> SandboxState:
        self.resume_count += 1
        self.resume_started.set()
        await self.allow_resume.wait()
        if self.resume_error is not None:
            raise self.resume_error
        result = SandboxState(
            name="preview",
            current_session_id="sbx_new",
            current_session=SandboxRuntimeSessionState(
                id="sbx_new", status=sandbox.SandboxStatus.RUNNING
            ),
        )
        self.resume_finished.set()
        return result


def _async_coordinated_box(
    service: _AsyncCoordinatedRecoveryService,
) -> sandbox.Sandbox:
    return sandbox.Sandbox(
        payload=SandboxState(
            name="preview",
            current_session_id="sbx_old",
            current_session=SandboxRuntimeSessionState(
                id="sbx_old", status=sandbox.SandboxStatus.STOPPED
            ),
        ),
        service=service,  # type: ignore[arg-type]
    )


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_recovery_shares_success(anyio_backend: str) -> None:
    service = _AsyncCoordinatedRecoveryService()
    box = _async_coordinated_box(service)
    results: list[list[sandbox.Process]] = []

    async def query() -> None:
        results.append(await box.query_processes())

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(query)
        await service.resume_started.wait()
        task_group.start_soon(query)
        await service.second_failure.wait()
        service.allow_resume.set()

    assert results == [[], []]
    assert service.resume_count == 1
    assert box.current_session_id == "sbx_new"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_recovery_waiter_re_elects_after_owner_cancellation(
    anyio_backend: str,
) -> None:
    service = _AsyncCoordinatedRecoveryService()
    box = _async_coordinated_box(service)
    owner_scope = anyio.CancelScope()
    owner_done = anyio.Event()
    waiter_result: list[sandbox.Process] | None = None

    async def own_recovery() -> None:
        with owner_scope:
            try:
                await box.query_processes()
            finally:
                owner_done.set()

    async def wait_for_recovery() -> None:
        nonlocal waiter_result
        waiter_result = await box.query_processes()

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(own_recovery)
        await service.resume_started.wait()
        task_group.start_soon(wait_for_recovery)
        await service.second_failure.wait()
        owner_scope.cancel()
        await owner_done.wait()
        service.allow_resume.set()

    assert waiter_result == []
    assert service.resume_count == 2
    assert box.current_session_id == "sbx_new"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_recovery_is_abandoned_when_all_callers_cancel(
    anyio_backend: str,
) -> None:
    service = _AsyncCoordinatedRecoveryService()
    box = _async_coordinated_box(service)

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(box.query_processes)
        await service.resume_started.wait()
        task_group.start_soon(box.query_processes)
        await service.second_failure.wait()
        task_group.cancel_scope.cancel()

    service.allow_resume.set()
    await anyio.sleep(0)
    assert not service.resume_finished.is_set()
    assert service.resume_count == 1
    assert box.current_session_id == "sbx_old"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_recovery_shares_failure_and_clears_slot_for_retry(
    anyio_backend: str,
) -> None:
    resume_error = RuntimeError("resume failed")
    service = _AsyncCoordinatedRecoveryService(resume_error=resume_error)
    box = _async_coordinated_box(service)
    results: list[BaseException] = []

    async def query() -> None:
        try:
            await box.query_processes()
        except BaseException as error:
            results.append(error)

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(query)
        await service.resume_started.wait()
        task_group.start_soon(query)
        await service.second_failure.wait()
        service.allow_resume.set()

    assert results == [resume_error, resume_error]
    assert service.resume_count == 1
    service.resume_error = None
    assert await box.query_processes() == []
    assert service.resume_count == 2


class _DelayedLifecycleFailureService:
    def __init__(self) -> None:
        self.arrived = (anyio.Event(), anyio.Event())
        self.release = (anyio.Event(), anyio.Event())
        self.old_operation_count = 0
        self.resume_count = 0

    async def query_processes(self, *, session_id: str) -> list[object]:
        if session_id != "sbx_old":
            return []
        index = self.old_operation_count
        self.old_operation_count += 1
        self.arrived[index].set()
        await self.release[index].wait()
        raise _stopped_error()

    async def resume_sandbox(self, **_kwargs: object) -> SandboxState:
        self.resume_count += 1
        return SandboxState(
            name="preview",
            current_session_id="sbx_new",
            current_session=SandboxRuntimeSessionState(
                id="sbx_new", status=sandbox.SandboxStatus.RUNNING
            ),
        )


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_delayed_async_lifecycle_failure_may_resume_again_after_slot_clears(
    anyio_backend: str,
) -> None:
    service = _DelayedLifecycleFailureService()
    box = sandbox.Sandbox(
        payload=SandboxState(
            name="preview",
            current_session_id="sbx_old",
            current_session=SandboxRuntimeSessionState(
                id="sbx_old", status=sandbox.SandboxStatus.STOPPED
            ),
        ),
        service=service,  # type: ignore[arg-type]
    )

    results: list[list[sandbox.Process]] = []

    async def query() -> None:
        results.append(await box.query_processes())

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(query)
        task_group.start_soon(query)
        await service.arrived[0].wait()
        await service.arrived[1].wait()
        service.release[0].set()
        while len(results) < 1:
            await anyio.sleep(0)
        service.release[1].set()

    assert results == [[], []]
    assert service.resume_count == 2
    assert box.current_session_id == "sbx_new"


class _SyncCoordinatedRecoveryService:
    def __init__(
        self,
        *,
        interrupt_first_resume: bool = False,
        first_resume_error: Exception | None = None,
    ) -> None:
        self.allow_resume = Event()
        self.resume_returning = Event()
        self.resume_started = Event()
        self.second_failure = Event()
        self._lock = Lock()
        self._interrupt_first_resume = interrupt_first_resume
        self.first_resume_error = first_resume_error
        self.operation_count = 0
        self.resume_count = 0

    async def query_processes(self, *, session_id: str) -> list[object]:
        with self._lock:
            self.operation_count += 1
            operation_count = self.operation_count
        if session_id == "sbx_old":
            if operation_count >= 2:
                self.second_failure.set()
            raise _stopped_error()
        return []

    async def resume_sandbox(self, **_kwargs: object) -> SandboxState:
        with self._lock:
            self.resume_count += 1
            resume_count = self.resume_count
        if resume_count == 1:
            self.resume_started.set()
            assert self.allow_resume.wait(timeout=5)
            if self._interrupt_first_resume:
                raise KeyboardInterrupt
            if self.first_resume_error is not None:
                raise self.first_resume_error
        self.resume_returning.set()
        return SandboxState(
            name="preview",
            current_session_id="sbx_new",
            current_session=SandboxRuntimeSessionState(
                id="sbx_new", status=sandbox.SandboxStatus.RUNNING
            ),
        )


def _sync_coordinated_box(
    service: _SyncCoordinatedRecoveryService,
) -> sandbox_sync.SyncSandbox:
    return sandbox_sync.SyncSandbox(
        payload=SandboxState(
            name="preview",
            current_session_id="sbx_old",
            current_session=SandboxRuntimeSessionState(
                id="sbx_old", status=sandbox.SandboxStatus.STOPPED
            ),
        ),
        service=service,  # type: ignore[arg-type]
    )


@pytest.mark.parametrize("interrupt_initiator", [False, True])
def test_sync_recovery_shares_or_re_elects_after_initiator_interruption(
    interrupt_initiator: bool,
) -> None:
    service = _SyncCoordinatedRecoveryService(interrupt_first_resume=interrupt_initiator)
    box = _sync_coordinated_box(service)

    with ThreadPoolExecutor(max_workers=2) as executor:
        initiating = executor.submit(box.query_processes)
        assert service.resume_started.wait(timeout=5)
        waiting = executor.submit(box.query_processes)
        assert service.second_failure.wait(timeout=5)
        service.allow_resume.set()
        if interrupt_initiator:
            with pytest.raises(KeyboardInterrupt):
                initiating.result(timeout=5)
        else:
            assert initiating.result(timeout=5) == []
        assert waiting.result(timeout=5) == []

    assert service.resume_count == (2 if interrupt_initiator else 1)
    assert box.current_session_id == "sbx_new"


def test_sync_recovery_shares_failure_and_clears_slot_for_retry() -> None:
    resume_error = RuntimeError("resume failed")
    service = _SyncCoordinatedRecoveryService(first_resume_error=resume_error)
    box = _sync_coordinated_box(service)

    with ThreadPoolExecutor(max_workers=2) as executor:
        initiating = executor.submit(box.query_processes)
        assert service.resume_started.wait(timeout=5)
        waiting = executor.submit(box.query_processes)
        assert service.second_failure.wait(timeout=5)
        service.allow_resume.set()
        for participant in (initiating, waiting):
            with pytest.raises(RuntimeError) as exc_info:
                participant.result(timeout=5)
            assert exc_info.value is resume_error

    assert service.resume_count == 1
    service.first_resume_error = None
    assert box.query_processes() == []
    assert service.resume_count == 2


class _SyncSupersededMutationService:
    def __init__(self) -> None:
        self.allow_mutation = Event()
        self.mutation_started = Event()

    async def extend_runtime_session_timeout(
        self, *, session_id: str, duration: timedelta
    ) -> SandboxRuntimeSessionState:
        assert session_id == "sbx_old"
        assert duration == timedelta(seconds=2)
        self.mutation_started.set()
        assert self.allow_mutation.wait(timeout=5)
        return SandboxRuntimeSessionState(id="sbx_old", status=sandbox.SandboxStatus.RUNNING)

    async def query_processes(self, *, session_id: str) -> list[object]:
        if session_id == "sbx_old":
            raise _stopped_error()
        assert session_id == "sbx_new"
        return []

    async def resume_sandbox(self, **_kwargs: object) -> SandboxState:
        return SandboxState(
            name="preview",
            current_session_id="sbx_new",
            current_session=SandboxRuntimeSessionState(
                id="sbx_new", status=sandbox.SandboxStatus.RUNNING
            ),
        )


def test_sync_mutation_reply_superseded_by_recovery_keeps_current_session() -> None:
    service = _SyncSupersededMutationService()
    box = sandbox_sync.SyncSandbox(
        payload=SandboxState(
            name="preview",
            current_session_id="sbx_old",
            current_session=SandboxRuntimeSessionState(
                id="sbx_old", status=sandbox.SandboxStatus.RUNNING
            ),
        ),
        service=service,  # type: ignore[arg-type]
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        mutation = executor.submit(box.extend_execution_time_limit, 2)
        assert service.mutation_started.wait(timeout=5)
        assert box.query_processes() == []
        service.allow_mutation.set()
        result = mutation.result(timeout=5)

    assert result.id == "sbx_new"
    assert box.current_session_id == "sbx_new"


class _AsyncTransitionCoordinationService:
    def __init__(self) -> None:
        self.allow_resume = anyio.Event()
        self.resume_started = anyio.Event()
        self.second_poll_arrived = anyio.Event()
        self.second_poll_returning = anyio.Event()
        self.operation_count = 0
        self.poll_count = 0
        self.resume_count = 0

    async def query_processes(self, *, session_id: str) -> list[object]:
        self.operation_count += 1
        if session_id == "sbx_old":
            raise _transition_error()
        return []

    async def get_runtime_session(self, *, session_id: str) -> SandboxRuntimeSessionState:
        assert session_id == "sbx_old"
        self.poll_count += 1
        poll_number = self.poll_count
        if poll_number == 1:
            await self.second_poll_arrived.wait()
        else:
            self.second_poll_arrived.set()
            await self.resume_started.wait()
            self.second_poll_returning.set()
        return SandboxRuntimeSessionState(id="sbx_old", status=sandbox.SandboxStatus.STOPPED)

    async def resume_sandbox(self, **_kwargs: object) -> SandboxState:
        self.resume_count += 1
        self.resume_started.set()
        await self.allow_resume.wait()
        return SandboxState(
            name="preview",
            current_session_id="sbx_new",
            current_session=SandboxRuntimeSessionState(
                id="sbx_new", status=sandbox.SandboxStatus.RUNNING
            ),
        )


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_async_transition_callers_poll_independently_and_share_resume(
    anyio_backend: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def no_delay(_delay: float) -> None:
        return None

    monkeypatch.setattr(anyio, "sleep", no_delay)
    service = _AsyncTransitionCoordinationService()
    box = sandbox.Sandbox(
        payload=SandboxState(
            name="preview",
            current_session_id="sbx_old",
            current_session=SandboxRuntimeSessionState(
                id="sbx_old", status=sandbox.SandboxStatus.STOPPING
            ),
        ),
        service=service,  # type: ignore[arg-type]
    )

    results: list[list[sandbox.Process]] = []

    async def query() -> None:
        results.append(await box.query_processes())

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(query)
        task_group.start_soon(query)
        await service.second_poll_returning.wait()
        service.allow_resume.set()

    assert results == [[], []]
    assert service.poll_count == 2
    assert service.resume_count == 1
    assert service.operation_count == 4


class _SyncTransitionCoordinationService:
    def __init__(self) -> None:
        self.allow_resume = Event()
        self.resume_started = Event()
        self.second_poll_arrived = Event()
        self.second_poll_returning = Event()
        self._lock = Lock()
        self.operation_count = 0
        self.poll_count = 0
        self.resume_count = 0

    async def query_processes(self, *, session_id: str) -> list[object]:
        with self._lock:
            self.operation_count += 1
        if session_id == "sbx_old":
            raise _transition_error()
        return []

    async def get_runtime_session(self, *, session_id: str) -> SandboxRuntimeSessionState:
        assert session_id == "sbx_old"
        with self._lock:
            self.poll_count += 1
            poll_number = self.poll_count
        if poll_number == 1:
            assert self.second_poll_arrived.wait(timeout=5)
        else:
            self.second_poll_arrived.set()
            assert self.resume_started.wait(timeout=5)
            self.second_poll_returning.set()
        return SandboxRuntimeSessionState(id="sbx_old", status=sandbox.SandboxStatus.STOPPED)

    async def resume_sandbox(self, **_kwargs: object) -> SandboxState:
        with self._lock:
            self.resume_count += 1
        self.resume_started.set()
        assert self.allow_resume.wait(timeout=5)
        return SandboxState(
            name="preview",
            current_session_id="sbx_new",
            current_session=SandboxRuntimeSessionState(
                id="sbx_new", status=sandbox.SandboxStatus.RUNNING
            ),
        )


def test_sync_transition_callers_poll_independently_and_share_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("vercel.sandbox._internal.sync_runtime.time.sleep", lambda _delay: None)
    service = _SyncTransitionCoordinationService()
    box = sandbox_sync.SyncSandbox(
        payload=SandboxState(
            name="preview",
            current_session_id="sbx_old",
            current_session=SandboxRuntimeSessionState(
                id="sbx_old", status=sandbox.SandboxStatus.STOPPING
            ),
        ),
        service=service,  # type: ignore[arg-type]
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(box.query_processes)
        second = executor.submit(box.query_processes)
        assert service.second_poll_returning.wait(timeout=5)
        service.allow_resume.set()
        assert first.result(timeout=5) == []
        assert second.result(timeout=5) == []

    assert service.poll_count == 2
    assert service.resume_count == 1
    assert service.operation_count == 4


class _SyncPollingExitService:
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.operation_count = 0
        self.poll_count = 0
        self.resume_count = 0

    async def query_processes(self, *, session_id: str) -> list[object]:
        self.operation_count += 1
        raise _transition_error()

    async def get_runtime_session(self, *, session_id: str) -> SandboxRuntimeSessionState:
        self.poll_count += 1
        raise self.error

    async def resume_sandbox(self, **_kwargs: object) -> SandboxState:
        self.resume_count += 1
        raise AssertionError("poll exit must not resume")


@pytest.mark.parametrize("poll_error", [RuntimeError("poll failed"), KeyboardInterrupt()])
def test_sync_transition_poll_failure_or_interruption_exits_without_resume(
    monkeypatch: pytest.MonkeyPatch,
    poll_error: BaseException,
) -> None:
    monkeypatch.setattr("vercel.sandbox._internal.sync_runtime.time.sleep", lambda _delay: None)
    service = _SyncPollingExitService(poll_error)
    box = sandbox_sync.SyncSandbox(
        payload=SandboxState(
            name="preview",
            current_session_id="sbx_old",
            current_session=SandboxRuntimeSessionState(
                id="sbx_old", status=sandbox.SandboxStatus.STOPPING
            ),
        ),
        service=service,  # type: ignore[arg-type]
    )

    with pytest.raises(type(poll_error)) as exc_info:
        box.query_processes()

    assert exc_info.value is poll_error
    assert service.operation_count == 1
    assert service.poll_count == 1
    assert service.resume_count == 0
