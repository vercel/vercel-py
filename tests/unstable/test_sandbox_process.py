import io
import json
import signal
import subprocess
from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
import respx

from vercel import unstable as vercel
from vercel._internal.unstable.sandbox.options import SandboxCredentials
from vercel.unstable import sandbox
from vercel.unstable.sandbox import SandboxServiceOptions, sync as sandbox_sync


def _sandbox_response() -> dict[str, object]:
    return {
        "sandbox": {"name": "preview", "currentSessionId": "sbx_1", "status": "running"},
        "session": {
            "id": "sbx_1",
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
) -> dict[str, object]:
    return {
        "command": {
            "id": command_id,
            "name": "python",
            "args": args or [],
            "cwd": "/vercel/sandbox",
            "sessionId": "sbx_1",
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


def _completed_response(returncode: int = 0, *, args: list[str] | None = None) -> httpx.Response:
    records = [
        _process_response(args=args),
        {"stream": "stdout", "data": "out\n"},
        {"stream": "stderr", "data": "err\n"},
        _process_response(returncode, args=args),
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
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self.content

    async def aclose(self) -> None:
        self.closed = True


class _TrackingSyncStream(httpx.SyncByteStream):
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield self.content

    def close(self) -> None:
        self.closed = True


def _completed_body() -> bytes:
    records = [
        _process_response(),
        {"stream": "stdout", "data": "out\n"},
        _process_response(0),
    ]
    return "".join(json.dumps(record) + "\n" for record in records).encode()


def _session_options() -> list[SandboxServiceOptions]:
    async def credentials_factory() -> SandboxCredentials:
        return SandboxCredentials(token="token", team_id="team_1", project_id="prj_1")

    return [
        SandboxServiceOptions(
            base_url="https://sandbox.test",
            credentials_factory=credentials_factory,
        )
    ]


def test_public_process_exports() -> None:
    for name in (
        "CompletedProcess",
        "Process",
        "ProcessStatus",
        "TextReader",
        "ProcessLog",
        "ProcessLogStream",
    ):
        assert name in sandbox.__all__
    for name in (
        "CompletedProcess",
        "ProcessStatus",
        "SyncProcess",
        "SyncTextReader",
        "ProcessLog",
        "ProcessLogStream",
    ):
        assert name in sandbox_sync.__all__


@respx.mock
async def test_async_process_readers_logs_wait_and_signals(mock_env_clear: None) -> None:
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

    async with vercel.session(service_options=_session_options()):
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
        assert await process.stdout.readline() == "out-1\n"
        assert await process.stdout.read() == "out-2"
        assert await process.stderr.read() == "err\n"
        assert [(event.stream, event.data) async for event in process.logs()] == [
            ("stdout", "out-1\nout-2"),
            ("stderr", "err\n"),
        ]
        assert [(event.stream, event.data) async for event in process.logs()] == [
            ("stdout", "out-1\nout-2"),
            ("stderr", "err\n"),
        ]
        assert await process.refresh() is process
        assert await process.wait() == 7
        assert process.returncode == 7
        assert process.status is sandbox.ProcessStatus.EXITED
        await process.terminate()
        await process.kill()
        await process.send_signal(signal.SIGINT)

    assert get_process.calls[0].request.url.params["wait"] == "false"
    assert get_process.calls[1].request.url.params["wait"] == "true"
    assert logs.call_count == 3
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

    with vercel.session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        process = box.create_process("python")
        assert process.communicate() == ("out-1\nout-2", "err\n")
        assert process.returncode == 0
        process.terminate()
        process.kill()

    assert signals == [signal.SIGTERM, signal.SIGKILL]


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

    async with vercel.session(service_options=_session_options()):
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

    async with vercel.session(service_options=_session_options()):
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

    with vercel.session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        inherited = box.run_process("python")
        assert inherited.stdout is None
        assert inherited.stderr is None
        assert capsys.readouterr() == ("out\n", "err\n")

        captured = box.run_process("python", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert captured.stdout == "out-1\nerr\nout-2\n"
        assert captured.stderr is None

    assert run.call_count == 2


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

    async with vercel.session(service_options=_session_options()):
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

    async with vercel.session(service_options=_session_options()):
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

    with vercel.session(service_options=_session_options()):
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

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(error, match=match):
            await box.run_process("python")
