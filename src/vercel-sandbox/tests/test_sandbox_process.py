import io
import json
import signal
import subprocess
from collections.abc import AsyncIterator, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event, Lock

import anyio
import httpx2 as httpx
import pytest
from sandbox_fixtures import sandbox_api_response, sandbox_service_options

import vendor.respx as respx
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


def _session_options(*, sync: bool | None = None) -> list[ServiceOptions]:
    return sandbox_service_options(
        team_id="team_1",
        project_id="prj_1",
        sync=sync,
    )


@respx.mock
async def test_async_process_readers_wait_and_signals(mock_env_clear: None) -> None:
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
    sandbox_api_response("POST", "/v2/sandboxes/sessions/sbx_1/cmd", _process_response())
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
        box = await sandbox.create_sandbox(name="preview")
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
        await process.send_signal(sandbox.ProcessSignal.SIGUSR1)
        await process.send_signal("USR1")
        with pytest.raises(ValueError, match="Unknown signal"):
            await process.send_signal(32)

    assert get_process.calls[0].request.url.params["wait"] == "false"
    assert get_process.calls[1].request.url.params["wait"] == "true"
    assert logs.call_count == 1
    assert all(call.request.headers["connection"] == "close" for call in logs.calls)
    assert signals == [
        sandbox.ProcessSignal.SIGTERM,
        sandbox.ProcessSignal.SIGKILL,
        sandbox.ProcessSignal.SIGINT,
        sandbox.ProcessSignal.SIGUSR1,
        sandbox.ProcessSignal.SIGUSR1,
    ]


@respx.mock
def test_sync_process_readers_wait_and_signals(mock_env_clear: None) -> None:
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
    sandbox_api_response("POST", "/v2/sandboxes/sessions/sbx_1/cmd", _process_response())
    sandbox_api_response("GET", "/v2/sandboxes/sessions/sbx_1/cmd/cmd_1", _process_response(0))
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
        box = sandbox_sync.create_sandbox(name="preview")
        process = box.create_process("python")
        assert process.communicate() == ("out-1\nout-2", "err\n")
        assert process.returncode == 0
        process.terminate()
        process.kill()

    assert signals == [sandbox.ProcessSignal.SIGTERM, sandbox.ProcessSignal.SIGKILL]


@respx.mock
@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize(
    ("stdout", "stderr", "expected"),
    [
        (subprocess.PIPE, subprocess.PIPE, ("out-1\nout-2", "err\n")),
        (subprocess.PIPE, subprocess.STDOUT, ("out-1\nerr\nout-2", None)),
        (subprocess.PIPE, subprocess.DEVNULL, ("out-1\nout-2", None)),
        (subprocess.DEVNULL, subprocess.PIPE, (None, "err\n")),
        (subprocess.DEVNULL, subprocess.DEVNULL, (None, None)),
        (subprocess.DEVNULL, subprocess.STDOUT, (None, None)),
    ],
    ids=["separate", "merged", "stdout-only", "stderr-only", "discarded", "discarded-merged"],
)
async def test_process_output_routing(
    mock_env_clear: None,
    sync: bool,
    stdout: int,
    stderr: int,
    expected: tuple[str | None, str | None],
) -> None:
    box: sandbox.Sandbox | sandbox_sync.SyncSandbox
    process: sandbox.Process | sandbox_sync.SyncProcess
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
    sandbox_api_response("POST", "/v2/sandboxes/sessions/sbx_1/cmd", _process_response())
    sandbox_api_response("GET", "/v2/sandboxes/sessions/sbx_1/cmd/cmd_1", _process_response(0))
    chunks = _chunked_ndjson(
        {"stream": "stdout", "data": "out-1\n"},
        {"stream": "stderr", "data": "err\n"},
        {"stream": "stdout", "data": "out-2"},
    )
    stream = _TrackingSyncStream(chunks) if sync else _TrackingAsyncStream(chunks)
    logs = respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    if sync:
        with session(service_options=_session_options(sync=True)):
            box = sandbox_sync.create_sandbox(name="preview")
            process = box.create_process("python", stdout=stdout, stderr=stderr)
            output = process.communicate()
    else:
        async with session(service_options=_session_options()):
            box = await sandbox.create_sandbox(name="preview")
            process = await box.create_process("python", stdout=stdout, stderr=stderr)
            output = await process.communicate()

    assert output == expected
    assert (process.stdout is None) == (expected[0] is None)
    assert (process.stderr is None) == (expected[1] is None)
    assert process.returncode == 0
    assert logs.call_count == int(expected != (None, None))
    assert stream.closed == (expected != (None, None))


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
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
    create = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd")

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        with pytest.raises((TypeError, ValueError)):
            await box.create_process("python", **kwargs)  # type: ignore[arg-type]

    assert create.call_count == 0


@respx.mock
def test_sync_create_process_rejects_output_options_before_request(
    mock_env_clear: None,
) -> None:
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
    create = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd")

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        with pytest.raises(ValueError, match="STDOUT is only supported for stderr"):
            box.create_process("python", stdout=subprocess.STDOUT)

    assert create.call_count == 0


@respx.mock
async def test_run_process_routes_output_checks_and_uses_one_request(
    mock_env_clear: None, capsys: pytest.CaptureFixture[str]
) -> None:
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
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
        box = await sandbox.create_sandbox(name="preview")
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
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
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
        box = await sandbox.create_sandbox(name="preview")
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
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
    run = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        side_effect=[_completed_response(), _interleaved_completed_response()]
    )

    with session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview")
        inherited = box.run_process("python")
        assert inherited.stdout is None
        assert inherited.stderr is None
        assert capsys.readouterr() == ("out\n", "err\n")

        captured = box.run_process("python", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert captured.stdout == "out-1\nerr\nout-2\n"
        assert captured.stderr is None

    assert run.call_count == 2


@respx.mock
@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("run", [False, True], ids=["readers", "run"])
async def test_process_decodes_chunked_ndjson_and_closes_response(
    mock_env_clear: None,
    sync: bool,
    run: bool,
) -> None:
    box: sandbox.Sandbox | sandbox_sync.SyncSandbox
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
    records: list[object] = [
        {"stream": "stdout", "data": "café\n"},
        {"stream": "stderr", "data": "雪\n"},
    ]
    if run:
        records = [_process_response(), *records, _process_response(0)]
        path = "/v2/sandboxes/sessions/sbx_1/cmd"
        method = "POST"
    else:
        sandbox_api_response("POST", "/v2/sandboxes/sessions/sbx_1/cmd", _process_response())
        sandbox_api_response("GET", "/v2/sandboxes/sessions/sbx_1/cmd/cmd_1", _process_response(0))
        path = "/v2/sandboxes/sessions/sbx_1/cmd/cmd_1/logs"
        method = "GET"
    chunks = _chunked_ndjson(*records)
    stream = _TrackingSyncStream(chunks) if sync else _TrackingAsyncStream(chunks)
    respx.request(method, f"https://sandbox.test{path}").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    if sync:
        with session(service_options=_session_options(sync=True)):
            box = sandbox_sync.create_sandbox(name="preview")
            if run:
                result = box.run_process("python", capture_output=True)
                output = (result.stdout, result.stderr)
            else:
                output = box.create_process("python").communicate()
    else:
        async with session(service_options=_session_options()):
            box = await sandbox.create_sandbox(name="preview")
            if run:
                result = await box.run_process("python", capture_output=True)
                output = (result.stdout, result.stderr)
            else:
                process = await box.create_process("python")
                output = await process.communicate()

    assert output == ("café\n", "雪\n")
    assert stream.closed


@respx.mock
@pytest.mark.parametrize("sync", [False, True])
async def test_run_process_replays_pre_stream_stopped_session_error(
    mock_env_clear: None, sync: bool
) -> None:
    box: sandbox.Sandbox | sandbox_sync.SyncSandbox
    events: list[str] = []
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response(session_id="sbx_old"))

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

    if sync:
        with session(service_options=_session_options(sync=True)):
            box = sandbox_sync.create_sandbox(name="preview")
            result = box.run_process("python", capture_output=True)
    else:
        async with session(service_options=_session_options()):
            box = await sandbox.create_sandbox(name="preview")
            result = await box.run_process("python", capture_output=True)

    assert result.session_id == "sbx_new"
    assert result.stdout == "out\n"
    assert events == ["old-command", "resume", "replacement-command"]


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
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
    run = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd")

    async with session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview")
        with pytest.raises((TypeError, ValueError)):
            await box.run_process("python", **kwargs)  # type: ignore[arg-type]

    assert run.call_count == 0


@respx.mock
@pytest.mark.parametrize(("sync", "fail_on"), [(False, "write"), (True, "flush")])
async def test_run_process_closes_response_after_sink_failure(
    mock_env_clear: None,
    sync: bool,
    fail_on: str,
) -> None:
    box: sandbox.Sandbox | sandbox_sync.SyncSandbox
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
    stream = (
        _TrackingSyncStream(_completed_body()) if sync else _TrackingAsyncStream(_completed_body())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, stream=stream)
    )
    if sync:
        with session(service_options=_session_options(sync=True)):
            box = sandbox_sync.create_sandbox(name="preview")
            with pytest.raises(OSError, match=f"sink {fail_on} failed"):
                box.run_process("python", stdout=_FailingTextIO(fail_on=fail_on))
    else:
        async with session(service_options=_session_options()):
            box = await sandbox.create_sandbox(name="preview")
            with pytest.raises(OSError, match=f"sink {fail_on} failed"):
                await box.run_process("python", stdout=_FailingTextIO(fail_on=fail_on))
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
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
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
        box = await sandbox.create_sandbox(name="preview")
        with pytest.raises(error, match=match):
            await box.run_process("python")


@respx.mock
@pytest.mark.parametrize("sync", [False, True])
async def test_run_process_does_not_replay_lifecycle_stream_errors(
    mock_env_clear: None, sync: bool
) -> None:
    box: sandbox.Sandbox | sandbox_sync.SyncSandbox
    sandbox_api_response("POST", "/v3/sandboxes", _sandbox_response())
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
    resume_route = sandbox_api_response("GET", "/v2/sandboxes/preview", _sandbox_response())

    if sync:
        with session(service_options=_session_options(sync=True)):
            box = sandbox_sync.create_sandbox(name="preview")
            with pytest.raises(sandbox.SandboxStreamError, match="stream stopped") as exc_info:
                box.run_process("python")
    else:
        async with session(service_options=_session_options()):
            box = await sandbox.create_sandbox(name="preview")
            with pytest.raises(sandbox.SandboxStreamError, match="stream stopped") as exc_info:
                await box.run_process("python")

    assert exc_info.value.code == "sandbox_stopped"
    assert not resume_route.called


def _stopped_error() -> sandbox.SandboxApiError:
    data = {"error": {"code": "sandbox_stopped", "message": "session stopped"}}
    return sandbox.SandboxApiError(httpx.Response(409), "session stopped", data=data)


def _lifecycle_error(code: str) -> sandbox.SandboxApiError:
    data = {"error": {"code": code, "message": "session unavailable"}}
    return sandbox.SandboxApiError(httpx.Response(409), "session unavailable", data=data)


class _AsyncCoordinatedRecoveryService:
    def __init__(
        self, *, resume_error: BaseException | None = None, error_code: str = "sandbox_stopped"
    ) -> None:
        self.error_code = error_code
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
            raise _lifecycle_error(self.error_code)
        return []

    async def get_runtime_session(self, *, session_id: str) -> SandboxRuntimeSessionState:
        raise AssertionError("recovery must not poll the old session")

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
@pytest.mark.parametrize(
    "error_code", ["sandbox_stopped", "sandbox_stopping", "sandbox_snapshotting"]
)
async def test_async_recovery_shares_success(anyio_backend: str, error_code: str) -> None:
    service = _AsyncCoordinatedRecoveryService(error_code=error_code)
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
        error_code: str = "sandbox_stopped",
    ) -> None:
        self.error_code = error_code
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
            raise _lifecycle_error(self.error_code)
        return []

    async def get_runtime_session(self, *, session_id: str) -> SandboxRuntimeSessionState:
        raise AssertionError("recovery must not poll the old session")

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


@pytest.mark.parametrize(
    "error_code", ["sandbox_stopped", "sandbox_stopping", "sandbox_snapshotting"]
)
@pytest.mark.parametrize("interrupt_initiator", [False, True])
def test_sync_recovery_shares_or_re_elects_after_initiator_interruption(
    interrupt_initiator: bool,
    error_code: str,
) -> None:
    service = _SyncCoordinatedRecoveryService(
        interrupt_first_resume=interrupt_initiator, error_code=error_code
    )
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


@pytest.mark.parametrize(
    "error_code", ["sandbox_stopped", "sandbox_stopping", "sandbox_snapshotting"]
)
def test_sync_recovery_shares_failure_and_clears_slot_for_retry(error_code: str) -> None:
    resume_error = RuntimeError("resume failed")
    service = _SyncCoordinatedRecoveryService(
        first_resume_error=resume_error, error_code=error_code
    )
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
