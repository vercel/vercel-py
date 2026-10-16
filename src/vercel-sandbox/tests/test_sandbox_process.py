"""Running processes: handles, output routing, streams, and recovery."""

import asyncio
import io
import json
import signal
import subprocess
from collections.abc import AsyncIterator, Iterator
from concurrent.futures import ThreadPoolExecutor
from threading import Condition, Event
from typing import Any, NoReturn

import anyio
import httpx2 as httpx
import pytest
from fake_sandbox import FakeSandboxApi, ProgramResult, RecordedRequest, wire
from sandbox_modes import Mode

from vercel import sandbox
from vercel.sandbox import SandboxApiError, sync as sandbox_sync


def _resumes(request: RecordedRequest) -> bool:
    return request.params.get("resume") == "true"


def _on(session_id: str) -> Any:
    return lambda request: request.path_params.get("session_id") == session_id


def _seed_programs(fake_api: FakeSandboxApi) -> None:
    fake_api.command("report", stdout="out\n", stderr="err\n")
    fake_api.command("report fail", stdout="out\n", stderr="err\n", exit_code=9)
    fake_api.command(
        "interleave",
        ProgramResult(output=[("stdout", "out-1\n"), ("stderr", "err\n"), ("stdout", "out-2\n")]),
    )


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


class _TrackingStream(httpx.AsyncByteStream, httpx.SyncByteStream):
    """A response body that delivers fixed chunks and records being closed."""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk

    def __iter__(self) -> Iterator[bytes]:
        yield from self.chunks

    async def aclose(self) -> None:
        self.closed = True

    def close(self) -> None:
        self.closed = True


def _ndjson(*records: object, separator: str = "\n") -> bytes:
    return (
        separator.join(
            record if isinstance(record, str) else json.dumps(record, ensure_ascii=False)
            for record in records
        ).encode()
        + separator.encode()
    )


def _command(exit_code: int | None = None, **fields: Any) -> wire.JSON:
    return {"command": wire.command_payload(name="report", exit_code=exit_code, **fields)}


def test_public_process_exports() -> None:
    for name in (
        "CompletedProcess",
        "Process",
        "ProcessSignal",
        "ProcessStatus",
        "SandboxCredentials",
        "SandboxCredentialsFactory",
        "TextReader",
    ):
        assert name in sandbox.__all__
    for name in (
        "CompletedProcess",
        "ProcessSignal",
        "ProcessStatus",
        "SandboxCredentials",
        "SyncProcess",
        "SyncSandboxCredentialsFactory",
        "SyncTextReader",
    ):
        assert name in sandbox_sync.__all__


# ---- process handles ------------------------------------------------------------


async def test_process_handles_read_wait_and_signal(fake_api: FakeSandboxApi) -> None:
    fake_api.command("serve", stdout="out-1\nout-2", stderr="err\n", exit_code=7, duration=10)

    async with fake_api.session():
        box = await sandbox.create_sandbox(name="preview")
        process = await box.create_process("serve", ["--port", "3000"])
        command = fake_api.current_session("preview").commands[process.id]
        assert process.name == "serve"
        assert process.args == ["--port", "3000"]
        assert process.cwd == "/vercel/sandbox"
        assert process.session_id == "sbx_1"
        assert process.started_at == command.started_at
        assert process.status is sandbox.ProcessStatus.RUNNING
        assert process.stdin is None
        assert process.returncode is None
        assert await process.refresh() is process
        assert process.status is sandbox.ProcessStatus.RUNNING
        assert process.stdout is not None and process.stderr is not None
        assert await process.stdout.readline() == "out-1\n"
        assert await process.stdout.read() == "out-2"
        assert await process.stderr.read() == "err\n"
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

    refresh, wait = fake_api.requests_to("commands.get")
    assert (refresh.params["wait"], wait.params["wait"]) == ("false", "true")
    (logs,) = fake_api.requests_to("commands.logs")
    assert logs.headers["connection"] == "close"
    assert [request.json()["signal"] for request in fake_api.requests_to("commands.kill")] == [
        sandbox.ProcessSignal.SIGTERM,
        sandbox.ProcessSignal.SIGKILL,
        sandbox.ProcessSignal.SIGINT,
        sandbox.ProcessSignal.SIGUSR1,
        sandbox.ProcessSignal.SIGUSR1,
    ]


async def test_a_signal_ends_a_running_process(mode: Mode, fake_api: FakeSandboxApi) -> None:
    fake_api.command("serve", stdout="never printed", duration=60)

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        process = await mode(box.create_process("serve"))
        await mode(process.terminate())
        assert await mode(process.communicate()) == ("", "")

    assert process.returncode == 128 + signal.SIGTERM


@pytest.mark.parametrize(
    ("stderr", "expected"),
    [
        (subprocess.STDOUT, ("out-1\nerr\nout-2\n", None)),
        (subprocess.DEVNULL, ("out-1\nout-2\n", None)),
    ],
)
async def test_stderr_can_be_merged_or_dropped(
    mode: Mode, fake_api: FakeSandboxApi, stderr: int, expected: tuple[str, None]
) -> None:
    _seed_programs(fake_api)

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        process = await mode(box.create_process("interleave", stderr=stderr))
        assert process.stderr is None
        assert process.stdout is not None
        assert await mode(process.communicate()) == expected
        assert process.returncode == 0

    assert len(fake_api.requests_to("commands.logs")) == 1


@pytest.mark.parametrize("stderr", [subprocess.DEVNULL, subprocess.STDOUT])
async def test_a_process_without_readers_never_requests_logs(
    mode: Mode, fake_api: FakeSandboxApi, stderr: int
) -> None:
    _seed_programs(fake_api)

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        process = await mode(box.create_process("report", stdout=subprocess.DEVNULL, stderr=stderr))
        assert process.stdout is None
        assert process.stderr is None
        assert await mode(process.communicate()) == (None, None)
        assert process.returncode == 0

    assert not fake_api.requests_to("commands.logs")


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
async def test_create_process_rejects_output_options_before_any_request(
    mode: Mode, fake_api: FakeSandboxApi, kwargs: dict[str, object]
) -> None:
    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        with pytest.raises((TypeError, ValueError)):
            await mode(box.create_process("report", **kwargs))

    assert not fake_api.requests_to("commands.create")


def test_sync_stdout_cannot_be_merged_into_stderr(fake_api: FakeSandboxApi) -> None:
    with fake_api.session(sync=True):
        box = sandbox_sync.create_sandbox(name="preview")
        with pytest.raises(ValueError, match="STDOUT is only supported for stderr"):
            box.create_process("report", stdout=subprocess.STDOUT)


# ---- run_process ----------------------------------------------------------------


async def test_run_process_routes_output_and_checks_in_one_request(
    mode: Mode, fake_api: FakeSandboxApi, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_programs(fake_api)

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        inherited = await mode(box.run_process("report", ("--verbose",)))
        assert isinstance(inherited, sandbox.CompletedProcess)
        assert inherited.args == ("report", "--verbose")
        assert inherited.returncode == 0
        assert (inherited.stdout, inherited.stderr) == (None, None)
        assert capsys.readouterr() == ("out\n", "err\n")

        captured = await mode(box.run_process("report", capture_output=True))
        assert (captured.stdout, captured.stderr) == ("out\n", "err\n")

        merged = await mode(
            box.run_process("interleave", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        )
        assert (merged.stdout, merged.stderr) == ("out-1\nerr\nout-2\n", None)

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            await mode(box.run_process("report", ["fail"], check=True))
        assert exc_info.value.returncode == 9
        assert (exc_info.value.stdout, exc_info.value.stderr) == (None, None)
        assert capsys.readouterr() == ("out\n", "err\n")

        with pytest.raises(subprocess.CalledProcessError) as captured_error:
            await mode(box.run_process("report", ["fail"], check=True, capture_output=True))
        assert (captured_error.value.stdout, captured_error.value.stderr) == ("out\n", "err\n")

    runs = fake_api.requests_to("commands.create")
    assert len(runs) == 5
    assert all(request.json()["wait"] and request.json()["logs"] for request in runs)
    assert not fake_api.requests_to("commands.logs")
    assert not fake_api.requests_to("commands.get")


async def test_run_process_writes_to_explicit_or_discarded_destinations(
    fake_api: FakeSandboxApi, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_programs(fake_api)
    sink = _RecordingTextIO()

    async with fake_api.session():
        box = await sandbox.create_sandbox(name="preview")
        result = await box.run_process("interleave", stdout=sink, stderr=subprocess.STDOUT)
        assert (result.stdout, result.stderr) == (None, None)
        assert sink.getvalue() == "out-1\nerr\nout-2\n"
        assert sink.flush_count == 3

        discarded = await box.run_process(
            "interleave", stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )
        assert (discarded.stdout, discarded.stderr) == (None, None)

        inherited = await box.run_process("interleave", stderr=subprocess.STDOUT)
        assert (inherited.stdout, inherited.stderr) == (None, None)
        assert capsys.readouterr() == ("out-1\nerr\nout-2\n", "")

        stdout_only = await box.run_process(
            "report", stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        assert (stdout_only.stdout, stdout_only.stderr) == ("out\n", None)


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
async def test_run_process_rejects_output_options_before_any_request(
    fake_api: FakeSandboxApi, kwargs: dict[str, object]
) -> None:
    async with fake_api.session():
        box = await sandbox.create_sandbox(name="preview")
        with pytest.raises((TypeError, ValueError)):
            await box.run_process("report", **kwargs)  # type: ignore[arg-type]

    assert not fake_api.requests_to("commands.create")


async def test_run_process_replays_on_a_replacement_when_the_session_stopped(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    _seed_programs(fake_api)

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview", destroy=False))
        fake_api.stop("preview")
        result = await mode(box.run_process("report", capture_output=True))

    assert result.session_id == "sbx_2"
    assert result.stdout == "out\n"
    assert [request.route for request in fake_api.requests[1:4]] == [
        "commands.create",
        "sandboxes.get",
        "commands.create",
    ]


# ---- the wire: chunked, closed, and malformed streams -----------------------------


async def test_run_process_reassembles_ndjson_split_across_chunks(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    body = _ndjson(
        _command(),
        wire.log_record("stdout", "café\n"),
        wire.log_record("stderr", "雪\n"),
        _command(0),
        separator="\r\n\r\n",
    )
    stream = _TrackingStream([body[offset : offset + 1] for offset in range(len(body))])
    fake_api.respond("commands.create", httpx.Response(200, stream=stream))

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        result = await mode(box.run_process("report", capture_output=True))

    assert (result.stdout, result.stderr) == ("café\n", "雪\n")
    assert stream.closed


async def test_process_readers_reassemble_ndjson_split_across_chunks(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    body = _ndjson(
        wire.log_record("stdout", "café\n"), wire.log_record("stderr", "雪\n"), separator="\r\n\r\n"
    )
    stream = _TrackingStream([body[offset : offset + 1] for offset in range(len(body))])
    fake_api.respond("commands.logs", httpx.Response(200, stream=stream))

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        process = await mode(box.create_process("true"))
        output = await mode(process.communicate())

    assert output == ("café\n", "雪\n")
    assert stream.closed


@pytest.mark.parametrize("fail_on", ["write", "flush"])
async def test_run_process_closes_the_response_when_the_sink_fails(
    mode: Mode, fake_api: FakeSandboxApi, fail_on: str
) -> None:
    stream = _TrackingStream([_ndjson(_command(), wire.log_record("stdout", "out\n"), _command(0))])
    fake_api.respond("commands.create", httpx.Response(200, stream=stream))

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        with pytest.raises(OSError, match=f"sink {fail_on} failed"):
            await mode(box.run_process("report", stdout=_FailingTextIO(fail_on=fail_on)))

    assert stream.closed


@pytest.mark.parametrize(
    ("records", "error", "match"),
    [
        (["not-json"], sandbox.SandboxResponseError, "malformed NDJSON"),
        ([_command()], sandbox.SandboxResponseError, "missing final"),
        (
            [_command(), _command(0, id="cmd_other")],
            sandbox.SandboxResponseError,
            "different final process identity",
        ),
        (
            [
                _command(),
                {"stream": "error", "data": {"code": "failed", "message": "stream failed"}},
            ],
            sandbox.SandboxStreamError,
            "stream failed",
        ),
    ],
)
async def test_run_process_rejects_invalid_streams(
    fake_api: FakeSandboxApi, records: list[object], error: type[Exception], match: str
) -> None:
    fake_api.respond("commands.create", httpx.Response(200, content=_ndjson(*records)))

    async with fake_api.session():
        box = await sandbox.create_sandbox(name="preview")
        with pytest.raises(error, match=match):
            await box.run_process("report")


async def test_run_process_does_not_replay_a_stream_that_failed_midway(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    stream_error = {
        "stream": "error",
        "data": {"code": "sandbox_stopped", "message": "stream stopped"},
    }
    fake_api.respond(
        "commands.create", httpx.Response(200, content=_ndjson(_command(), stream_error))
    )

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        with pytest.raises(sandbox.SandboxStreamError, match="stream stopped") as exc_info:
            await mode(box.run_process("report"))

    assert exc_info.value.code == "sandbox_stopped"
    assert not fake_api.requests_to("sandboxes.get")


# ---- recovery coordination ----------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
@pytest.mark.parametrize("status", ["stopped", "stopping", "snapshotting"])
async def test_concurrent_operations_share_one_recovery(
    fake_api: FakeSandboxApi, anyio_backend: str, status: str
) -> None:
    fake_api.seed()
    resume = fake_api.hold("sandboxes.get", when=_resumes)
    results: list[object] = []

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        fake_api.stop("preview", status=status)

        async def run() -> None:
            results.append(await box.run_process("true"))

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(run)
            await resume.wait_entered()
            task_group.start_soon(run)
            await fake_api.wait_for_requests("commands.create", 2, when=_on("sbx_1"))
            await anyio.sleep(0.01)
            resume.release()

    assert len(results) == 2
    assert len(fake_api.requests_to("sandboxes.get", when=_resumes)) == 1
    assert len(fake_api.requests_to("commands.create", when=_on("sbx_2"))) == 2
    assert box.current_session_id == "sbx_2"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_a_waiter_takes_over_when_the_recovering_caller_is_cancelled(
    fake_api: FakeSandboxApi, anyio_backend: str
) -> None:
    fake_api.seed()
    resume = fake_api.hold("sandboxes.get", when=_resumes, times=1)
    waiter_result: object = None

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        fake_api.stop("preview")
        owner_scope = anyio.CancelScope()
        owner_done = anyio.Event()

        async def own_recovery() -> None:
            with owner_scope:
                try:
                    await box.run_process("true")
                finally:
                    owner_done.set()

        async def wait_for_recovery() -> None:
            nonlocal waiter_result
            waiter_result = await box.run_process("true")

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(own_recovery)
            await resume.wait_entered()
            task_group.start_soon(wait_for_recovery)
            await fake_api.wait_for_requests("commands.create", 2, when=_on("sbx_1"))
            await anyio.sleep(0.01)
            owner_scope.cancel()
            await owner_done.wait()
        resume.release()

    assert waiter_result is not None
    assert len(fake_api.requests_to("sandboxes.get", when=_resumes)) == 2
    assert box.current_session_id == "sbx_2"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_a_recovery_is_abandoned_when_every_caller_is_cancelled(
    fake_api: FakeSandboxApi, anyio_backend: str
) -> None:
    fake_api.seed()
    resume = fake_api.hold("sandboxes.get", when=_resumes)

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        fake_api.stop("preview")
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(box.run_process, "true")
            await resume.wait_entered()
            task_group.start_soon(box.run_process, "true")
            await fake_api.wait_for_requests("commands.create", 2, when=_on("sbx_1"))
            task_group.cancel_scope.cancel()
        resume.release()
        await anyio.sleep(0.01)

    assert box.current_session_id == "sbx_1"
    assert fake_api.sandbox("preview").current_session_id == "sbx_1"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_a_failed_recovery_fails_every_caller_and_can_be_retried(
    fake_api: FakeSandboxApi, anyio_backend: str
) -> None:
    fake_api.seed()
    resume = fake_api.hold("sandboxes.get", when=_resumes, times=1)
    fake_api.fail("sandboxes.get", status=500, code="internal_error", when=_resumes)
    errors: list[BaseException] = []

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        fake_api.stop("preview")

        async def run() -> None:
            try:
                await box.run_process("true")
            except SandboxApiError as error:
                errors.append(error)

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(run)
            await resume.wait_entered()
            task_group.start_soon(run)
            await fake_api.wait_for_requests("commands.create", 2, when=_on("sbx_1"))
            await anyio.sleep(0.01)
            resume.release()

        assert len(errors) == 2
        assert errors[0] is errors[1]
        assert len(fake_api.requests_to("sandboxes.get", when=_resumes)) == 1
        await box.run_process("true")

    assert len(fake_api.requests_to("sandboxes.get", when=_resumes)) == 2
    assert box.current_session_id == "sbx_2"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_a_late_failure_from_the_old_session_recovers_again(
    fake_api: FakeSandboxApi, anyio_backend: str
) -> None:
    fake_api.seed()
    first = fake_api.hold("commands.create", when=_on("sbx_1"), times=1)
    second = fake_api.hold("commands.create", when=_on("sbx_1"), times=1)
    results: list[object] = []

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        fake_api.stop("preview")

        async def run() -> None:
            results.append(await box.run_process("true"))

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(run)
            task_group.start_soon(run)
            await first.wait_entered()
            await second.wait_entered()
            first.release()
            while not results:
                await anyio.sleep(0.001)
            second.release()

    assert len(results) == 2
    assert len(fake_api.requests_to("sandboxes.get", when=_resumes)) == 2
    assert box.current_session_id == "sbx_2"


@pytest.mark.parametrize("status", ["stopped", "stopping", "snapshotting"])
@pytest.mark.parametrize("interrupt_initiator", [False, True])
def test_sync_recovery_is_shared_or_taken_over_after_an_interrupt(
    fake_api: FakeSandboxApi, interrupt_initiator: bool, status: str
) -> None:
    fake_api.seed()
    resume = fake_api.hold("sandboxes.get", when=_resumes, times=1)
    if interrupt_initiator:

        def interrupt(_request: RecordedRequest) -> NoReturn:
            raise KeyboardInterrupt

        fake_api.respond("sandboxes.get", interrupt, when=_resumes)
    joined = Event()

    class ObservedCondition(Condition):
        def wait(self, timeout: float | None = None) -> bool:
            joined.set()
            return super().wait(timeout)

    with fake_api.session(sync=True):
        box = sandbox_sync.get_sandbox(name="preview")
        box._recovery_condition = ObservedCondition()
        fake_api.stop("preview", status=status)
        with ThreadPoolExecutor(max_workers=2) as executor:
            initiating = executor.submit(box.run_process, "true")
            resume.wait_entered_sync()
            waiting = executor.submit(box.run_process, "true")
            assert joined.wait(timeout=5)
            resume.release()
            if interrupt_initiator:
                with pytest.raises(KeyboardInterrupt):
                    initiating.result(timeout=5)
            else:
                initiating.result(timeout=5)
            waiting.result(timeout=5)

    assert len(fake_api.requests_to("sandboxes.get", when=_resumes)) == (
        2 if interrupt_initiator else 1
    )
    assert box.current_session_id == "sbx_2"


def test_sync_failed_recovery_fails_every_caller_and_can_be_retried(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed()
    resume = fake_api.hold("sandboxes.get", when=_resumes, times=1)
    fake_api.fail("sandboxes.get", status=500, code="internal_error", when=_resumes)
    joined = Event()

    class ObservedCondition(Condition):
        def wait(self, timeout: float | None = None) -> bool:
            joined.set()
            return super().wait(timeout)

    with fake_api.session(sync=True):
        box = sandbox_sync.get_sandbox(name="preview")
        box._recovery_condition = ObservedCondition()
        fake_api.stop("preview")
        with ThreadPoolExecutor(max_workers=2) as executor:
            participants = [executor.submit(box.run_process, "true")]
            resume.wait_entered_sync()
            participants.append(executor.submit(box.run_process, "true"))
            assert joined.wait(timeout=5)
            resume.release()
            errors = [participant.exception(timeout=5) for participant in participants]

        assert isinstance(errors[0], SandboxApiError)
        assert errors[0] is errors[1]
        box.run_process("true")

    assert len(fake_api.requests_to("sandboxes.get", when=_resumes)) == 2
    assert box.current_session_id == "sbx_2"


async def test_a_stale_reply_does_not_undo_a_recovery(mode: Mode, fake_api: FakeSandboxApi) -> None:
    fake_api.seed()
    extend = fake_api.hold("sessions.extend_timeout", after_response=True)

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        with ThreadPoolExecutor(max_workers=1) as executor:
            if mode.sync:
                mutation: Any = executor.submit(box.extend_execution_time_limit, 2)
                extend.wait_entered_sync()
            else:
                mutation = asyncio.create_task(box.extend_execution_time_limit(2))
                await extend.wait_entered()
            fake_api.stop("preview")
            await mode(box.run_process("true"))
            extend.release()
            result = mutation.result(timeout=5) if mode.sync else await mutation

    assert result.id == "sbx_2"
    assert box.current_session_id == "sbx_2"
