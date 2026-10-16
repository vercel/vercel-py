"""Runtime sessions: acquiring them, recovering from stopped ones, cleaning up.

A sandbox outlives its sessions. When one stops underneath a handle (a
timeout, the platform, another client), the next operation that needs a live
session resumes the sandbox once and replays itself on the replacement.
These tests stop sessions out of band with `FakeSandboxApi.stop` and hold
requests with gates to pin down the races.
"""

import asyncio
import gc
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Condition, Event
from typing import Any

import anyio
import httpx2 as httpx
import pytest
from fake_sandbox import FakeSandboxApi, RecordedRequest, wire
from sandbox_modes import Mode

from vercel import sandbox
from vercel.sandbox import (
    NetworkPolicy,
    SandboxApiError,
    SandboxCleanupError,
    SandboxResponseError,
    SandboxStatus,
    sync as sandbox_sync,
)


def _resumes(request: RecordedRequest) -> bool:
    return request.params.get("resume") == "true"


def _trail(fake_api: FakeSandboxApi) -> list[str]:
    """Requests so far, as lookups, resumes, and ``route@session``."""
    trail = []
    for request in fake_api.requests:
        if request.route == "sandboxes.get":
            trail.append("resume" if _resumes(request) else "lookup")
        elif "session_id" in request.path_params:
            trail.append(f"{request.route}@{request.path_params['session_id']}")
        else:
            trail.append(request.route)
    return trail


def _stops(fake_api: FakeSandboxApi) -> list[str]:
    return [request.path_params["session_id"] for request in fake_api.requests_to("sessions.stop")]


def _stop_response(*, sandbox_payload: wire.JSON | None = None) -> httpx.Response:
    body: wire.JSON = {"session": wire.session_payload(status="stopped", stopped_at=2)}
    if sandbox_payload is not None:
        body["sandbox"] = sandbox_payload
    return httpx.Response(200, json=body)


_RECOVERABLE: dict[str, tuple[Callable[[Any], Any], str]] = {
    "create_process": (lambda box: box.create_process("python", ["--version"]), "commands.create"),
    "extend_execution_time_limit": (
        lambda box: box.extend_execution_time_limit(2),
        "sessions.extend_timeout",
    ),
    "update_network_policy": (
        lambda box: box.update_network_policy(NetworkPolicy.allow_all()),
        "sessions.network_policy",
    ),
    "snapshot": (lambda box: box.snapshot(), "sessions.snapshot"),
}


@pytest.mark.parametrize("operation", list(_RECOVERABLE))
async def test_an_operation_on_a_stopped_session_resumes_and_replays_once(
    mode: Mode, fake_api: FakeSandboxApi, operation: str
) -> None:
    fake_api.seed()
    call, route = _RECOVERABLE[operation]

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        fake_api.stop("preview")
        result = await mode(call(box))

    assert result is not None
    assert box.current_session_id == "sbx_2"
    assert _trail(fake_api) == ["lookup", f"{route}@sbx_1", "resume", f"{route}@sbx_2"]


async def test_process_history_stays_readable_after_a_stop(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    # The API serves command history for stopped sessions, so reading it never
    # needs a replacement session.
    fake_api.seed()

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        process = await mode(box.create_process("python", ["--version"]))
        fake_api.stop("preview")
        listed = await mode(box.query_processes())
        fetched = await mode(box.get_process(process.id))

    assert [item.id for item in listed] == [fetched.id] == [process.id]
    assert box.current_session_id == "sbx_1"
    assert not fake_api.requests_to("sandboxes.get", when=_resumes)


async def test_a_sparse_lookup_targets_the_current_session_without_resuming(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed()
    fake_api.respond(
        "sandboxes.get",
        lambda _request: httpx.Response(
            200,
            json={
                "sandbox": fake_api.sandbox("preview").payload(fake_api.current_session("preview"))
            },
        ),
    )

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        assert box.current_session_id == "sbx_1"
        assert box.current_session is None
        process = await box.create_process("python")

    assert process.session_id == "sbx_1"
    assert _trail(fake_api) == ["lookup", "commands.create@sbx_1"]


@pytest.mark.parametrize(
    ("include_system_routes", "projection"), [(True, "true"), (False, "false"), (None, None)]
)
async def test_recovery_keeps_the_lookup_project_and_route_projection(
    fake_api: FakeSandboxApi, include_system_routes: bool | None, projection: str | None
) -> None:
    fake_api.project_id = "prj_bound"
    fake_api.seed()

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview", include_system_routes=include_system_routes)
        fake_api.stop("preview")
        await box.create_process("python")

    lookups = fake_api.requests_to("sandboxes.get")
    assert [request.params["resume"] for request in lookups] == ["false", "true"]
    for request in lookups:
        assert request.params["projectId"] == "prj_bound"
        assert request.params.get("__includeSystemRoutes") == projection


async def test_box_session_returns_the_live_session_or_a_replacement(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed()

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview", include_system_routes=True)
        old = box.current_session
        assert old is not None
        acquired = await box.session()
        assert acquired is old is box.current_session

        fake_api.stop("preview")
        replacement = await box.session()
        assert replacement is box.current_session
        assert replacement is not old
        assert replacement.id == "sbx_2"

    lookups = fake_api.requests_to("sandboxes.get")
    assert [request.params["resume"] for request in lookups] == ["false", "true", "true"]
    assert all(request.params["__includeSystemRoutes"] == "true" for request in lookups)


def test_sync_box_session_is_both_a_session_and_a_context(fake_api: FakeSandboxApi) -> None:
    fake_api.seed()

    with fake_api.session(sync=True):
        box = sandbox_sync.get_sandbox(name="preview")
        old = box.current_session
        acquired = box.session()
        assert acquired is old is box.current_session
        with acquired as entered:
            assert entered is acquired
            fake_api.stop("preview")
            replacement = box.session()
            assert replacement is box.current_session
            assert replacement is not acquired

    assert _stops(fake_api) == ["sbx_1"]
    assert replacement.status is SandboxStatus.RUNNING


async def test_a_managed_session_applies_its_own_stop_to_the_sandbox(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed(ports=[3000])

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        routes, updated_at, project_id = box.routes, box.updated_at, box.project_id
        fake_api.clock.advance(5)
        async with box.session() as acquired:
            assert acquired is box.current_session
        assert acquired.status is SandboxStatus.STOPPED
        assert box.status is SandboxStatus.STOPPED
        assert box.updated_at is not None and updated_at is not None
        assert box.updated_at > updated_at
        assert box.routes == routes
        assert box.raw is not None
        assert box.project_id == project_id == "prj_123"
        assert box.current_session is acquired

    assert _stops(fake_api) == ["sbx_1"]


async def test_managed_session_cleanup_never_rolls_back_a_replacement(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed()

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        async with mode.enter(box.session()) as acquired:
            fake_api.stop("preview")
            await mode(box.create_process("true"))
            replacement = box.current_session
            assert replacement is not None and replacement.id == "sbx_2"
        assert acquired.status is SandboxStatus.STOPPED
        assert box.current_session is replacement
        assert replacement.status is SandboxStatus.RUNNING

    assert _stops(fake_api) == ["sbx_1"]


@pytest.mark.parametrize(
    "error",
    [
        wire.error_payload("sandbox_stopped", "Sandbox has stopped execution"),
        {"error": {"message": "gone"}},
    ],
)
async def test_session_cleanup_tolerates_an_already_stopped_session(
    mode: Mode, fake_api: FakeSandboxApi, error: wire.JSON
) -> None:
    fake_api.seed()
    fake_api.respond("sessions.stop", httpx.Response(410, json=error), times=2)

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        async with mode.enter(box.session()) as acquired:
            pass
        if mode.sync:
            with acquired:
                pass

    assert acquired.status is SandboxStatus.STOPPED
    assert len(_stops(fake_api)) == 1


async def test_session_cleanup_failure_after_a_failed_block_warns(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed()
    fake_api.fail("sessions.stop", status=500, code="stop_failed")
    original = ValueError("block failed")

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        with pytest.warns(RuntimeWarning) as warnings_info:
            with pytest.raises(ValueError) as exc_info:
                async with box.session():
                    raise original

    assert exc_info.value is original
    cleanup_error = warnings_info[0].source
    assert isinstance(cleanup_error, SandboxCleanupError)
    assert cleanup_error.resource_type == "sandbox_runtime_session"
    assert cleanup_error.resource_id == "sbx_1"
    assert isinstance(cleanup_error.cause, SandboxApiError)


async def test_session_cleanup_failure_after_a_successful_block_raises(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed()
    fake_api.fail("sessions.stop", status=503, code="unavailable")

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        with pytest.raises(SandboxCleanupError) as exc_info:
            async with mode.enter(box.session()):
                pass

    assert exc_info.value.resource_type == "sandbox_runtime_session"
    assert exc_info.value.resource_id == "sbx_1"
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "unavailable"


@pytest.mark.parametrize("block_fails", [False, True])
async def test_cleanup_wraps_a_stop_result_about_another_sandbox(
    fake_api: FakeSandboxApi, block_fails: bool
) -> None:
    fake_api.seed()
    fake_api.respond(
        "sessions.stop",
        _stop_response(sandbox_payload=wire.sandbox_payload(name="other", status="stopped")),
    )
    original = ValueError("block failed")

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        cleanup_error: BaseException | None
        if block_fails:
            with pytest.warns(RuntimeWarning) as warnings_info:
                with pytest.raises(ValueError) as exc_info:
                    async with box.session():
                        raise original
            assert exc_info.value is original
            cleanup_error = warnings_info[0].source
        else:
            with pytest.raises(SandboxCleanupError) as cleanup_info:
                async with box.session():
                    pass
            cleanup_error = cleanup_info.value

    assert isinstance(cleanup_error, SandboxCleanupError)
    assert cleanup_error.resource_type == "sandbox_runtime_session"
    assert cleanup_error.resource_id == "sbx_1"
    assert isinstance(cleanup_error.cause, SandboxResponseError)


@pytest.mark.parametrize("current_session_id", [None, "sbx_other"])
def test_sync_session_cleanup_ignores_sandbox_metadata_it_does_not_own(
    fake_api: FakeSandboxApi, current_session_id: str | None
) -> None:
    fake_api.seed()
    fake_api.respond(
        "sessions.stop",
        _stop_response(
            sandbox_payload=None
            if current_session_id is None
            else wire.sandbox_payload(current_session_id=current_session_id, status="stopped")
        ),
    )

    with fake_api.session(sync=True):
        box = sandbox_sync.get_sandbox(name="preview")
        with box.session() as acquired:
            pass

    assert acquired.status is SandboxStatus.STOPPED
    assert box.status is SandboxStatus.RUNNING
    assert box.current_session_id == "sbx_1"
    assert len(fake_api.requests_to("sandboxes.get")) == 2


def test_stopping_a_listed_session_does_not_touch_its_sandbox(fake_api: FakeSandboxApi) -> None:
    fake_api.seed()

    with fake_api.session(sync=True):
        box = sandbox_sync.get_sandbox(name="preview")
        (listed,) = box.list_sessions()
        with listed:
            pass

    assert listed.status is SandboxStatus.STOPPED
    assert box.status is SandboxStatus.RUNNING
    assert box.current_session_id == "sbx_1"


async def test_box_session_operations_are_single_use(fake_api: FakeSandboxApi) -> None:
    fake_api.seed()

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        operation = box.session()
        await operation
        with pytest.raises(RuntimeError, match="can only be used once"):
            await operation
        with pytest.warns(RuntimeWarning, match=r"await box\.session"):
            unconsumed = box.session()
            del unconsumed
            gc.collect()


@pytest.mark.parametrize("error_code", ["sandbox_stopping", "sandbox_snapshotting"])
async def test_acquisition_retries_a_resume_that_lands_mid_transition(
    mode: Mode, fake_api: FakeSandboxApi, error_code: str
) -> None:
    # The API waits out a stopping or snapshotting session before it resumes,
    # but a resume that still reports one is retried once.
    fake_api.seed()
    fake_api.fail("sandboxes.get", status=422, code=error_code, when=_resumes)

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        fake_api.stop("preview")
        acquired = await mode(box.session())

    assert acquired.id == "sbx_2"
    assert _trail(fake_api) == ["lookup", "resume", "resume"]


async def test_managed_handles_stop_the_session_they_end_up_with(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed()

    async with mode.session():
        async with mode.enter(mode.sandbox.resume_sandbox(name="preview")) as resumed:
            fake_api.stop("preview")
            await mode(resumed.create_process("true"))
            assert resumed.current_session_id == "sbx_2"
        async with mode.enter(mode.sandbox.create_sandbox(name="created")) as created:
            fake_api.stop("created")
            await mode(created.create_process("true"))
            assert created.current_session_id == "sbx_4"

    assert _stops(fake_api) == ["sbx_2", "sbx_4"]
    assert [
        request.path_params["name"] for request in fake_api.requests_to("sandboxes.delete")
    ] == ["created"]


# ---- concurrency ---------------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_cancelling_session_entry_abandons_the_resume(
    fake_api: FakeSandboxApi, anyio_backend: str
) -> None:
    fake_api.seed()
    resume = fake_api.hold("sandboxes.get", when=_resumes)

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        fake_api.stop("preview")
        cancel_scope = anyio.CancelScope()
        entry_done = anyio.Event()

        async def enter() -> None:
            with cancel_scope:
                try:
                    async with box.session():
                        raise AssertionError("cancelled entry must not yield")
                finally:
                    entry_done.set()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(enter)
            await resume.wait_entered()
            cancel_scope.cancel()
            await entry_done.wait()
            resume.release()
        assert box.current_session_id == "sbx_1"

    assert not fake_api.requests_to("sessions.stop")


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_session_cleanup_finishes_before_cancellation_propagates(
    fake_api: FakeSandboxApi, anyio_backend: str
) -> None:
    fake_api.seed()
    stop = fake_api.hold("sessions.stop")

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        cancel_scope = anyio.CancelScope()
        entered = anyio.Event()
        managed_done = anyio.Event()

        async def managed() -> None:
            with cancel_scope:
                try:
                    async with box.session():
                        entered.set()
                        await anyio.sleep_forever()
                finally:
                    managed_done.set()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(managed)
            await entered.wait()
            cancel_scope.cancel()
            await stop.wait_entered()
            assert not managed_done.is_set()
            stop.release()

    assert _stops(fake_api) == ["sbx_1"]
    assert fake_api.current_session("preview").status == "stopped"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_cancellation_with_a_cleanup_failure_warns(
    fake_api: FakeSandboxApi, anyio_backend: str
) -> None:
    fake_api.seed()
    fake_api.fail("sessions.stop", status=503, code="stop_failed")

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        cancel_scope = anyio.CancelScope()
        entered = anyio.Event()

        async def managed() -> None:
            with cancel_scope:
                async with box.session():
                    entered.set()
                    await anyio.sleep_forever()

        with pytest.warns(RuntimeWarning) as warnings_info:
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(managed)
                await entered.wait()
                cancel_scope.cancel()

    cleanup_error = warnings_info[0].source
    assert isinstance(cleanup_error, SandboxCleanupError)
    assert cleanup_error.resource_id == "sbx_1"
    assert isinstance(cleanup_error.cause, SandboxApiError)
    assert cleanup_error.cause.code == "stop_failed"


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
async def test_acquisition_joins_an_implicit_resume_in_flight(
    fake_api: FakeSandboxApi, anyio_backend: str
) -> None:
    fake_api.seed()
    resume = fake_api.hold("sandboxes.get", when=_resumes)

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        fake_api.stop("preview")
        acquired: sandbox.SandboxRuntimeSession | None = None

        async def acquire() -> None:
            nonlocal acquired
            acquired = await box.session()

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(box.create_process, "true")
            await resume.wait_entered()
            task_group.start_soon(acquire)
            await anyio.sleep(0.01)
            resume.release()

    assert acquired is not None
    assert acquired is box.current_session
    assert acquired.id == "sbx_2"
    assert len(fake_api.requests_to("sandboxes.get", when=_resumes)) == 1


def test_sync_acquisition_joins_an_implicit_resume_in_flight(fake_api: FakeSandboxApi) -> None:
    fake_api.seed()
    resume = fake_api.hold("sandboxes.get", when=_resumes)
    acquire_joined = Event()

    class ObservedCondition(Condition):
        def wait(self, timeout: float | None = None) -> bool:
            acquire_joined.set()
            return super().wait(timeout)

    with fake_api.session(sync=True):
        box = sandbox_sync.get_sandbox(name="preview")
        fake_api.stop("preview")
        box._recovery_condition = ObservedCondition()
        with ThreadPoolExecutor(max_workers=2) as executor:
            implicit = executor.submit(box.create_process, "true")
            resume.wait_entered_sync()
            explicit = executor.submit(box.session)
            assert acquire_joined.wait(timeout=5)
            resume.release()
            implicit.result(timeout=5)
            acquired = explicit.result(timeout=5)

    assert acquired is box.current_session
    assert acquired.id == "sbx_2"
    assert len(fake_api.requests_to("sandboxes.get", when=_resumes)) == 1


async def test_managed_exit_does_not_wait_for_a_racing_recovery(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed()

    async with mode.session():
        with ThreadPoolExecutor(max_workers=1) as executor:
            async with mode.enter(mode.sandbox.resume_sandbox(name="preview")) as box:
                fake_api.stop("preview")
                resume = fake_api.hold("sandboxes.get", when=_resumes)
                if mode.sync:
                    operation: Any = executor.submit(box.create_process, "true")
                    resume.wait_entered_sync()
                else:
                    operation = asyncio.create_task(box.create_process("true"))
                    await resume.wait_entered()
            assert _stops(fake_api) == ["sbx_1"]
            resume.release()
            if mode.sync:
                operation.result(timeout=5)
            else:
                await operation

    assert box.current_session_id == "sbx_2"
    assert box.current_session is not None
    assert box.current_session.status is SandboxStatus.RUNNING
    assert _stops(fake_api) == ["sbx_1"]


async def test_a_stop_racing_recovery_leaves_the_replacement_running(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed()
    fake_api.respond(
        "sandboxes.get",
        lambda _request: httpx.Response(
            200,
            json={
                "sandbox": fake_api.sandbox("preview").payload(fake_api.current_session("preview"))
            },
        ),
    )
    resume = fake_api.hold("sandboxes.get", when=_resumes)
    stop = fake_api.hold("sessions.stop")

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        assert box.current_session is None
        fake_api.stop("preview")
        operation = asyncio.create_task(box.create_process("true"))
        await resume.wait_entered()
        stopping = asyncio.create_task(box.stop())
        await stop.wait_entered()
        resume.release()
        await fake_api.wait_for_requests(
            "commands.create", when=lambda request: request.path_params["session_id"] == "sbx_2"
        )
        stop.release()
        await operation
        assert await stopping is box

    assert _stops(fake_api) == ["sbx_1"]
    assert box.current_session_id == "sbx_2"
    assert box.current_session is not None
    assert box.current_session.status is SandboxStatus.RUNNING
