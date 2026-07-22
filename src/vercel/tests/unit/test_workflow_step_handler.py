"""Tests for step_handler's start-first control flow and the too-early/terminal paths.

step_handler mirrors the upstream JS step-handler: it issues ``step_started``
first and lets the world surface state as typed errors — ``TooEarlyError``
(retryAfter not reached, HTTP 425) and ``EntityConflictError`` (terminal step,
HTTP 409) — instead of pre-reading the step. A too-early step defers via a queue
timeout; a terminal step re-enqueues the parent workflow and acks.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import httpx
import pytest
import respx

from vercel._internal.polyfills import UTC
from vercel._internal.workflow import core, runtime, world as w

NOW = datetime(2026, 1, 1, tzinfo=UTC)
RUN_ID = "wrun_test"
STEP_ID = "step_test"
WORKFLOW_NAME = "workflow//tests.wf"


def _running_step(step_name: str, *, attempt: int) -> w.WorkflowStep:
    return w.NonFinalWorkflowStep(
        runId=RUN_ID,
        stepId=STEP_ID,
        stepName=step_name,
        status="running",
        attempt=attempt,
        createdAt=NOW,
        updatedAt=NOW,
        startedAt=NOW,
        input=[b"json[[], {}]"],
    )


class FakeWorld(w.World):
    """In-memory world driving step_handler.

    ``step`` is the persisted step ``steps_get`` returns (the pre-read snapshot).
    ``step_started`` then raises ``start_error`` if set — modelling the step's
    state changing between the read and the write — otherwise returns
    ``started_step``.
    """

    def __init__(
        self,
        *,
        step: w.WorkflowStep | None = None,
        started_step: w.WorkflowStep | None = None,
        start_error: Exception | None = None,
    ) -> None:
        self.step = step
        self.started_step = started_step
        self.start_error = start_error
        self.queued: list[tuple[str, Any]] = []
        self.events: list[Any] = []

    async def get_deployment_id(self) -> str:
        return ""

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        self.queued.append((queue_name, message))
        return "msg_fake"

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        raise NotImplementedError

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        raise NotImplementedError

    async def steps_get(self, run_id: str, step_id: str) -> w.WorkflowStep:
        assert self.step is not None, "test did not set a persisted step"
        return self.step

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        raise NotImplementedError

    async def events_list(self, run_id: str, *, pagination: Any = None) -> Any:
        raise NotImplementedError

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        if data.event_type == "step_started":
            if self.start_error is not None:
                raise self.start_error
            return w.EventResult(step=self.started_step)
        self.events.append(data)
        return w.EventResult()


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


@pytest.fixture
def registry() -> core.Workflows:
    return core.Workflows(as_vercel_job=False)


async def _invoke(registry: core.Workflows, step_name: str) -> w.QueueContinuation | None:
    payload = w.StepInvokePayload(
        workflowName=WORKFLOW_NAME,
        workflowRunId=RUN_ID,
        workflowStartedAt=0.0,
        stepId=STEP_ID,
    )
    return await runtime.step_handler(
        payload.model_dump(by_alias=True),
        attempt=1,
        queue_name=f"__wkf_step_{step_name}",
        message_id="msg_1",
        registry=registry,
    )


def _event_types(fake: FakeWorld) -> list[str]:
    return [e.event_type for e in fake.events]


def _workflow_enqueues(fake: FakeWorld) -> list[tuple[str, Any]]:
    return [q for q in fake.queued if q[0] == f"__wkf_workflow_{WORKFLOW_NAME}"]


async def test_too_early_defers_without_running(registry: core.Workflows) -> None:
    """A step_started TooEarlyError (retryAfter not reached) defers via a queue
    timeout sized to retry_after, and the body never runs."""
    ran = False

    @registry.step
    async def my_step() -> str:
        nonlocal ran
        ran = True
        return "ok"

    fake = FakeWorld(start_error=w.TooEarlyError("too early", retry_after=42))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert result == w.QueueContinuation(delay_seconds=42)
    assert ran is False
    assert fake.events == []
    assert fake.queued == []


async def test_too_early_without_retry_after_defaults_to_one(registry: core.Workflows) -> None:
    @registry.step
    async def my_step() -> str:
        return "ok"

    fake = FakeWorld(start_error=w.TooEarlyError("too early"))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert result == w.QueueContinuation(delay_seconds=1)


async def test_step_started_conflict_reenqueues_workflow_and_acks(registry: core.Workflows) -> None:
    """Read-then-write race: steps_get sees the step running, but a concurrent worker
    drives it to a terminal state before this delivery's step_started lands, so
    step_started conflicts. The handler must re-enqueue the parent workflow and ack.

    The old handler returned here without re-enqueueing, relying on the concurrent
    worker to do it — but that worker can crash after writing the terminal event and
    before re-enqueueing, hanging the run. Re-enqueueing from the conflict path makes
    this delivery a reliable backstop.
    """
    ran = False

    @registry.step
    async def my_step() -> str:
        nonlocal ran
        ran = True
        return "ok"

    # steps_get returns a running step; step_started then conflicts because the step
    # reached a terminal state between the read and the write.
    fake = FakeWorld(
        step=_running_step(my_step.name, attempt=1),
        start_error=w.EntityConflictError('Cannot modify step in terminal state "completed"'),
    )
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert result is None
    assert ran is False
    assert fake.events == []
    enqueues = _workflow_enqueues(fake)
    assert len(enqueues) == 1
    assert enqueues[0][1].run_id == RUN_ID


async def test_max_retries_checked_after_start(registry: core.Workflows) -> None:
    """The max-retries guard runs on the attempt returned by step_started. A
    step whose incremented attempt exceeds max_retries + 1 is failed and the
    workflow re-enqueued — the body never runs."""
    ran = False

    @registry.step
    async def my_step() -> str:
        nonlocal ran
        ran = True
        return "ok"

    my_step.max_retries = 0
    # step_started returns attempt=2 > max_retries(0) + 1
    fake = FakeWorld(started_step=_running_step(my_step.name, attempt=2))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert result is None
    assert ran is False
    assert _event_types(fake) == ["step_failed"]
    assert len(_workflow_enqueues(fake)) == 1


async def test_happy_path_completes_and_reenqueues(registry: core.Workflows) -> None:
    @registry.step
    async def my_step() -> str:
        return "ok"

    fake = FakeWorld(started_step=_running_step(my_step.name, attempt=1))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert result is None
    assert _event_types(fake) == ["step_completed"]
    assert len(_workflow_enqueues(fake)) == 1


async def test_local_world_step_started_too_early_raises(tmp_path, monkeypatch) -> None:
    """LocalWorld surfaces a future retryAfter as TooEarlyError (the 425 analog),
    carrying the seconds remaining — not a bare RuntimeError — so the handler's
    `except TooEarlyError` defers locally the same way it does against prod."""
    from vercel._internal.workflow.worlds import local as local_mod

    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()

    future = datetime.now(UTC) + timedelta(seconds=30)
    step = w.NonFinalWorkflowStep(
        runId=RUN_ID,
        stepId=STEP_ID,
        stepName="step//tests.my_step",
        status="pending",
        attempt=0,
        createdAt=NOW,
        updatedAt=NOW,
        retryAfter=future,
        input=[b"json[[], {}]"],
    )
    local_mod.write_json(world.data_dir / "steps" / f"{RUN_ID}-{STEP_ID}.json", step.model_dump())

    with pytest.raises(w.TooEarlyError) as ei:
        await world.events_create(RUN_ID, w.StepStartedEvent(correlationId=STEP_ID))

    assert ei.value.retry_after is not None
    assert 1 <= ei.value.retry_after <= 30


async def test_vercel_world_maps_425_to_too_early() -> None:
    """VercelWorld maps an HTTP 425 to TooEarlyError, reading the seconds from
    the Retry-After header (exercising the shared response mapping)."""
    from vercel._internal.workflow.worlds import vercel as vercel_mod

    world = vercel_mod.VercelWorld(token="tok")

    with respx.mock:
        respx.route(method="POST").mock(
            return_value=httpx.Response(
                425, headers={"Retry-After": "17"}, json={"message": "too early"}
            )
        )
        with pytest.raises(w.TooEarlyError) as ei:
            await world._cbor_request("POST", "/test", schema=w.EventResult, data={"x": 1})

    assert ei.value.retry_after == 17
    assert "too early" in str(ei.value)
