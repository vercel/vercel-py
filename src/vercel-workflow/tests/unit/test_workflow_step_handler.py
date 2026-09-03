"""Tests for the step path's run context and too-early/terminal paths.

Steps arrive on the workflow topic as a ``WorkflowInvokePayload`` carrying a
``stepId``, and ``workflow_handler`` dispatches those to its step path instead
of replaying. The handler reads the parent run for its format policy, then
issues ``step_started`` and lets the world surface state as typed errors —
``TooEarlyError`` (retryAfter not reached, HTTP 425) and
``EntityConflictError`` (terminal step, HTTP 409) — without pre-reading the
step. A too-early step defers via a queue timeout; a terminal step re-enqueues
the parent workflow and acks.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import httpx
import pydantic
import pytest
import respx

from tests.payloads import PLAIN_ENCODER
from vercel._internal.core.polyfills import UTC
from vercel.workflow import FatalError, StepNotRegisteredError
from vercel.workflow._internal import core, runtime, serialization as ser, world as w

from ..world_stubs import NoStreams


class Order(pydantic.BaseModel):
    sku: str


NOW = datetime(2026, 1, 1, tzinfo=UTC)
RUN_ID = "wrun_test"
STEP_ID = "step_test"
WORKFLOW_NAME = "workflow//tests.wf"


def _running_step(
    step_name: str,
    *,
    attempt: int,
    input: bytes | None = None,
) -> w.WorkflowStep:
    return w.NonFinalWorkflowStep(
        run_id=RUN_ID,
        step_id=STEP_ID,
        step_name=step_name,
        status="running",
        attempt=attempt,
        created_at=NOW,
        updated_at=NOW,
        started_at=NOW,
        input=input if input is not None else PLAIN_ENCODER.encode(ser.step_arguments((), {})),
    )


def _running_run(*, spec_version: int | None = w.SPEC_VERSION_CURRENT) -> w.WorkflowRun:
    return w.NonFinalWorkflowRun(
        run_id=RUN_ID,
        status="running",
        deployment_id="dpl_test",
        workflow_name=WORKFLOW_NAME,
        spec_version=spec_version,
        created_at=NOW,
        updated_at=NOW,
    )


class FakeWorld(NoStreams, w.World):
    """In-memory world driving the handler's step path.

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
        run_spec_version: int | None = w.SPEC_VERSION_CURRENT,
    ) -> None:
        self.step = step
        self.started_step = started_step
        self.start_error = start_error
        self.run = _running_run(spec_version=run_spec_version)
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
        assert run_id == RUN_ID
        return self.run

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


WORKFLOW_QUEUE = f"__wkf_workflow_{WORKFLOW_NAME}"


async def _invoke(registry: core.Workflows, step_name: str) -> w.QueueContinuation | None:
    payload = w.WorkflowInvokePayload(
        run_id=RUN_ID,
        step_id=STEP_ID,
        step_name=step_name,
    )
    return await runtime.workflow_handler(
        payload.model_dump(by_alias=True),
        attempt=1,
        queue_name=WORKFLOW_QUEUE,
        message_id="msg_1",
        registry=registry,
    )


def _event_types(fake: FakeWorld) -> list[str]:
    return [e.event_type for e in fake.events]


def _workflow_enqueues(fake: FakeWorld) -> list[tuple[str, Any]]:
    return [q for q in fake.queued if q[0] == WORKFLOW_QUEUE]


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


@pytest.mark.parametrize(
    ("spec_version", "prefix"),
    [
        (w.SPEC_VERSION_SUPPORTS_COMPRESSION - 1, ser.DEVALUE_V1),
        (w.SPEC_VERSION_SUPPORTS_COMPRESSION, ser.ZSTD),
    ],
)
async def test_step_output_compression_follows_the_run_version(
    registry: core.Workflows, spec_version: int, prefix: bytes
) -> None:
    @registry.step
    async def my_step() -> str:
        return "charged " * 256

    fake = FakeWorld(
        started_step=_running_step(my_step.name, attempt=1),
        run_spec_version=spec_version,
    )
    w.set_world(fake)

    await _invoke(registry, my_step.name)

    (completed,) = fake.events
    assert isinstance(completed, w.StepCompletedEvent)
    assert completed.event_data.result.startswith(prefix)


async def test_unregistered_step_fails_without_retrying(registry: core.Workflows) -> None:
    step_name = "step//tests.non_existent_step"
    fake = FakeWorld(started_step=_running_step(step_name, attempt=1))
    w.set_world(fake)

    result = await _invoke(registry, step_name)

    assert result is None
    assert _event_types(fake) == ["step_failed"]
    assert len(_workflow_enqueues(fake)) == 1
    (failed,) = fake.events
    recorded = ser.hydrate_error(failed.event_data.error, what="the recorded error")
    assert isinstance(recorded, FatalError)
    assert "is not registered with this Workflows instance" in str(recorded)


def test_missing_step_lookup_raises_named_error(registry: core.Workflows) -> None:
    step_name = "step//tests.non_existent_step"

    with pytest.raises(StepNotRegisteredError) as exc_info:
        registry._get_step(step_name)

    assert exc_info.value.step_name == step_name
    assert isinstance(exc_info.value, FatalError)
    assert str(exc_info.value) == (
        f'Step "{step_name}" is not registered with this Workflows instance. '
        "Ensure the module defining it is imported and registers the step "
        "when this deployment starts."
    )


async def test_an_ordinary_failure_below_max_retries_asks_for_a_retry(
    registry: core.Workflows,
) -> None:
    @registry.step
    async def my_step() -> str:
        raise RuntimeError("flaky")

    fake = FakeWorld(started_step=_running_step(my_step.name, attempt=1))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert result == w.QueueContinuation(delay_seconds=1.0)
    assert _event_types(fake) == ["step_retrying"]
    assert _workflow_enqueues(fake) == []
    (retrying,) = fake.events
    recorded = ser.hydrate_error(retrying.event_data.error, what="the recorded error")
    assert isinstance(recorded, RuntimeError)
    assert str(recorded) == "flaky"


async def test_keyboard_interrupt_bypasses_step_error_serialization(
    registry: core.Workflows,
) -> None:
    @registry.step
    async def interrupted_step() -> None:
        raise KeyboardInterrupt

    fake = FakeWorld(started_step=_running_step(interrupted_step.name, attempt=1))
    w.set_world(fake)

    with pytest.raises(KeyboardInterrupt):
        await _invoke(registry, interrupted_step.name)

    assert fake.events == []
    assert fake.queued == []


async def test_the_last_attempt_wraps_the_thrown_error_in_a_fatal_one(
    registry: core.Workflows,
) -> None:
    @registry.step
    async def my_step() -> str:
        raise RuntimeError("still flaky")

    my_step.max_retries = 1
    fake = FakeWorld(started_step=_running_step(my_step.name, attempt=2))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert result is None
    assert _event_types(fake) == ["step_failed"]
    (failed,) = fake.events
    recorded = ser.hydrate_error(failed.event_data.error, what="the recorded error")
    assert isinstance(recorded, FatalError)
    assert str(recorded) == f"Step '{my_step.name}' failed after 1 retry: RuntimeError: still flaky"
    assert isinstance(recorded.__cause__, RuntimeError)
    assert str(recorded.__cause__) == "still flaky"
    assert "RuntimeError: still flaky" in recorded.stack  # type: ignore[attr-defined]


async def test_a_fatal_failure_gives_up_on_the_first_attempt(registry: core.Workflows) -> None:
    """`FatalError` skips the remaining attempts.

    The same call would be replayed to reach the same place, so the handler
    records `step_failed` and lets the workflow observe it instead.
    """
    attempts = 0

    @registry.step
    async def my_step() -> str:
        nonlocal attempts
        attempts += 1
        raise FatalError("card declined")

    fake = FakeWorld(started_step=_running_step(my_step.name, attempt=1))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert result is None
    assert attempts == 1
    assert _event_types(fake) == ["step_failed"]
    assert len(_workflow_enqueues(fake)) == 1
    (failed,) = fake.events
    recorded = ser.hydrate_error(failed.event_data.error, what="the recorded error")
    assert isinstance(recorded, FatalError)
    assert str(recorded) == "card declined"
    assert recorded.__cause__ is None


async def test_input_that_does_not_match_the_annotation_is_fatal(
    registry: core.Workflows,
) -> None:
    """The recorded bytes are what did not match, and a retry replays them."""
    ran = False

    @registry.step
    async def my_step(order: Order) -> str:
        nonlocal ran
        ran = True
        return "ok"

    fake = FakeWorld(
        started_step=_running_step(
            my_step.name,
            attempt=1,
            input=PLAIN_ENCODER.encode(ser.step_arguments((), {"order": {"nope": 1}})),
        )
    )
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert result is None
    assert ran is False
    assert _event_types(fake) == ["step_failed"]
    recorded = ser.hydrate_error(fake.events[0].event_data.error, what="the recorded error")
    assert isinstance(recorded, FatalError)
    assert "does not match" in str(recorded)


async def test_local_world_step_started_too_early_raises(tmp_path, monkeypatch) -> None:
    """LocalWorld surfaces a future retryAfter as TooEarlyError (the 425 analog),
    carrying the seconds remaining — not a bare RuntimeError — so the handler's
    `except TooEarlyError` defers locally the same way it does against prod."""
    from vercel.workflow._internal.worlds import local as local_mod

    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()

    future = datetime.now(UTC) + timedelta(seconds=30)
    step = w.NonFinalWorkflowStep(
        run_id=RUN_ID,
        step_id=STEP_ID,
        step_name="step//tests.my_step",
        status="pending",
        attempt=0,
        created_at=NOW,
        updated_at=NOW,
        retry_after=future,
        input=PLAIN_ENCODER.encode(ser.step_arguments((), {})),
    )
    local_mod.write_json(world.data_dir / "steps" / f"{RUN_ID}-{STEP_ID}.json", step.model_dump())

    with pytest.raises(w.TooEarlyError) as ei:
        await world.events_create(RUN_ID, w.StepStartedEvent(correlation_id=STEP_ID))

    assert ei.value.retry_after is not None
    assert 1 <= ei.value.retry_after <= 30


async def test_vercel_world_maps_425_to_too_early() -> None:
    """VercelWorld maps an HTTP 425 to TooEarlyError, reading the seconds from
    the Retry-After header (exercising the shared response mapping)."""
    from vercel.workflow._internal.worlds import vercel as vercel_mod

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
