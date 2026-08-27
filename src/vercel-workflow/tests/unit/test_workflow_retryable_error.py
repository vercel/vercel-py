"""``RetryableError`` and when a failed step's next attempt runs.

Three layers have to agree for the delay to hold, and there is a section of
tests for each:

* the error resolves ``retry_after`` to an absolute time when it is
  constructed, accepting what ``sleep()`` accepts;
* the step path writes that time onto the ``step_retrying`` event and asks the
  queue for a matching delay;
* ``LocalWorld`` stores it on the step and then rejects a ``step_started``
  that arrives before it with ``TooEarlyError``. That is the layer that holds
  the delay against a delivery the queue continuation did not schedule.

The last group also pins the limit of the feature: an error asking to be
retried does not add an attempt, so a step that has used up ``max_retries``
still fails.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pytest

from vercel._internal.core.polyfills import UTC
from vercel.workflow import RetryableError
from vercel.workflow._internal import core, runtime, serialization as ser, world as w
from vercel.workflow._internal.worlds import local as local_mod

from ..world_stubs import NoStreams

NOW = datetime(2026, 1, 1, tzinfo=UTC)
RUN_ID = "wrun_retryable"
STEP_ID = "step_retryable"
WORKFLOW_NAME = "workflow//tests.wf"
WORKFLOW_QUEUE = f"__wkf_workflow_{WORKFLOW_NAME}"


# ── the error ──────────────────────────────────────────────────────────────


def test_retry_after_defaults_to_a_second() -> None:
    before = datetime.now(UTC)
    err = RetryableError("later")

    assert timedelta(seconds=1) <= err.retry_at - before <= timedelta(seconds=2)


@pytest.mark.parametrize(
    "retry_after,expected",
    [
        ("10s", timedelta(seconds=10)),
        (1_500, timedelta(milliseconds=1_500)),
        (timedelta(seconds=10), timedelta(seconds=10)),
    ],
)
def test_retry_after_takes_the_durations_sleep_takes(
    retry_after: str | int | timedelta, expected: timedelta
) -> None:
    before = datetime.now(UTC)
    err = RetryableError("later", retry_after=retry_after)

    assert expected <= err.retry_at - before <= expected + timedelta(seconds=1)


def test_retry_after_takes_an_absolute_datetime() -> None:
    deadline = datetime(2030, 6, 1, tzinfo=UTC)

    assert RetryableError("later", retry_after=deadline).retry_at == deadline


def test_retry_after_rejects_a_naive_datetime() -> None:
    with pytest.raises(RuntimeError, match="tzinfo"):
        RetryableError("later", retry_after=datetime(2030, 6, 1))


def test_retry_after_rejects_a_negative_timedelta() -> None:
    with pytest.raises(RuntimeError, match="non-negative"):
        RetryableError("later", retry_after=timedelta(seconds=-1))


# ── the step path ──────────────────────────────────────────────────────────


def _running_step(step_name: str, *, attempt: int) -> w.WorkflowStep:
    return w.NonFinalWorkflowStep(
        run_id=RUN_ID,
        step_id=STEP_ID,
        step_name=step_name,
        status="running",
        attempt=attempt,
        created_at=NOW,
        updated_at=NOW,
        started_at=NOW,
        input=ser.dehydrate(ser.step_arguments((), {})),
    )


class FakeWorld(NoStreams, w.World):
    """In-memory world recording what the step path writes and enqueues."""

    def __init__(self, *, started_step: w.WorkflowStep) -> None:
        self.started_step = started_step
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
        raise NotImplementedError

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        raise NotImplementedError

    async def events_list(self, run_id: str, *, pagination: Any = None) -> Any:
        raise NotImplementedError

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        if data.event_type == "step_started":
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
    payload = w.WorkflowInvokePayload(run_id=RUN_ID, step_id=STEP_ID, step_name=step_name)
    return await runtime.workflow_handler(
        payload.model_dump(by_alias=True),
        attempt=1,
        queue_name=WORKFLOW_QUEUE,
        message_id="msg_1",
        registry=registry,
    )


async def test_retryable_error_carries_its_deadline_to_the_event(
    registry: core.Workflows,
) -> None:
    @registry.step
    async def my_step() -> str:
        raise RetryableError("rate limited", retry_after="10s")

    fake = FakeWorld(started_step=_running_step(my_step.name, attempt=1))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert [e.event_type for e in fake.events] == ["step_retrying"]
    retry_after = fake.events[0].event_data.retry_after
    assert retry_after is not None
    assert timedelta(seconds=9) <= retry_after - datetime.now(UTC) <= timedelta(seconds=10)
    # The continuation asks the queue for the same wait, rounded up. Bounded
    # rather than exact because it is measured against the clock: the second
    # spent getting here counts against the ten.
    assert result is not None
    assert 9 <= result.delay_seconds <= 10


async def test_a_plain_error_keeps_the_one_second_retry(registry: core.Workflows) -> None:
    """The other side of the branch above. A step that did not ask for a delay
    writes no retryAfter, so the World has nothing to hold its next attempt
    back with and the retry goes out a second later as it always has."""

    @registry.step
    async def my_step() -> str:
        raise RuntimeError("boom")

    fake = FakeWorld(started_step=_running_step(my_step.name, attempt=1))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert fake.events[0].event_data.retry_after is None
    assert result == w.QueueContinuation(delay_seconds=1.0)


async def test_a_deadline_already_past_still_defers_a_second(registry: core.Workflows) -> None:
    """A deadline in the past leaves a negative wait, and a queue cannot be
    asked to deliver in zero or minus one second."""

    @registry.step
    async def my_step() -> str:
        raise RetryableError("late", retry_after=datetime(2020, 1, 1, tzinfo=UTC))

    fake = FakeWorld(started_step=_running_step(my_step.name, attempt=1))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert result == w.QueueContinuation(delay_seconds=1.0)


async def test_retryable_error_does_not_add_an_attempt(registry: core.Workflows) -> None:
    """Asking for a later retry is not asking for another one: a step out of
    attempts fails, and the deadline it named is ignored."""

    @registry.step(max_retries=1)
    async def my_step() -> str:
        raise RetryableError("still failing", retry_after="10s")

    # attempt 2 is max_retries(1) + 1: the last one.
    fake = FakeWorld(started_step=_running_step(my_step.name, attempt=2))
    w.set_world(fake)

    result = await _invoke(registry, my_step.name)

    assert [e.event_type for e in fake.events] == ["step_failed"]
    assert result is None


# ── the local world ────────────────────────────────────────────────────────


def _local_world(tmp_path, monkeypatch) -> local_mod.LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return local_mod.LocalWorld()


async def _run_with_started_step(world: local_mod.LocalWorld) -> str:
    """A run whose one step is on its first attempt, ready to be retried."""
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deployment_id="dpl_1",
            workflow_name=WORKFLOW_NAME,
            input=ser.dehydrate(ser.argument_array((), {})),
        ).into_event(),
    )
    assert created.run is not None
    run_id = created.run.run_id
    await world.events_create(
        run_id,
        w.StepCreatedEventData(
            step_name="my_step", input=ser.dehydrate(ser.step_arguments((), {}))
        ).into_event(STEP_ID),
    )
    await world.events_create(run_id, w.StepStartedEvent(correlation_id=STEP_ID))
    return run_id


async def test_local_world_parks_the_step_until_its_deadline(tmp_path, monkeypatch) -> None:
    world = _local_world(tmp_path, monkeypatch)
    run_id = await _run_with_started_step(world)
    # Truncated to milliseconds: the local world's on-disk format is the
    # JavaScript ISO string, which is what a `world-local` peer reads back.
    now = datetime.now(UTC)
    retry_after = (now + timedelta(seconds=30)).replace(microsecond=now.microsecond // 1000 * 1000)

    await world.events_create(
        run_id,
        w.StepRetryingEventData(
            error="boom", stack="Traceback...", retry_after=retry_after
        ).into_event(STEP_ID),
    )

    step = await world.steps_get(run_id, STEP_ID)
    assert step.status == "pending"
    assert step.retry_after == retry_after
    assert step.error is not None and step.error.message == "boom"
    # The first attempt's start is kept, not overwritten, which is what makes
    # StepInfo.step_started_at measure the whole step.
    assert step.started_at is not None

    with pytest.raises(w.TooEarlyError) as excinfo:
        await world.events_create(run_id, w.StepStartedEvent(correlation_id=STEP_ID))
    # Seconds left until the deadline, rounded up, for the caller to defer by.
    assert excinfo.value.retry_after is not None
    assert 29 <= excinfo.value.retry_after <= 30


async def test_a_deadline_past_the_queues_limit_reports_all_of_it(tmp_path, monkeypatch) -> None:
    """The World reports the whole remaining wait, however long it is.

    A queue will not hold a message for 30 days -- it caps a delay at 23h and
    delivers early. What makes the deadline hold anyway is this number: the
    step path defers again by whatever is still left, so a long wait chains in
    23h hops the way a long sleep does.
    """
    world = _local_world(tmp_path, monkeypatch)
    run_id = await _run_with_started_step(world)
    thirty_days = timedelta(days=30)

    await world.events_create(
        run_id,
        w.StepRetryingEventData(
            error="rate limited", retry_after=datetime.now(UTC) + thirty_days
        ).into_event(STEP_ID),
    )

    with pytest.raises(w.TooEarlyError) as excinfo:
        await world.events_create(run_id, w.StepStartedEvent(correlation_id=STEP_ID))
    assert excinfo.value.retry_after == pytest.approx(thirty_days.total_seconds(), abs=2)


async def test_local_world_starts_the_step_once_the_deadline_passed(tmp_path, monkeypatch) -> None:
    world = _local_world(tmp_path, monkeypatch)
    run_id = await _run_with_started_step(world)

    await world.events_create(
        run_id,
        w.StepRetryingEventData(
            error="boom", retry_after=datetime.now(UTC) - timedelta(seconds=1)
        ).into_event(STEP_ID),
    )
    result = await world.events_create(run_id, w.StepStartedEvent(correlation_id=STEP_ID))

    assert result.step is not None
    assert result.step.status == "running"
    assert result.step.attempt == 2
    # Cleared by the start, so a crash before the next step_retrying does not
    # leave the step parked forever.
    assert result.step.retry_after is None
