"""Cancellable steps.

Cancelling the task awaiting a step declared ``cancellable=True`` does not
cancel the await: like ``Task.cancel()``, it is a request. The orchestrator
records it as a system hook -- ``hook_created`` with an ``abrt_<step-ulid>``
token, then ``hook_received`` -- and writes one chunk to a
``strm_<step-ulid>_system_abort`` control stream that the running step listens
on. The step shuts down with a fatal ``step_failed``, and the awaiting body
receives that error (or the step's result, if it finished first). Replaying
the ``hook_received`` clears the pending cancellation so nothing is re-sent,
and a body that exits with a cancellable step still pending cancels it on the
way out.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any, TypeVar

import pytest

from vercel.workflow._internal import (
    core,
    errors,
    runtime,
    serialization as ser,
    streams,
    world as w,
)
from vercel.workflow._internal.worlds import local as local_mod

registry = core.Workflows(as_vercel_job=False)


# What slow_step's CancelledError carried; steps run outside the sandbox, so
# the module state is shared with the test.
_cancel_messages: list[str | None] = []


@registry.step(cancellable=True)
async def slow_step() -> str:
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError as e:
        _cancel_messages.append(str(e.args[0]) if e.args else None)
        raise
    return "never"


@registry.step
async def quick_step() -> str:
    return "quick"


@registry.step
async def second_step() -> str:
    return "second"


@registry.step(cancellable=True)
async def uncancelled_step() -> str:
    return "ran"


@registry.step(cancellable=True)
async def stubborn_step() -> str:
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        return "survived"
    return "never"


async def _cancel_slow_after_quick() -> str:
    """Cancel the slow step after the quick one, then await its real outcome."""
    slow = asyncio.ensure_future(slow_step())
    first = await quick_step()
    slow.cancel("no longer needed")
    try:
        outcome = await slow
    except asyncio.CancelledError as e:
        # The step's recorded death from our cancel arrives as a genuine
        # CancelledError, so it composes with normal asyncio idioms.
        outcome = f"cancelled: {e.args[0] if e.args else ''}"
    return f"{first}+{outcome}"


@registry.workflow
async def cancel_and_wait() -> str:
    first = await _cancel_slow_after_quick()
    second = await second_step()
    return f"{first}:{second}"


@registry.workflow
async def cancel_and_return() -> str:
    slow = asyncio.ensure_future(slow_step())
    first = await quick_step()
    slow.cancel("no longer needed")
    return first


@registry.workflow
async def abandon_step() -> str:
    asyncio.ensure_future(slow_step())
    return await quick_step()


@registry.workflow
async def cancel_twice() -> str:
    slow = asyncio.ensure_future(slow_step())
    first = await quick_step()
    slow.cancel("first request")
    second = await second_step()
    slow.cancel("second request")
    try:
        outcome = await slow
    except Exception as e:
        outcome = str(e)
    return f"{first}:{second}:{outcome}"


async def _await_shielded() -> str:
    return await asyncio.shield(slow_step())


@registry.workflow
async def cancel_shielded() -> str:
    wrapper = asyncio.ensure_future(_await_shielded())
    first = await quick_step()
    wrapper.cancel()
    second = await second_step()
    return f"{first}:{second}"


@registry.workflow
async def cancel_stubborn() -> str:
    stubborn = asyncio.ensure_future(stubborn_step())
    first = await quick_step()
    stubborn.cancel("please stop")
    outcome = await stubborn
    return f"{first}:{outcome}"


@registry.workflow
async def run_cancellable() -> str:
    return await uncancelled_step()


class RecordingLocalWorld(local_mod.LocalWorld):
    def __init__(self) -> None:
        super().__init__()
        self.queued: list[tuple[str, w.QueuePayload]] = []

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        self.queued.append((queue_name, message))
        return "msg_test"


def _world(tmp_path, monkeypatch) -> RecordingLocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = RecordingLocalWorld()
    w.set_world(world)
    return world


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


async def _create_run(world: local_mod.LocalWorld, workflow_name: str) -> str:
    result = await world.events_create(
        None,
        w.RunCreatedEventData(
            deployment_id="",
            workflow_name=workflow_name,
            input=ser.dehydrate(ser.argument_array((), {})),
        ).into_event(),
    )
    assert result.run is not None
    return result.run.run_id


async def _invoke(run_id: str, workflow_name: str) -> w.QueueContinuation | None:
    return await runtime.workflow_handler(
        w.WorkflowInvokePayload(run_id=run_id).model_dump(by_alias=True),
        attempt=1,
        queue_name=w.get_queue_name(workflow_name),
        message_id="msg_1",
        registry=registry,
    )


async def _invoke_step(
    payload: w.WorkflowInvokePayload, workflow_name: str
) -> w.QueueContinuation | None:
    return await runtime.workflow_handler(
        payload.model_dump(by_alias=True),
        attempt=1,
        queue_name=w.get_queue_name(workflow_name),
        message_id="msg_2",
        registry=registry,
    )


def _queued_step(world: RecordingLocalWorld, step_name: str) -> w.WorkflowInvokePayload:
    for _, payload in world.queued:
        if isinstance(payload, w.WorkflowInvokePayload) and payload.step_name == step_name:
            return payload
    raise AssertionError(f"no queued invocation for step {step_name!r}")


async def _events(world: local_mod.LocalWorld, run_id: str) -> list[w.Event]:
    return (await world.events_list(run_id)).data


E = TypeVar("E", bound=w.BaseEvent)


def _of_type(events: list[w.Event], event_cls: type[E]) -> list[E]:
    return [e for e in events if isinstance(e, event_cls)]


async def test_cancellation_records_hook_and_signals_stream(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_id = await _create_run(world, cancel_and_wait.workflow_id)

    # Pass 1: both steps issued, nothing cancelled yet. The suspension pass
    # cancels the body's tasks itself; that must not read as a user cancel.
    await _invoke(run_id, cancel_and_wait.workflow_id)
    events = await _events(world, run_id)
    assert not _of_type(events, w.HookCreatedEvent)
    slow_payload = _queued_step(world, slow_step.name)
    assert slow_payload.step_id is not None

    # Complete the quick step, then pass 2: the body cancels the slow step and
    # keeps awaiting it; the cancellation is recorded and signalled.
    await _invoke_step(_queued_step(world, quick_step.name), cancel_and_wait.workflow_id)
    await _invoke(run_id, cancel_and_wait.workflow_id)

    events = await _events(world, run_id)
    (created,) = _of_type(events, w.HookCreatedEvent)
    (received,) = _of_type(events, w.HookReceivedEvent)
    step_ulid = slow_payload.step_id.split("_", 1)[1]
    assert created.event_data.token == f"abrt_{step_ulid}"
    assert created.event_data.is_system is True
    assert received.correlation_id == created.correlation_id
    assert received.event_data.token == created.event_data.token
    payload = ser.hydrate(received.event_data.payload, what="the cancellation payload")
    assert payload == {"aborted": True, "reason": "no longer needed"}

    stream_name = runtime._abort_stream_name(slow_payload.step_id)
    info = await world.streams_get_info(run_id, stream_name)
    assert info.done
    assert info.tail_index == 0


async def test_redelivery_does_not_resend_the_cancellation(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_id = await _create_run(world, cancel_and_wait.workflow_id)

    await _invoke(run_id, cancel_and_wait.workflow_id)
    await _invoke_step(_queued_step(world, quick_step.name), cancel_and_wait.workflow_id)
    await _invoke(run_id, cancel_and_wait.workflow_id)
    # A redelivered invoke replays the recorded hook_received, which clears
    # the pending cancellation instead of re-sending it.
    await _invoke(run_id, cancel_and_wait.workflow_id)

    events = await _events(world, run_id)
    assert len(_of_type(events, w.HookCreatedEvent)) == 1
    assert len(_of_type(events, w.HookReceivedEvent)) == 1


class _FlakyStreamWorld(RecordingLocalWorld):
    fail_stream_writes = 0
    stream_write_status = 503

    async def streams_write(self, run_id: str, name: str, chunk: bytes) -> None:
        if self.fail_stream_writes:
            self.fail_stream_writes -= 1
            if self.stream_write_status == 429:
                raise w.ThrottleError("stream write throttled", status=429)
            raise w.WorkflowWorldError("stream write lost", status=self.stream_write_status)
        await super().streams_write(run_id, name, chunk)


async def test_lost_cancellation_signal_fails_the_pass_and_is_retried(
    tmp_path, monkeypatch
) -> None:
    """The stream packet is the only way a running step learns of the
    cancellation, so losing it must fail the pass -- and, since nothing
    durable was recorded yet, the redelivered pass re-sends everything."""
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = _FlakyStreamWorld()
    w.set_world(world)
    run_id = await _create_run(world, cancel_and_wait.workflow_id)

    await _invoke(run_id, cancel_and_wait.workflow_id)
    await _invoke_step(_queued_step(world, quick_step.name), cancel_and_wait.workflow_id)

    world.fail_stream_writes = 1
    with pytest.raises(Exception) as excinfo:
        await _invoke(run_id, cancel_and_wait.workflow_id)
    assert "stream write lost" in repr(excinfo.value)
    events = await _events(world, run_id)
    assert not _of_type(events, w.HookReceivedEvent)

    # The redelivered pass sends the signal and the events.
    await _invoke(run_id, cancel_and_wait.workflow_id)
    events = await _events(world, run_id)
    assert len(_of_type(events, w.HookReceivedEvent)) == 1
    slow_payload = _queued_step(world, slow_step.name)
    assert slow_payload.step_id is not None
    info = await world.streams_get_info(run_id, runtime._abort_stream_name(slow_payload.step_id))
    assert info.done


async def test_throttled_cancellation_signal_is_not_recorded(tmp_path, monkeypatch) -> None:
    """A rejected append must leave cancellation pending for redelivery."""
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = _FlakyStreamWorld()
    w.set_world(world)
    run_id = await _create_run(world, cancel_and_wait.workflow_id)

    await _invoke(run_id, cancel_and_wait.workflow_id)
    await _invoke_step(_queued_step(world, quick_step.name), cancel_and_wait.workflow_id)

    world.fail_stream_writes = 1
    world.stream_write_status = 429
    with pytest.raises(Exception) as excinfo:
        await _invoke(run_id, cancel_and_wait.workflow_id)
    assert "stream write throttled" in repr(excinfo.value)
    assert not _of_type(await _events(world, run_id), w.HookReceivedEvent)

    await _invoke(run_id, cancel_and_wait.workflow_id)
    assert len(_of_type(await _events(world, run_id), w.HookReceivedEvent)) == 1


async def test_cancelled_step_shuts_down_and_body_gets_its_error(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_id = await _create_run(world, cancel_and_wait.workflow_id)

    await _invoke(run_id, cancel_and_wait.workflow_id)
    await _invoke_step(_queued_step(world, quick_step.name), cancel_and_wait.workflow_id)
    await _invoke(run_id, cancel_and_wait.workflow_id)

    # The slow step would run for an hour; the abort-stream listener kills it.
    _cancel_messages.clear()
    slow_payload = _queued_step(world, slow_step.name)
    continuation = await _invoke_step(slow_payload, cancel_and_wait.workflow_id)

    assert continuation is None  # failed fatally, not queued for retry
    # The step function itself saw the body's message on its CancelledError.
    assert _cancel_messages == ["no longer needed"]
    events = await _events(world, run_id)
    assert not _of_type(events, w.StepRetryingEvent)
    (failed,) = _of_type(events, w.StepFailedEvent)
    assert failed.correlation_id == slow_payload.step_id
    assert "no longer needed" in str(failed.event_data.error)

    # The body was still awaiting the cancelled step, so it receives the
    # step's actual fatal error (not a CancelledError) and carries on.
    await _invoke(run_id, cancel_and_wait.workflow_id)
    await _invoke_step(_queued_step(world, second_step.name), cancel_and_wait.workflow_id)
    await _invoke(run_id, cancel_and_wait.workflow_id)
    assert (await world.runs_get(run_id)).status == "completed"
    # The body's own cancel message comes back verbatim on the CancelledError.
    assert await runtime.Run(run_id).return_value() == "quick+cancelled: no longer needed:second"


async def test_body_exit_with_cancelled_step_waits_for_it(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_id = await _create_run(world, cancel_and_return.workflow_id)

    await _invoke(run_id, cancel_and_return.workflow_id)
    await _invoke_step(_queued_step(world, quick_step.name), cancel_and_return.workflow_id)
    # Pass 2: the body cancels the slow step and returns without awaiting it.
    # The run does not complete yet: the cancellation is sent and the run
    # suspends until the step has actually finished.
    await _invoke(run_id, cancel_and_return.workflow_id)

    events = await _events(world, run_id)
    assert len(_of_type(events, w.HookReceivedEvent)) == 1
    assert (await world.runs_get(run_id)).status == "running"
    slow_payload = _queued_step(world, slow_step.name)
    assert slow_payload.step_id is not None
    info = await world.streams_get_info(run_id, runtime._abort_stream_name(slow_payload.step_id))
    assert info.done

    # The step dies from the signal; only then does the run complete.
    await _invoke_step(slow_payload, cancel_and_return.workflow_id)
    await _invoke(run_id, cancel_and_return.workflow_id)
    events = await _events(world, run_id)
    types = [e.event_type for e in events]
    assert types.index("step_failed") < types.index("run_completed")
    assert await runtime.Run(run_id).return_value() == "quick"


async def test_body_exit_with_outstanding_step_cancels_and_waits(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_id = await _create_run(world, abandon_step.workflow_id)

    await _invoke(run_id, abandon_step.workflow_id)
    await _invoke_step(_queued_step(world, quick_step.name), abandon_step.workflow_id)
    # Pass 2: the body returns while the slow step is still pending; the exit
    # itself cancels it (with no message) and the run suspends.
    await _invoke(run_id, abandon_step.workflow_id)

    events = await _events(world, run_id)
    (received,) = _of_type(events, w.HookReceivedEvent)
    payload = ser.hydrate(received.event_data.payload, what="the cancellation payload")
    assert payload == {"aborted": True, "reason": None}
    assert (await world.runs_get(run_id)).status == "running"

    slow_payload = _queued_step(world, slow_step.name)
    await _invoke_step(slow_payload, abandon_step.workflow_id)
    await _invoke(run_id, abandon_step.workflow_id)
    events = await _events(world, run_id)
    (failed,) = _of_type(events, w.StepFailedEvent)
    assert "step cancelled by its workflow" in str(failed.event_data.error)
    assert await runtime.Run(run_id).return_value() == "quick"


async def test_repeat_cancel_is_a_no_op(tmp_path, monkeypatch) -> None:
    """A step is cancelled at most once: a second cancel() -- even after the
    recorded cancellation has replayed -- sends nothing new. It could not
    reach the step anyway; the abort stream is closed and its listener gone."""
    world = _world(tmp_path, monkeypatch)
    run_id = await _create_run(world, cancel_twice.workflow_id)

    await _invoke(run_id, cancel_twice.workflow_id)
    await _invoke_step(_queued_step(world, quick_step.name), cancel_twice.workflow_id)
    # Pass 2: the first cancel is recorded and sent.
    await _invoke(run_id, cancel_twice.workflow_id)
    await _invoke_step(_queued_step(world, second_step.name), cancel_twice.workflow_id)
    # Pass 3: replay clears the first request, then the body cancels again.
    await _invoke(run_id, cancel_twice.workflow_id)

    events = await _events(world, run_id)
    assert len(_of_type(events, w.HookCreatedEvent)) == 1
    (received,) = _of_type(events, w.HookReceivedEvent)
    payload = ser.hydrate(received.event_data.payload, what="the cancellation payload")
    assert payload == {"aborted": True, "reason": "first request"}


async def test_shield_defers_cancellation_to_body_exit(tmp_path, monkeypatch) -> None:
    """Cancelling a task that awaits ``shield(step)`` kills the wrapper, not
    the step -- plain asyncio semantics. Only the body's exit, which cancels
    every leftover task the way asyncio.run() does, reaches the step itself."""
    world = _world(tmp_path, monkeypatch)
    run_id = await _create_run(world, cancel_shielded.workflow_id)

    await _invoke(run_id, cancel_shielded.workflow_id)
    await _invoke_step(_queued_step(world, quick_step.name), cancel_shielded.workflow_id)
    # Pass 2: the body cancels the shield wrapper and suspends on second_step;
    # the shielded slow step is NOT cancelled.
    await _invoke(run_id, cancel_shielded.workflow_id)
    events = await _events(world, run_id)
    assert not _of_type(events, w.HookCreatedEvent)

    # Pass 3: the body exits with the shielded step still pending -- now it is
    # cancelled, and the run waits for it.
    await _invoke_step(_queued_step(world, second_step.name), cancel_shielded.workflow_id)
    await _invoke(run_id, cancel_shielded.workflow_id)
    events = await _events(world, run_id)
    assert len(_of_type(events, w.HookReceivedEvent)) == 1
    assert (await world.runs_get(run_id)).status == "running"

    await _invoke_step(_queued_step(world, slow_step.name), cancel_shielded.workflow_id)
    await _invoke(run_id, cancel_shielded.workflow_id)
    assert await runtime.Run(run_id).return_value() == "quick:second"


async def test_step_that_survives_cancellation_keeps_its_result(tmp_path, monkeypatch) -> None:
    """A step that catches the cancellation and returns anyway completes with
    that value, and the awaiting body receives it -- the cancel was only ever
    a request."""
    world = _world(tmp_path, monkeypatch)
    run_id = await _create_run(world, cancel_stubborn.workflow_id)

    await _invoke(run_id, cancel_stubborn.workflow_id)
    await _invoke_step(_queued_step(world, quick_step.name), cancel_stubborn.workflow_id)
    await _invoke(run_id, cancel_stubborn.workflow_id)
    await _invoke_step(_queued_step(world, stubborn_step.name), cancel_stubborn.workflow_id)
    await _invoke(run_id, cancel_stubborn.workflow_id)

    events = await _events(world, run_id)
    assert len(_of_type(events, w.HookReceivedEvent)) == 1  # the cancel was sent
    assert not _of_type(events, w.StepFailedEvent)
    assert len(_of_type(events, w.StepCompletedEvent)) == 2
    assert await runtime.Run(run_id).return_value() == "quick:survived"


async def test_uncancelled_cancellable_step_runs_normally(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_id = await _create_run(world, run_cancellable.workflow_id)

    await _invoke(run_id, run_cancellable.workflow_id)
    # The listener finds no abort chunk and is torn down when the step
    # finishes; the step completes as usual.
    await _invoke_step(_queued_step(world, uncancelled_step.name), run_cancellable.workflow_id)
    await _invoke(run_id, run_cancellable.workflow_id)

    events = await _events(world, run_id)
    assert not _of_type(events, w.HookCreatedEvent)
    assert len(_of_type(events, w.StepCompletedEvent)) == 1
    assert await runtime.Run(run_id).return_value() == "ran"


class _AbortListenerWorld:
    def __init__(self, reason: str, *, break_once: bool = False, split: bool = False) -> None:
        payload = ser.dehydrate({"aborted": True, "reason": reason})
        self.wire = streams.encode_frame(payload)
        self.break_once = break_once
        self.split = split
        self.reads: list[int | None] = []

    def streams_get(
        self, run_id: str, name: str, start_index: int | None = None
    ) -> AsyncGenerator[bytes, None]:
        self.reads.append(start_index)
        should_break = self.break_once
        self.break_once = False
        return self._serve(should_break)

    async def _serve(self, should_break: bool) -> AsyncGenerator[bytes, None]:
        if should_break:
            yield self.wire[:3]
            raise w.WorkflowWorldError("live read expired", status=500)
        if self.split:
            yield self.wire[:2]
            yield self.wire[2:7]
            yield self.wire[7:]
        else:
            yield self.wire


async def _run_abort_listener(world: _AbortListenerWorld, reason: str) -> None:
    async def waits_forever() -> None:
        await asyncio.sleep(3600)

    step = core.Step(waits_forever, cancellable=True)
    with pytest.raises(errors.StepCancelledError, match=reason):
        await runtime._run_cancellable_step(
            step,
            (),
            {},
            world=world,  # type: ignore[arg-type]
            run_id="wrun_test",
            step_id="step_01M1329YCHWS235PHTPW377KZM",
        )


async def test_abort_listener_reconnects_after_live_read_failure() -> None:
    reason = "delivered after reconnect"
    world = _AbortListenerWorld(reason, break_once=True)

    await _run_abort_listener(world, reason)

    assert world.reads == [0, 0]


async def test_abort_listener_reassembles_transport_fragments() -> None:
    reason = "complete fragmented reason"
    world = _AbortListenerWorld(reason, split=True)

    await _run_abort_listener(world, reason)


class _OpenAbortListenerWorld:
    def streams_get(
        self, run_id: str, name: str, start_index: int | None = None
    ) -> AsyncGenerator[bytes, None]:
        return self._serve()

    async def _serve(self) -> AsyncGenerator[bytes, None]:
        await asyncio.sleep(3600)
        yield b"unreachable"


async def test_cancellable_step_error_is_not_wrapped_in_exception_group() -> None:
    async def fails() -> None:
        raise LookupError("plain step failure")

    step = core.Step(fails, cancellable=True)
    with pytest.raises(LookupError, match="plain step failure"):
        await runtime._run_cancellable_step(
            step,
            (),
            {},
            world=_OpenAbortListenerWorld(),  # type: ignore[arg-type]
            run_id="wrun_test",
            step_id="step_01M1329YCHWS235PHTPW377KZM",
        )
