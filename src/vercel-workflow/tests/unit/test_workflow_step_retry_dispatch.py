"""Step dispatch recovery and retry scheduling through queue handlers.

When a step fails below ``max_retries``, the handler's step path returns a retry
timeout. The wrapper registered by ``World.create_queue_handler``
(``async_handler``) is responsible for rescheduling it after that delay.

Steps and orchestration share one topic, so what distinguishes a step message is
the ``stepId`` on its ``WorkflowInvokePayload``. ``async_handler`` re-enqueues the
continuation by round-tripping the raw payload through ``QueuePayload``, which
means a dropped ``stepId`` would silently turn the retry into a plain replay: the
step would never run again, and the run would stall until redelivery. These tests
drive the real ``async_handler`` closure and assert what survives that round trip.
Recovery tests also replay persisted creations after dispatch failures and check
that running steps and their retries stay on the original execution message.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import cbor2
import httpx2 as httpx
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.queue import Message, MessageMetadata, SanitizedName, get_subscriptions
from vercel.workflow._internal import core, runtime, world as w
from vercel.workflow._internal.worlds.local import LocalWorld
from vercel.workflow._internal.worlds.vercel import VercelWorld

CREATED_AT = datetime(2024, 1, 1, tzinfo=UTC)


class _RecordingWorld(VercelWorld):
    """VercelWorld whose outbound queue is captured instead of sent over the network."""

    def __init__(self) -> None:
        super().__init__()
        self.queued: list[tuple[str, object, object, object]] = []

    async def queue(self, queue_name: str, message: object, **kwargs: object) -> str:
        self.queued.append(
            (queue_name, message, kwargs.get("delay_seconds"), kwargs.get("idempotency_key"))
        )
        return "msg_test"


async def test_step_retry_timeout_reschedules_step(isolated_subscriptions: None) -> None:
    world = _RecordingWorld()

    async def handler(
        message: object, *, queue_name: str, attempt: int, message_id: str
    ) -> w.QueueContinuation:
        # Stand in for the step path deciding to retry: a non-None continuation.
        del message, queue_name, attempt, message_id
        return w.QueueContinuation(delay_seconds=1.0)

    # Registers async_handler into the global subscription registry as a side effect.
    world.create_queue_handler("__wkf_workflow_", handler)
    subscriptions = list(get_subscriptions())
    assert len(subscriptions) == 1
    assert subscriptions[0].topic == "__wkf_workflow_*"
    assert subscriptions[0].consumer_group == "default"
    async_handler = subscriptions[0].func
    assert async_handler is not None

    step_payload = w.WorkflowInvokePayload(
        run_id="wrun_1",
        step_id="step_1",
        step_name="step//tests.add",
    ).model_dump()
    body = {
        "payload": step_payload,
        "queueName": "__wkf_workflow_wf",
        "deploymentId": "<local>",
    }
    metadata = MessageMetadata(
        message_id="m1",
        delivery_count=1,
        created_at=CREATED_AT,
        topic="__wkf_workflow_wf",
        consumer_group=SanitizedName("tests"),
    )

    await async_handler(Message(payload=body, metadata=metadata))

    assert world.queued, "step retry was not re-enqueued"
    qn, msg, delay, _idem = world.queued[-1]
    assert qn == "__wkf_workflow_wf"
    assert isinstance(msg, w.WorkflowInvokePayload)
    # Without both of these the redelivery replays the run instead of retrying
    # the step, since that is the only thing telling the two message kinds apart.
    assert msg.step_id == "step_1"
    assert msg.step_name == "step//tests.add"
    assert delay == 1.0


async def test_wait_continuation_forwards_idempotency_key(isolated_subscriptions: None) -> None:
    """A QueueContinuation return re-enqueues with its idempotency key, so repeated
    suspension passes over the same pending wait dedupe to one delayed wake-up."""
    world = _RecordingWorld()

    async def handler(
        message: object, *, queue_name: str, attempt: int, message_id: str
    ) -> w.QueueContinuation:
        # Stand in for workflow_handler suspending on a wait.
        del message, queue_name, attempt, message_id
        return w.QueueContinuation(delay_seconds=5.0, idempotency_key="wait_xyz")

    world.create_queue_handler("__wkf_workflow_", handler)
    subscriptions = list(get_subscriptions())
    assert len(subscriptions) == 1
    assert subscriptions[0].topic == "__wkf_workflow_*"
    assert subscriptions[0].consumer_group == "default"
    async_handler = subscriptions[0].func
    assert async_handler is not None

    wf_payload = w.WorkflowInvokePayload(run_id="wrun_1").model_dump()
    body = {
        "payload": wf_payload,
        "queueName": "__wkf_workflow_wf",
        "deploymentId": "<local>",
    }
    metadata = MessageMetadata(
        message_id="m1",
        delivery_count=1,
        created_at=CREATED_AT,
        topic="__wkf_workflow_wf",
        consumer_group=SanitizedName("tests"),
    )

    await async_handler(Message(payload=body, metadata=metadata))

    assert world.queued, "wait continuation was not re-enqueued"
    qn, _msg, delay, idem = world.queued[-1]
    assert qn == "__wkf_workflow_wf"
    assert delay == 5.0
    assert idem == "wait_xyz"


DISPATCH_REGISTRY = core.Workflows(as_vercel_job=False)


@DISPATCH_REGISTRY.step
async def dispatch_step() -> int:
    return 42


@DISPATCH_REGISTRY.workflow
async def dispatch_workflow() -> int:
    return await dispatch_step()


# @lat: [[vercel-workflow#Execution model#Step dispatch recovery]]
@pytest.mark.parametrize("spec_version", [5, 6])
@pytest.mark.parametrize(
    "fault", ["none", "creation_response_lost", "queue_send_failed", "creation_conflict"]
)
async def test_replay_recovers_step_dispatch(tmp_path, monkeypatch, fault, spec_version) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = LocalWorld()
    queue = AsyncMock(return_value="msg_repro")
    monkeypatch.setattr(world, "queue", queue)
    w.set_world(world)
    try:
        created = await world.events_create(
            None,
            w.RunCreatedEventData(
                deployment_id="dpl_repro",
                workflow_name=dispatch_workflow.workflow_id,
                input=PLAIN_ENCODER.encode([]),
            )
            .into_event()
            .model_copy(update={"spec_version": spec_version}),
        )
        assert created.run is not None
        run_id = created.run.run_id
        queue_name = w.get_queue_name(dispatch_workflow.workflow_id)

        async def invoke(message=None, *, attempt=1):
            return await runtime.workflow_handler(
                message or {"runId": run_id},
                attempt=attempt,
                queue_name=queue_name,
                message_id=f"msg_replay_{attempt}",
                registry=DISPATCH_REGISTRY,
            )

        events_create = world.events_create
        injected = False

        async def commit_then_lose_response(target_run_id, event):
            nonlocal injected
            result = await events_create(target_run_id, event)
            if event.event_type == "step_created" and not injected:
                injected = True
                if fault == "creation_conflict":
                    raise w.EntityConflictError("created by a concurrent invocation")
                raise httpx.ReadTimeout("step_created committed; response lost")
            return result

        if fault in {"creation_response_lost", "creation_conflict"}:
            monkeypatch.setattr(world, "events_create", commit_then_lose_response)
        elif fault == "queue_send_failed":
            queue.side_effect = httpx.ConnectError("queue unavailable before send")

        if fault in {"none", "creation_conflict"}:
            await invoke()
            assert queue.await_count == 1
        else:
            with pytest.raises(ExceptionGroup) as raised:
                await invoke()
            expected = (
                httpx.ReadTimeout if fault == "creation_response_lost" else httpx.ConnectError
            )
            assert len(raised.value.exceptions) == 1
            assert isinstance(raised.value.exceptions[0], expected)
            assert queue.await_count == (0 if fault == "creation_response_lost" else 1)
            if fault == "creation_response_lost":
                assert injected

        events = (await world.events_list(run_id)).data
        step_events = [event for event in events if event.event_type == "step_created"]
        assert len(step_events) == 1
        step_id = step_events[0].correlation_id
        assert step_id is not None
        assert not any(event.event_type == "step_started" for event in events)
        step = await world.steps_get(run_id, step_id)
        assert step.status == "pending"
        assert step.attempt == 0

        if fault not in {"none", "creation_conflict"}:
            # A fresh world reads the committed event from disk. All subsequent
            # calls are healthy, and each invocation builds a new orchestrator.
            await world.aclose()
            world = LocalWorld()
            queue = AsyncMock(return_value="msg_recovered")
            monkeypatch.setattr(world, "queue", queue)
            w.set_world(world)
            for attempt in range(2, 5):
                continuation = await invoke(attempt=attempt)
                assert continuation is None
            assert (await world.events_list(run_id)).data == events
            assert (await world.runs_get(run_id)).status == "running"
            assert (await world.steps_get(run_id, step_id)).attempt == 0
            assert queue.await_count == 3

        # Deliver the recovered step message and finish the workflow.
        message = queue.call_args.args[1]
        assert isinstance(message, w.WorkflowInvokePayload)
        assert message.step_id == step_id
        assert message.requested_at is not None
        assert queue.call_args.kwargs["idempotency_key"] == step_id
        queue.reset_mock()
        await invoke(message.model_dump())
        await invoke(attempt=5)
        run = await world.runs_get(run_id)
        assert run.status == "completed"
        assert run.output == PLAIN_ENCODER.encode(42)
    finally:
        w.set_world(None)
        await world.aclose()


@pytest.mark.parametrize("state", ["running", "retrying", "completed", "failed"])
async def test_replay_does_not_redispatch_started_or_terminal_steps(
    tmp_path, monkeypatch, state
) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = LocalWorld()
    queue = AsyncMock(return_value="msg_step")
    monkeypatch.setattr(world, "queue", queue)
    w.set_world(world)
    try:
        created = await world.events_create(
            None,
            w.RunCreatedEventData(
                deployment_id="dpl_test",
                workflow_name=dispatch_workflow.workflow_id,
                input=PLAIN_ENCODER.encode([]),
            ).into_event(),
        )
        assert created.run is not None
        run_id = created.run.run_id

        async def invoke():
            await runtime.workflow_handler(
                {"runId": run_id},
                attempt=1,
                queue_name=w.get_queue_name(dispatch_workflow.workflow_id),
                message_id="msg_replay",
                registry=DISPATCH_REGISTRY,
            )

        await invoke()
        step_id = queue.call_args.args[1].step_id
        await world.events_create(run_id, w.StepStartedEvent(correlation_id=step_id))
        if state == "retrying":
            await world.events_create(
                run_id,
                w.StepRetryingEventData(
                    error=PLAIN_ENCODER.encode_error(ValueError("retry later")),
                    retry_after=datetime.now(UTC) + timedelta(hours=1),
                ).into_event(step_id),
            )
        elif state == "completed":
            await world.events_create(
                run_id,
                w.StepCompletedEventData(result=PLAIN_ENCODER.encode(42)).into_event(step_id),
            )
        elif state == "failed":
            await world.events_create(
                run_id,
                w.StepFailedEventData(
                    error=PLAIN_ENCODER.encode_error(ValueError("failed"))
                ).into_event(step_id),
            )
        queue.reset_mock()
        await invoke()
        queue.assert_not_awaited()
        step = await world.steps_get(run_id, step_id)
        assert step.attempt == 1
        assert step.status == ("pending" if state == "retrying" else state)
    finally:
        w.set_world(None)
        await world.aclose()


@pytest.mark.parametrize("spec_version", [5, 6])
async def test_replay_dispatches_step_after_lost_creation_response(
    monkeypatch: pytest.MonkeyPatch, spec_version: int
) -> None:
    import vendor.respx as respx

    with respx.mock:
        world = VercelWorld(token="test-token")
        queue = AsyncMock(return_value="msg_step")
        monkeypatch.setattr(world, "queue", queue)
        now = datetime.now(UTC)
        run_id = "wrun_dispatch"
        run = {
            "runId": run_id,
            "deploymentId": "dpl_test",
            "workflowName": dispatch_workflow.workflow_id,
            "status": "running",
            "specVersion": spec_version,
            "input": PLAIN_ENCODER.encode([]),
            "createdAt": now,
            "updatedAt": now,
            "startedAt": now,
        }
        events: list[dict[str, object]] = []

        def post_event(request: httpx.Request) -> httpx.Response:
            event = cbor2.loads(request.content)
            if event["eventType"] == "run_started":
                return httpx.Response(200, content=cbor2.dumps({"run": run}))
            assert event["eventType"] == "step_created"
            event.update(runId=run_id, eventId="evnt_created", createdAt=now)
            events.append(event)
            # Commit the event before losing the response. Replay reads it via GET.
            raise httpx.ReadTimeout("creation committed; response lost")

        route = respx.post(f"{world._base_url}/v3/runs/{run_id}/events").mock(
            side_effect=post_event
        )
        respx.get(f"{world._base_url}/v2/runs/{run_id}").respond(content=cbor2.dumps(run))
        respx.get(url__startswith=f"{world._base_url}/v3/runs/{run_id}/events").mock(
            side_effect=lambda _: httpx.Response(
                200, content=cbor2.dumps({"data": events, "cursor": None, "hasMore": False})
            )
        )

        async def invoke():
            return await runtime.workflow_handler(
                {"runId": run_id},
                attempt=1,
                queue_name=w.get_queue_name(dispatch_workflow.workflow_id),
                message_id="msg_replay",
                registry=DISPATCH_REGISTRY,
            )

        w.set_world(world)
        try:
            with pytest.raises(ExceptionGroup) as raised:
                await invoke()
            assert len(raised.value.exceptions) == 1
            assert isinstance(raised.value.exceptions[0], httpx.ReadTimeout)
            queue.assert_not_awaited()
            assert len(events) == 1

            assert await invoke() is None
            queue.assert_awaited_once()
            assert queue.call_args.args[1].step_id == events[0]["correlationId"]
            assert queue.call_args.kwargs["idempotency_key"] == events[0]["correlationId"]
            assert route.call_count == 3  # Two run starts; only one step creation.
        finally:
            w.set_world(None)
            await world.aclose()
