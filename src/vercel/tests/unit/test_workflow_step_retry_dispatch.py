"""Regression test for step-retry dispatch in the queue-handler wrapper.

When a step fails below ``max_retries``, the handler's step path returns a retry
timeout. The wrapper registered by ``World.create_queue_handler``
(``async_handler``) is responsible for rescheduling it after that delay.

Steps and orchestration share one topic, so what distinguishes a step message is
the ``stepId`` on its ``WorkflowInvokePayload``. ``async_handler`` re-enqueues the
continuation by round-tripping the raw payload through ``QueuePayload``, which
means a dropped ``stepId`` would silently turn the retry into a plain replay: the
step would never run again, and the run would stall until redelivery. These tests
drive the real ``async_handler`` closure and assert what survives that round trip.
"""

from __future__ import annotations

from datetime import datetime

from vercel._internal.core.polyfills import UTC
from vercel._internal.workflow import world as w
from vercel._internal.workflow.worlds.vercel import VercelWorld
from vercel.queue import Message, MessageMetadata, SanitizedName, get_subscriptions

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
        runId="wrun_1",
        stepId="step_1",
        stepName="step//tests.add",
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

    wf_payload = w.WorkflowInvokePayload(runId="wrun_1").model_dump()
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
