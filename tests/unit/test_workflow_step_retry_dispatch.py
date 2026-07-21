"""Regression test for step-retry dispatch in the queue-handler wrapper.

When a step fails below ``max_retries``, ``step_handler`` returns a retry timeout
(``return 1.0``). The wrapper registered by ``World.create_queue_handler``
(``async_handler``) is responsible for rescheduling the step after that delay.

``async_handler`` must re-enqueue the retry preserving the message's payload type.
Previously it re-enqueued *every* timeout return as a ``WorkflowInvokePayload``,
which raised ``ValidationError`` for a ``StepInvokePayload`` → the handler 500'd and
the step only retried via un-acked redelivery (wrong cadence, no backoff). The fix
validates against the ``QueuePayload`` union, so a step retry re-enqueues a
``StepInvokePayload`` on its own queue with the delay.

This drives the real ``async_handler`` closure with a step payload and a handler
that asks to retry.
"""

from __future__ import annotations

import pytest

from vercel._internal.workflow import world as w
from vercel._internal.workflow.worlds.local import LocalWorld
from vercel.workers._queue.subscribe import subscriptions as _subs_registry


class _RecordingWorld(LocalWorld):
    """LocalWorld whose outbound queue is captured instead of sent over the network."""

    def __init__(self) -> None:
        super().__init__()
        self.queued: list[tuple[str, object, object, object]] = []

    async def queue(self, queue_name: str, message: object, **kwargs: object) -> str:
        self.queued.append(
            (queue_name, message, kwargs.get("delay_seconds"), kwargs.get("idempotency_key"))
        )
        return "msg_test"


@pytest.fixture
def isolated_subscriptions():
    saved = list(_subs_registry)
    _subs_registry.clear()
    try:
        yield _subs_registry
    finally:
        _subs_registry[:] = saved


async def test_step_retry_timeout_reschedules_step(isolated_subscriptions) -> None:
    world = _RecordingWorld()

    async def handler(payload, *, queue_name, attempt, message_id):
        # Stand in for step_handler deciding to retry: a non-None continuation.
        return w.QueueContinuation(delay_seconds=1.0)

    # Registers async_handler into the global subscription registry as a side effect.
    world.create_queue_handler("__wkf_step_", handler)
    assert len(isolated_subscriptions) == 1
    async_handler = isolated_subscriptions[0].func

    step_payload = w.StepInvokePayload(
        workflowName="wf",
        workflowRunId="wrun_1",
        workflowStartedAt=0.0,
        stepId="step_1",
    ).model_dump()
    body = {
        "payload": step_payload,
        "queueName": "__wkf_step_wf",
        "deploymentId": "<local>",
    }
    meta = {"deliveryCount": 1, "messageId": "m1", "topic": "__wkf_step_wf"}

    await async_handler(body, meta)

    assert world.queued, "step retry was not re-enqueued"
    qn, msg, delay, _idem = world.queued[-1]
    assert qn == "__wkf_step_wf"
    step_id = getattr(msg, "step_id", None) or (
        msg.get("stepId") if isinstance(msg, dict) else None
    )
    assert step_id == "step_1"
    assert delay == 1.0


async def test_wait_continuation_forwards_idempotency_key(isolated_subscriptions) -> None:
    """A QueueContinuation return re-enqueues with its idempotency key, so repeated
    suspension passes over the same pending wait dedupe to one delayed wake-up."""
    world = _RecordingWorld()

    async def handler(payload, *, queue_name, attempt, message_id):
        # Stand in for workflow_handler suspending on a wait.
        return w.QueueContinuation(delay_seconds=5.0, idempotency_key="wait_xyz")

    world.create_queue_handler("__wkf_workflow_", handler)
    assert len(isolated_subscriptions) == 1
    async_handler = isolated_subscriptions[0].func

    wf_payload = w.WorkflowInvokePayload(runId="wrun_1").model_dump()
    body = {
        "payload": wf_payload,
        "queueName": "__wkf_workflow_wf",
        "deploymentId": "<local>",
    }
    meta = {"deliveryCount": 1, "messageId": "m1", "topic": "__wkf_workflow_wf"}

    await async_handler(body, meta)

    assert world.queued, "wait continuation was not re-enqueued"
    qn, _msg, delay, idem = world.queued[-1]
    assert qn == "__wkf_workflow_wf"
    assert delay == 5.0
    assert idem == "wait_xyz"
