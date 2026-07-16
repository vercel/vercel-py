"""Regression test for step-retry dispatch in the queue-handler wrapper.

When a step fails below ``max_retries``, ``step_handler`` returns a retry timeout
(``return 1.0``). The wrapper registered by ``World.create_queue_handler``
(``@subscribe`` handler) is responsible for rescheduling the step after that delay.

The handler must re-enqueue the retry preserving the message's payload type.
Previously it re-enqueued *every* timeout return as a ``WorkflowInvokePayload``,
which raised ``ValidationError`` for a ``StepInvokePayload`` → the handler 500'd and
the step only retried via un-acked redelivery (wrong cadence, no backoff). The fix
validates against the ``QueuePayload`` union, so a step retry re-enqueues a
``StepInvokePayload`` on its own queue with the delay.

This drives the ``@subscribe`` handler via ``accept_and_handle`` with a step
payload and a handler that asks to retry.
"""

from __future__ import annotations

import json

from vercel._internal.workflow import world as w
from vercel._internal.workflow.worlds.local import LocalWorld


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


async def test_step_retry_timeout_reschedules_step() -> None:
    world = _RecordingWorld()

    async def handler(payload, *, queue_name, attempt, message_id):
        # Stand in for step_handler deciding to retry: a non-None continuation.
        return w.QueueContinuation(delay_seconds=1.0)

    world.create_queue_handler("__wkf_step_", handler)

    # Simulate push delivery through accept_and_handle, which invokes
    # the @subscribe handler registered by create_queue_handler.
    step_payload = w.StepInvokePayload(
        workflowName="wf",
        workflowRunId="wrun_1",
        workflowStartedAt=0.0,
        stepId="step_1",
    ).model_dump()
    body = json.dumps(
        {
            "payload": step_payload,
            "queueName": "__wkf_step_wf",
            "deploymentId": "<local>",
        }
    ).encode()
    headers = {
        "ce-type": "com.vercel.queue.v2beta",
        "ce-vqsqueuename": "__wkf_step_wf",
        "ce-vqsmessageid": "m1",
        "ce-vqsdeliverycount": "1",
        "ce-vqsconsumergroup": "wkf-__wkf_step_",
        "ce-vqsreceipthandle": "receipt_1",
        "ce-vqscreatedat": "2026-01-01T00:00:00Z",
        "ce-vqsregion": "iad1",
        "content-type": "application/json",
    }

    client = world._eq_app.get_async_client()
    await client._accept_and_handle(body, headers)

    assert world.queued, "step retry was not re-enqueued"
    qn, msg, delay, _idem = world.queued[-1]
    assert qn == "__wkf_step_wf"
    step_id = getattr(msg, "step_id", None) or (
        msg.get("stepId") if isinstance(msg, dict) else None
    )
    assert step_id == "step_1"
    assert delay == 1.0


async def test_wait_continuation_forwards_idempotency_key() -> None:
    """A QueueContinuation return re-enqueues with its idempotency key, so repeated
    suspension passes over the same pending wait dedupe to one delayed wake-up."""
    world = _RecordingWorld()

    async def handler(payload, *, queue_name, attempt, message_id):
        # Stand in for workflow_handler suspending on a wait.
        return w.QueueContinuation(delay_seconds=5.0, idempotency_key="wait_xyz")

    world.create_queue_handler("__wkf_workflow_", handler)

    wf_payload = w.WorkflowInvokePayload(runId="wrun_1").model_dump()
    body = json.dumps(
        {
            "payload": wf_payload,
            "queueName": "__wkf_workflow_wf",
            "deploymentId": "<local>",
        }
    ).encode()
    headers = {
        "ce-type": "com.vercel.queue.v2beta",
        "ce-vqsqueuename": "__wkf_workflow_wf",
        "ce-vqsmessageid": "m2",
        "ce-vqsdeliverycount": "1",
        "ce-vqsconsumergroup": "wkf-__wkf_workflow_",
        "ce-vqsreceipthandle": "receipt_2",
        "ce-vqscreatedat": "2026-01-01T00:00:00Z",
        "ce-vqsregion": "iad1",
        "content-type": "application/json",
    }

    client = world._eq_app.get_async_client()
    await client._accept_and_handle(body, headers)

    assert world.queued, "wait continuation was not re-enqueued"
    qn, _msg, delay, idem = world.queued[-1]
    assert qn == "__wkf_workflow_wf"
    assert delay == 5.0
    assert idem == "wait_xyz"
