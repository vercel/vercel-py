"""Bytes crossing the local world's queue.

The transport is plain JSON, which has nowhere to put `bytes`, so
`@workflow/world-local` smuggles them through as a `{__type: "Uint8Array"}`
envelope via its `jsonReplacer` / `jsonReviver` pair. The file store already
speaks that dialect (`dumps_js` / `read_json`); the queue has to as well, or a
payload carrying a run's input either fails to serialize on the way out or
arrives as an envelope nothing unwraps.

The pair has to stay a pair. Decoding on receive without encoding on send breaks
the re-enqueue path, which re-sends the payload it was handed.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from vercel.workflow._internal import serialization as ser, world as w
from vercel.workflow._internal.worlds import local as local_mod

RUN_ID = "wrun_codec"
WORKFLOW = "add_ten"
# What `dehydrate` produces, and what a TS producer base64s into the envelope.
INPUT = ser.dehydrate([[1], 123])


def _payload() -> w.WorkflowInvokePayload:
    return w.WorkflowInvokePayload(
        run_id=RUN_ID,
        run_input=w.RunInput.from_wire(
            {
                "input": INPUT,
                "deploymentId": "dpl_local",
                "workflowName": WORKFLOW,
                "specVersion": 6,
            }
        ),
    )


def _on_the_wire(payload: w.WorkflowInvokePayload) -> dict:
    """What `queue()` hands the transport, proven JSON-serializable."""
    encoded = local_mod._encode_js(payload.model_dump())
    return json.loads(json.dumps(encoded))


def _parse(wire: dict) -> w.WorkflowInvokePayload:
    """Decode a wire dict the way the receive handler does."""
    parsed = w.QueuePayloadAdaptor.from_wire(local_mod._decode_js(wire))
    assert isinstance(parsed, w.WorkflowInvokePayload)
    assert parsed.run_input is not None
    return parsed


def test_send_encodes_bytes_as_the_uint8array_envelope() -> None:
    """Raw `bytes` would not survive `json.dumps` at all."""
    wire = _on_the_wire(_payload())

    assert wire["runInput"]["input"] == {
        "__type": "Uint8Array",
        "data": "ZGV2bFtbMSwzXSxbMl0sMSwxMjNd",
    }


def test_receive_restores_the_bytes() -> None:
    parsed = _parse(_on_the_wire(_payload()))

    assert parsed.run_input is not None and parsed.run_input.input == INPUT


def test_a_ts_written_envelope_is_accepted() -> None:
    """The producer is `@workflow/world-local`, so this is the real input shape —
    and the run entity it builds wants `bytes`, not the envelope."""
    ts_wire = {
        "runId": RUN_ID,
        "runInput": {
            "input": {"__type": "Uint8Array", "data": "ZGV2bFtbMSwzXSxbMl0sMSwxMjNd"},
            "deploymentId": "dpl_local",
            "workflowName": WORKFLOW,
            "specVersion": 6,
        },
    }

    parsed = _parse(ts_wire)

    assert parsed.run_input is not None and parsed.run_input.input == INPUT
    # The whole point: this is what `_resilient_create_run` feeds the run row.
    run = w.NonFinalWorkflowRun(
        run_id=RUN_ID,
        deployment_id="dpl_local",
        status="pending",
        workflow_name=WORKFLOW,
        spec_version=6,
        input=parsed.run_input.input,
        created_at=local_mod.js_now(),
        updated_at=local_mod.js_now(),
    )
    assert run.input == INPUT


def test_re_enqueue_round_trips() -> None:
    """The receive handler re-sends the payload it was handed, so a decoded
    `bytes` has to be re-encodable. Decoding without encoding breaks here."""
    parsed = _parse(_on_the_wire(_payload()))

    again = _parse(_on_the_wire(parsed))

    assert again.run_input is not None and again.run_input.input == INPUT


def test_a_payload_without_bytes_is_unchanged() -> None:
    """A re-enqueue carries no `runInput`, and must not grow anything."""
    bare = w.WorkflowInvokePayload(run_id=RUN_ID)

    wire = _on_the_wire(bare)

    assert wire == {"runId": RUN_ID}


async def test_bytes_survive_a_real_local_queue_delivery(tmp_path, monkeypatch) -> None:
    """End to end through the embedded queue, which is where the two halves of
    the codec actually have to meet."""
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    delivered: list[dict] = []

    async def handler(
        message: Any, *, attempt: int, queue_name: str, message_id: str
    ) -> w.QueueContinuation | None:
        delivered.append(message)
        return None

    world.create_queue_handler(w.get_queue_topic_prefix(), handler)
    try:
        await world.queue(w.get_queue_name(WORKFLOW), _payload())
        for _ in range(100):
            if delivered:
                break
            await asyncio.sleep(0.05)
    finally:
        await world.aclose()

    assert delivered, "the message was never delivered"
    parsed = w.QueuePayloadAdaptor.from_wire(delivered[0])
    assert isinstance(parsed, w.WorkflowInvokePayload)
    assert parsed.run_input is not None and parsed.run_input.input == INPUT
