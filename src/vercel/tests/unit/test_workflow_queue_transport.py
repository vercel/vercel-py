"""Tests for the CBOR queue transport VercelWorld sends and receives with.

Mirrors ``DualTransport`` in ``@workflow/world-vercel``: every producer at
``specVersion >= 3`` puts workflow messages on the queue as CBOR, so the
default JSON transport cannot read a single delivery.
"""

from __future__ import annotations

import datetime
import json
from collections.abc import AsyncIterator
from typing import Any

import cbor2

from vercel._internal.core.polyfills import UTC
from vercel._internal.workflow import world as w
from vercel._internal.workflow.worlds import vercel as vercel_mod
from vercel.queue import MessageMetadata
from vercel.queue._internal.subscribers import infer_subscriber_transport

WRAPPER = {
    "payload": {"runId": "wrun_1", "stepId": "step_1"},
    "queueName": "__wkf_workflow_greet",
    "deploymentId": "dpl_1",
}


async def _chunks(body: bytes) -> AsyncIterator[bytes]:
    # Deliveries arrive as a stream; split so a transport that assumes a
    # single chunk fails here rather than in production.
    yield body[:3]
    yield body[3:]


async def _decode(body: bytes, *, content_type: str = "application/cbor") -> Any:
    return await vercel_mod._QueueTransport().deserialize(_chunks(body), content_type=content_type)


def test_serialize_writes_cbor() -> None:
    transport = vercel_mod._QueueTransport()

    assert transport.content_type == "application/cbor"
    assert cbor2.loads(transport.serialize(WRAPPER)) == WRAPPER


async def test_deserialize_reads_cbor() -> None:
    assert await _decode(cbor2.dumps(WRAPPER)) == WRAPPER


async def test_deserialize_resolves_undefined_and_typed_arrays() -> None:
    """The JS encoder emits both; neither has a Python equivalent."""
    body = cbor2.dumps(
        {
            "payload": {"runId": "wrun_1", "stepId": cbor2.undefined},
            "queueName": cbor2.CBORTag(64, b"\x00\x01"),
        }
    )

    assert await _decode(body) == {
        "payload": {"runId": "wrun_1", "stepId": None},
        "queueName": b"\x00\x01",
    }


async def test_deserialize_falls_back_to_json_for_an_unlabelled_body() -> None:
    """A producer that predates the CBOR transport sends JSON."""
    assert await _decode(json.dumps(WRAPPER).encode()) == WRAPPER


async def test_deserialize_reads_json_when_the_content_type_says_so() -> None:
    body = json.dumps(WRAPPER).encode()

    assert await _decode(body, content_type="application/json") == WRAPPER


def test_dispatch_resolves_the_transport_without_help_from_the_client(
    isolated_subscriptions: None,
) -> None:
    """The deployed entrypoint is a generated ``vercel.queue.asgi_app()``.

    Its client is built by the platform and carries no transport, so what
    matters is the transport dispatch resolves from the *subscription* — not
    anything hanging off a client the world constructed. Asserting the latter
    is what hid this: the world's own client never receives a delivery.
    """
    world = vercel_mod.VercelWorld(token="tok")

    async def handler(message, *, queue_name, attempt, message_id):  # noqa: ANN001, ANN202
        del message, queue_name, attempt, message_id
        return None

    world.create_queue_handler("__wkf_workflow_", handler)

    metadata = MessageMetadata(
        message_id="m1",
        delivery_count=1,
        created_at=datetime.datetime(2026, 1, 1, tzinfo=UTC),
        topic="__wkf_workflow_greet",
        consumer_group=w.QUEUE_CONSUMER_GROUP,
        content_type="application/cbor",
    )

    assert infer_subscriber_transport(metadata) is vercel_mod._QUEUE_TRANSPORT
