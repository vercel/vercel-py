"""The wire shapes lazy hook resume adds, and what goes out on a request.

Resuming a hook takes two writes -- the ``hook_received`` event and the queue
message that wakes the run -- and ``resumeHook()`` can do both at once, putting
the payload on the message so the consumer can write the event itself. That adds
three things to the wire: ``hookInput`` on the queue message, ``token`` on the
event body, and ``resumeId`` on the stored event and on the request that writes
it.

All of it is pinned here because the producer is the TypeScript SDK and a field
dropped in parsing is silent: it presents as a run that suspends and is never
woken again.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import cbor2
import httpx
import pytest
import respx

from vercel._internal.core.polyfills import UTC
from vercel.workflow._internal import core, runtime, serialization as ser, world as w
from vercel.workflow._internal.worlds.local import LocalWorld
from vercel.workflow._internal.worlds.vercel import VercelWorld

RUN_ID = "wrun_test"
TOKEN = "he5a2dqaa26"
RESUME_ID = "01KZZ42P2MR25DYSW1MTMMACA3"
DIGEST = "66c534074063d7f2dd180074e1f622f2190f830094837ab86d056913eb42560a"
CBOR = {"content-type": "application/cbor"}

# What `@workflow/core`'s resumeHook() puts on the queue on the parallel path,
# keyed the way it appears on the wire. Captured from a real delivery.
HOOK_INPUT_WIRE = {
    "resumeId": RESUME_ID,
    "hookId": "hook_01KZZ42NTMN109EHVZX692JWD5",
    "token": TOKEN,
    "payload": b'devl[{"type":1,"id":2},"subscribe",1]',
    "payloadDigest": DIGEST,
    "deploymentId": "dpl_local@5.0.0-beta.34",
}


# ── the queue message ──────────────────────────────────────────────────────


def test_hook_input_round_trips_every_field() -> None:
    """A field we drop here is a field the event we write loses -- and the digest
    and resume id are what keep the two writers to one event."""
    payload = w.WorkflowInvokePayload.model_validate(
        {"runId": "wrun_1", "hookInput": HOOK_INPUT_WIRE}
    )

    assert payload.hook_input is not None
    assert payload.hook_input.model_dump() == HOOK_INPUT_WIRE


def test_hook_input_absent_on_the_sequential_path() -> None:
    """A producer that wrote the event first, or predates the fast path, sends the
    run id alone. That is normal: the event is already there."""
    payload = w.WorkflowInvokePayload.model_validate({"runId": "wrun_1"})

    assert payload.hook_input is None
    # Omitted rather than serialized as null: the TS reader types it `undefined`.
    assert "hookInput" not in payload.model_dump()


def test_hook_input_deployment_id_may_be_absent() -> None:
    """Older producers omit it; everything else is required of any producer that
    takes the fast path at all."""
    hook_input = w.HookResumeInput.model_validate(
        {k: v for k, v in HOOK_INPUT_WIRE.items() if k != "deploymentId"}
    )

    assert hook_input.deployment_id is None
    assert "deploymentId" not in hook_input.model_dump()


# ── the event ──────────────────────────────────────────────────────────────


def test_hook_received_carries_the_token_the_producer_wrote() -> None:
    """The two writers of one resume have to produce the same event body, and the
    producer's has `token` in it."""
    event = w.EventAdaptor.validate_python(
        {
            "eventType": "hook_received",
            "correlationId": "hook_1",
            "runId": "wrun_1",
            "eventId": "evnt_00000000000000000000000002",
            "createdAt": "2026-08-14T03:08:48.849Z",
            "specVersion": 5,
            "eventData": {"token": TOKEN, "payload": b"devl[]"},
        }
    )

    assert isinstance(event, w.HookReceivedEvent)
    assert event.event_data.token == TOKEN
    assert event.event_data.payload == b"devl[]"


def test_hook_received_token_is_omitted_when_there_is_none() -> None:
    """A plain `resume_hook()` is the only writer of its event, so the row it
    writes has to stay byte-identical to what it has always been."""
    data = w.HookReceivedEventData(payload=b"devl[]")

    assert data.model_dump() == {"payload": b"devl[]"}


def test_hook_received_payloads_expose_only_the_payload() -> None:
    """`payloads()` decides whether the run needs a decryption key. The token is
    plaintext by design -- counting it would ask for a key on every resume."""
    data = w.HookReceivedEventData(payload=b"encr\x00sealed", token=TOKEN)

    assert data.into_event("hook_1").payloads() == (b"encr\x00sealed",)


def test_resume_id_folds_into_server_props() -> None:
    """Set by the world like `eventId`, not by the writer, so it belongs with the
    rest of the server-assigned fields rather than in the authored event."""
    event = w.EventAdaptor.validate_python(
        {
            "eventType": "hook_received",
            "correlationId": "hook_1",
            "runId": "wrun_1",
            "eventId": "evnt_2",
            "createdAt": "2026-08-14T03:08:48.849Z",
            "resumeId": RESUME_ID,
            "eventData": {"payload": b"devl[]"},
        }
    )

    assert event.server_props is not None
    assert event.server_props.resume_id == RESUME_ID
    assert "resumeId" not in event.model_dump()


def test_resume_id_absent_on_every_other_event() -> None:
    """Most events have none, and sending a null instead of nothing would give the
    TS reader a field it types `undefined`."""
    event = w.EventAdaptor.validate_python(
        {
            "eventType": "hook_received",
            "correlationId": "hook_1",
            "runId": "wrun_1",
            "eventId": "evnt_2",
            "createdAt": "2026-08-14T03:08:48.849Z",
            "eventData": {"payload": b"devl[]"},
        }
    )

    assert event.server_props is not None
    assert event.server_props.resume_id is None
    assert "resumeId" not in event.server_props.model_dump()


# ── the request that writes the event ──────────────────────────────────────
#
# `LocalWorld`'s half of this is in `test_workflow_local_hook_resume.py`, since
# there the resume is not forwarded to a server but enforced on the spot.


def _carried(**overrides: Any) -> w.HookResumeInput:
    """The `hookInput` a delivery arrived with."""
    return w.HookResumeInput.model_validate(HOOK_INPUT_WIRE | overrides)


def _hook_received(carried: w.HookResumeInput | None = None) -> w.HookReceivedEvent:
    event = w.HookReceivedEventData(payload=ser.dehydrate({"ok": True}), token=TOKEN).into_event(
        "hook_1"
    )
    if carried is not None:
        event._queue_input = carried
    return event


def _vercel_route(world: VercelWorld):
    return respx.post(f"{world._base_url}/v3/runs/{RUN_ID}/events").mock(
        return_value=httpx.Response(200, content=cbor2.dumps({}), headers=CBOR),
    )


def _sent_body(route) -> dict:
    return cbor2.loads(route.calls.last.request.content)


@respx.mock
async def test_the_resume_rides_the_request_as_flat_keys() -> None:
    """Next to `remoteRefBehavior`, not inside the event: they tell the server
    which resume this write is, which is how it collapses two writes into one."""
    world = VercelWorld(token="test-token")
    route = _vercel_route(world)

    await world.events_create(RUN_ID, _hook_received(_carried()))

    body = _sent_body(route)
    assert body["resumeId"] == RESUME_ID
    assert body["resumePayloadDigest"] == DIGEST
    # Dated when the resume happened, which the id carries. Not when this write
    # happened -- the queue round trip is not the run's latency.
    assert body["occurredAt"] == datetime(2026, 8, 14, 3, 12, 3, 156000, tzinfo=UTC)
    # The event body is untouched -- both writers of one resume send the same one.
    assert body["eventData"]["token"] == TOKEN
    assert body["remoteRefBehavior"] == "lazy"


@respx.mock
async def test_a_write_without_a_resume_sends_no_extra_keys() -> None:
    """Every other write has to stay what it was. A `resumeId: null` would enrol
    each of them in a constraint it has no business in."""
    world = VercelWorld(token="test-token")
    route = _vercel_route(world)

    await world.events_create(RUN_ID, _hook_received())

    body = _sent_body(route)
    assert "resumeId" not in body
    assert "resumePayloadDigest" not in body
    assert "occurredAt" not in body


@respx.mock
async def test_a_resume_id_with_no_time_in_it_sends_no_date() -> None:
    """Absent, not null: the reader on the other side types this `undefined`, and
    a null would date the event to the epoch. Only ULID ids carry a time, and
    older producers and hand-written ids are not ULIDs."""
    world = VercelWorld(token="test-token")
    route = _vercel_route(world)

    await world.events_create(RUN_ID, _hook_received(_carried(resumeId="not-a-ulid")))

    body = _sent_body(route)
    assert body["resumeId"] == "not-a-ulid"
    assert "occurredAt" not in body


def test_a_ulid_resume_id_carries_the_time_it_was_minted() -> None:
    """Which is what the world dates the event by."""
    assert _carried().occurred_at() == datetime(2026, 8, 14, 3, 12, 3, 156000, tzinfo=UTC)


def test_a_non_ulid_resume_id_has_no_time_to_read() -> None:
    assert _carried(resumeId="not-a-ulid").occurred_at() is None


# ── what a run promises a later resumer ────────────────────────────────────


@pytest.fixture
def _reset_world():
    yield
    w.set_world(None)


async def test_a_run_we_create_says_its_consumer_writes_the_event(
    tmp_path, monkeypatch, _reset_world
) -> None:
    """Which is what lets a resume of this run take the fast path at all: the
    producer reads this off the run and falls back to sequential without it.

    That `queueNamespace` survives beside it is covered by
    `test_workflow_queue_namespace.py`, which asserts the whole context.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = core.Workflows(as_vercel_job=False)

    @registry.workflow
    async def example() -> None:
        pass

    class _World(LocalWorld):
        async def queue(self, queue_name: str, message: w.QueuePayload, **kw: Any) -> str:
            return "msg_1"

    world = _World()
    w.set_world(world)
    run = await runtime.start(example)

    stored = await world.runs_get(run.run_id)
    assert stored.execution_context is not None
    assert stored.execution_context["hookResumeInputVersion"] == w.HOOK_RESUME_INPUT_VERSION
