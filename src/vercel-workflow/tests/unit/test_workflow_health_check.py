"""The queue health-check probe, and what answering it may claim.

`workflow health`, `workflow inspect run` and a cross-deployment `start()` all
publish a probe onto the workflow topic and then poll a stream for the answer.

The probe carries no ``runId``, so before this existed it was not merely
unanswered -- parsing it as an invoke payload raised, the message was never
acked, and the queue redelivered it forever.

Its reader gates capability decisions on individual fields, so the three fields
this SDK deliberately omits are asserted as directly as the ones it sends.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from typing import Any

import pydantic
import pytest

from vercel.workflow._internal import core, runtime, world as w
from vercel.workflow._internal.worlds import local as local_mod

from ..world_stubs import NoStreams

CID = "01K1HEALTHCHECK0000000000"
RUN_ID = f"wrun_hc_{CID}"
STREAM = f"__health_check__{CID}"

# The topic the prober publishes to: one queue name for every deployment, since
# a probe names no workflow.
HEALTH_QUEUE = "__wkf_workflow_health_check"


class StreamsWorld(NoStreams, w.World):
    """Records what reaches the stream surface; everything else raises.

    ``runs_get`` raising is load-bearing: it proves the probe is dispatched
    before the run is read.
    """

    def __init__(self) -> None:
        self.writes: list[tuple[str, str, bytes]] = []
        self.closes: list[tuple[str, str]] = []

    async def streams_write(self, run_id: str, name: str, chunk: bytes) -> None:
        self.writes.append((run_id, name, chunk))

    async def streams_write_multi(self, run_id: str, name: str, chunks: Sequence[bytes]) -> None:
        for chunk in chunks:
            await self.streams_write(run_id, name, chunk)

    async def streams_close(self, run_id: str, name: str) -> None:
        self.closes.append((run_id, name))

    # -- unused World surface -------------------------------------------
    async def get_deployment_id(self) -> str:
        return ""

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        raise NotImplementedError

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

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        raise NotImplementedError

    async def events_list(self, run_id: str, *, pagination: Any = None) -> Any:
        raise NotImplementedError


class NoStreamsWorld(StreamsWorld):
    """`StreamsWorld` with the stream surface taken away."""

    async def streams_write(self, run_id: str, name: str, chunk: bytes) -> None:
        raise NotImplementedError

    async def streams_close(self, run_id: str, name: str) -> None:
        raise NotImplementedError


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


@pytest.fixture
def registry() -> core.Workflows:
    return core.Workflows(as_vercel_job=False)


async def _probe(registry: core.Workflows, **extra: Any) -> w.QueueContinuation | None:
    return await runtime.workflow_handler(
        {"__healthCheck": True, "correlationId": CID, **extra},
        attempt=1,
        queue_name=HEALTH_QUEUE,
        message_id="msg_probe",
        registry=registry,
    )


async def test_probe_is_answered_on_the_stream_the_prober_polls(registry) -> None:
    """One chunk, then a close, under the names both sides derive from the id.

    Neither name is ever sent, so a divergence is invisible until a real health
    check times out.
    """
    world = StreamsWorld()
    w.set_world(world)

    assert await _probe(registry) is None

    assert [(run_id, name) for run_id, name, _ in world.writes] == [(RUN_ID, STREAM)]
    assert world.closes == [(RUN_ID, STREAM)]


async def test_the_answer_is_plain_json(registry) -> None:
    """No devalue framing, no CBOR: the prober concatenates chunks and
    `JSON.parse`s the result."""
    world = StreamsWorld()
    w.set_world(world)

    await _probe(registry)

    body = json.loads(world.writes[0][2])
    # Its reader requires `healthy` to be a boolean specifically -- a truthy
    # value of another type is read as a malformed answer, not a healthy one.
    assert body["healthy"] is True
    assert body["correlationId"] == CID
    assert body["specVersion"] == w.SPEC_VERSION_CURRENT
    assert isinstance(body["timestamp"], int)


async def test_the_answer_claims_no_capability_it_does_not_have(registry) -> None:
    """The three deliberate omissions -- see `_health_check_response`. Each is
    read as a capability claim, and the consequence of a wrong one lands in the
    caller, not here."""
    world = StreamsWorld()
    w.set_world(world)

    await _probe(registry)

    body = json.loads(world.writes[0][2])
    assert "workflowCoreVersion" not in body
    assert "hookResumeInputVersion" not in body
    assert "encryptionPublicKey" not in body


async def test_a_probe_naming_a_run_is_still_a_probe(registry) -> None:
    """`runId` is set when the probe prepares a cross-deployment `start()`.

    That run does not exist yet, and such a payload also satisfies the invoke
    schema, whose only required field is `runId` -- which is what makes the
    dispatch order load-bearing rather than cosmetic.
    """
    world = StreamsWorld()
    w.set_world(world)

    assert await _probe(registry, runId="wrun_not_created_yet") is None

    assert world.closes == [(RUN_ID, STREAM)]


async def test_a_message_without_the_discriminator_is_not_a_probe(registry) -> None:
    """A correlation id alone must not be read as a probe: answering one
    instead of the work the message asked for would strand a run."""
    world = StreamsWorld()
    w.set_world(world)

    with pytest.raises(pydantic.ValidationError):
        await runtime.workflow_handler(
            {"correlationId": CID},
            attempt=1,
            queue_name=HEALTH_QUEUE,
            message_id="msg_not_a_probe",
            registry=registry,
        )

    assert world.writes == []


async def test_a_world_without_streams_acks_the_probe(registry, caplog) -> None:
    """Redelivering would only re-reach the same missing surface, so the probe
    is acked and the prober times out -- as it does against an endpoint that
    never answers."""
    w.set_world(NoStreamsWorld())

    with caplog.at_level(logging.WARNING, logger="vercel.workflow"):
        assert await _probe(registry) is None

    assert CID in caplog.text


async def test_local_world_stores_the_answer_where_the_prober_reads_it(
    registry, tmp_path, monkeypatch
) -> None:
    """The same exchange against a real `LocalWorld`.

    The prober reads the answer out of the shared ``.workflow-data`` directory,
    not over a connection it holds, so what is asserted is what it looks for:
    the run's stream registry names the stream, and the chunks come back as one
    closed chunk.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    w.set_world(world)

    await _probe(registry)

    assert await world.streams_list(RUN_ID) == [STREAM]

    page = await world.streams_get_chunks(RUN_ID, STREAM)
    assert page.done is True
    assert [chunk.index for chunk in page.data] == [0]
    assert json.loads(page.data[0].data)["correlationId"] == CID
