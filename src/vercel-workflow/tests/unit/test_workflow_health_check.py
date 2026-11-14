"""The queue health-check probe, and what answering it may claim.

`workflow health`, `workflow inspect run` and a cross-deployment `start()` all
publish a probe onto the workflow topic and then poll a stream for the answer.

The probe carries no ``runId``, so before this existed it was not merely
unanswered -- parsing it as an invoke payload raised, the message was never
acked, and the queue redelivered it forever.

Its reader gates capability decisions on individual fields, so the ones this SDK
deliberately omits are asserted as directly as the ones it sends.
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Sequence
from typing import Any
from unittest.mock import AsyncMock, Mock

import pydantic
import pytest

from vercel.workflow._internal import core, deployment, encryption, runtime, world as w
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


async def test_the_answer_claims_only_its_audited_core_compatibility(registry) -> None:
    """The synthetic version is a wire capability claim, not our package version."""
    world = StreamsWorld()
    w.set_world(world)

    await _probe(registry)

    body = json.loads(world.writes[0][2])
    assert body["workflowCoreVersion"] == w.WORKFLOW_CORE_COMPAT_VERSION
    assert "encryptionPublicKey" not in body


async def test_the_answer_advertises_the_hook_resume_input_version(registry) -> None:
    """The one of the three omissions that has since become claimable. A
    cross-deployment `start()` stamps the *target's* value onto the run it
    creates, so this is what lets a resume of such a run take the parallel path
    -- and it is only honest because the consumer re-ensures `hook_received`
    from the queue message's `hookInput`."""
    world = StreamsWorld()
    w.set_world(world)

    await _probe(registry)

    body = json.loads(world.writes[0][2])
    assert body["hookResumeInputVersion"] == w.HOOK_RESUME_INPUT_VERSION


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


@pytest.mark.parametrize("field", ["probeRunId", "runId"])
async def test_a_probe_naming_a_run_publishes_its_encryption_key(registry, field) -> None:
    key = bytes(range(32))

    class EncryptedWorld(StreamsWorld):
        async def run_key(self, run_id: str, *, deployment_id: str | None = None) -> bytes | None:
            assert run_id == "wrun_not_created_yet"
            return key

    world = EncryptedWorld()
    w.set_world(world)

    await _probe(registry, **{field: "wrun_not_created_yet"})

    body = json.loads(world.writes[0][2])
    expected = base64.b64encode(encryption.derive_run_key_pair(key).public_key).decode()
    assert body["encryptionPublicKey"] == expected


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


async def test_probe_cache_does_not_reuse_run_public_key():
    world = Mock(spec=w.World)
    world.create_run_id.return_value = "correlation"
    world.queue = AsyncMock()

    async def read(*args):
        body = json.dumps(
            {"healthy": True, "specVersion": 5, "encryptionPublicKey": "key"}
        ).encode()
        yield body[:8]
        yield body[8:]

    world.streams_get.side_effect = read
    first = await deployment._probe(world, "dpl_target", "wrun_first", "orders")
    second = await deployment._probe(world, "dpl_target", "wrun_second", "orders")
    assert first is not None and second is not None
    assert first["encryptionPublicKey"] == "key"
    assert "encryptionPublicKey" not in second
    assert world.queue.await_count == 1
    args, kwargs = world.queue.call_args
    assert args[0] == w.get_queue_name("health_check", "orders")
    assert args[1].model_dump()["probeRunId"] == "wrun_first"
    assert kwargs == {"deployment_id": "dpl_target", "spec_version": 1}


@pytest.mark.parametrize("retry_delay,retry_duration", [(30, 0), (59, 2)])
async def test_probe_miss_shortens_retry_budget_without_extending_miss(
    monkeypatch, retry_delay, retry_duration
):
    import anyio

    clock = [1000.0]
    monkeypatch.setattr(deployment, "time", Mock(monotonic=lambda: clock[0]))
    world = Mock(spec=w.World)
    world.create_run_id.return_value = "correlation"

    async def enqueue(queue_name, message, **kwargs):
        if message.probe_run_id == "run_2":
            clock[0] += retry_duration
        raise RuntimeError("unavailable")

    world.queue = AsyncMock(side_effect=enqueue)
    budgets = []
    original = anyio.move_on_after

    def timeout(seconds):
        budgets.append(seconds)
        return original(seconds)

    monkeypatch.setattr(deployment.anyio, "move_on_after", timeout)
    assert await deployment._probe(world, "missing", "run_1", None) is None
    clock[0] += retry_delay
    assert await deployment._probe(world, "missing", "run_2", None) is None
    clock[0] = max(clock[0], 1060)
    assert await deployment._probe(world, "missing", "run_3", None) is None
    assert budgets == [10, 2, 10]


async def test_probe_success_replaces_recent_miss(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(deployment, "time", Mock(monotonic=lambda: clock[0]))
    world = Mock(spec=w.World)
    world.create_run_id.return_value = "correlation"
    world.queue = AsyncMock(side_effect=[RuntimeError("unavailable"), None])

    async def read(*args):
        yield b'{"healthy":true,"specVersion":5,"encryptionPublicKey":"per-run"}'

    world.streams_get.side_effect = read
    assert await deployment._probe(world, "target", "run_1", None) is None
    clock[0] += 30
    recovered = await deployment._probe(world, "target", "run_2", None)
    assert recovered is not None and recovered["encryptionPublicKey"] == "per-run"
    clock[0] += 31
    cached = await deployment._probe(world, "target", "run_3", None)
    assert cached is not None and cached["specVersion"] == 5
    assert "encryptionPublicKey" not in cached
    assert world.queue.await_count == 2


@pytest.mark.parametrize(
    "body,expected",
    [
        (b"", None),
        (b"healthy", {"healthy": True, "format": "text"}),
        (
            b'Workflow SDK "old" endpoint is healthy',
            {"healthy": True, "format": "text"},
        ),
        (b"null", None),
        (b"[]", None),
        (b'{"healthy":1}', None),
        (b'{"healthy":false}', {"healthy": False, "format": "json"}),
        (
            b'{"healthy":true,"specVersion":3}',
            {"healthy": True, "specVersion": 3, "format": "json"},
        ),
    ],
)
def test_probe_response_formats(body, expected):
    assert deployment._parse_response(body) == expected


async def test_probe_cache_is_scoped_and_expires(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(deployment, "time", Mock(monotonic=lambda: clock[0]))

    async def read(*args):
        yield b'{"healthy":true,"specVersion":5,"encryptionPublicKey":"per-run"}'

    def make_world():
        world = Mock(spec=w.World)
        world.create_run_id.return_value = "correlation"
        world.queue = AsyncMock()
        world.streams_get.side_effect = read
        return world

    first, second = make_world(), make_world()
    for world, namespace in [(first, None), (first, "orders"), (second, None)]:
        answer = await deployment._probe(world, "dpl_target", "first_run", namespace)
        assert answer is not None and answer["encryptionPublicKey"] == "per-run"
    assert first.queue.await_count == 2
    assert second.queue.await_count == 1
    cached = await deployment._probe(first, "dpl_target", "second_run", None)
    assert cached is not None and "encryptionPublicKey" not in cached
    assert first.queue.await_count == 2
    clock[0] += 601
    renewed = await deployment._probe(first, "dpl_target", "third_run", None)
    assert renewed is not None and renewed["encryptionPublicKey"] == "per-run"
    assert first.queue.await_count == 3


async def test_probe_cache_evicts_oldest_deployment(monkeypatch):
    monkeypatch.setattr(deployment, "_PROBE_CACHE_MAX_ENTRIES", 2)
    world = Mock(spec=w.World)
    world.create_run_id.return_value = "correlation"
    world.queue = AsyncMock()

    async def read(*args):
        yield b'{"healthy":true,"specVersion":5}'

    world.streams_get.side_effect = read
    for target in ("a", "b", "c"):
        await deployment._probe(world, target, "run", None)
    assert list(deployment._cache[world]) == [("b", None), ("c", None)]


async def test_probe_keeps_slow_response_stream_open(monkeypatch):
    """A pause between chunks must not discard the reply and restart the read."""
    import anyio

    monkeypatch.setattr(deployment, "_PROBE_TIMEOUT", 3)
    world = Mock(spec=w.World)
    world.create_run_id.return_value = "correlation"
    world.queue = AsyncMock()
    closed = []

    async def read(*args):
        try:
            yield b'{"healthy":'
            await anyio.sleep(1.05)
            yield b'true,"specVersion":5}'
        finally:
            closed.append(True)

    world.streams_get.side_effect = read
    assert await deployment._probe(world, "target", "run", None) == {
        "healthy": True,
        "specVersion": 5,
        "format": "json",
    }
    world.streams_get.assert_called_once_with("wrun_hc_correlation", "__health_check__correlation")
    assert closed == [True]


@pytest.mark.parametrize("first_read", ["error", "empty", "invalid"])
async def test_probe_retries_unsuccessful_reads_without_republishing(monkeypatch, first_read):
    monkeypatch.setattr(deployment, "_PROBE_RETRY_INTERVAL", 0)
    world = Mock(spec=w.World)
    world.create_run_id.return_value = "correlation"
    world.queue = AsyncMock()
    reads = []
    closed = []

    async def read(*args):
        reads.append(args)
        try:
            if len(reads) == 1:
                if first_read == "error":
                    yield b'{"healthy":'
                    raise RuntimeError("Connection dropped before EOF")
                if first_read == "invalid":
                    yield b'{"healthy":1}'
                return
            yield b'{"healthy":true,"specVersion":5}'
        finally:
            closed.append(True)

    world.streams_get.side_effect = read
    assert await deployment._probe(world, "target", "run", None) == {
        "healthy": True,
        "specVersion": 5,
        "format": "json",
    }
    assert reads == [("wrun_hc_correlation", "__health_check__correlation")] * 2
    assert closed == [True, True]
    world.queue.assert_awaited_once()


async def test_probe_timeout_closes_stream(monkeypatch):
    import anyio

    monkeypatch.setattr(deployment, "_PROBE_TIMEOUT", 0.01)
    world = Mock(spec=w.World)
    world.create_run_id.return_value = "correlation"
    world.queue = AsyncMock()
    closed = []

    async def read(*args):
        try:
            await anyio.sleep_forever()
            yield b"unreachable"
        finally:
            closed.append(True)

    world.streams_get.side_effect = read
    assert await deployment._probe(world, "target", "run", None) is None
    assert closed == [True]


@pytest.mark.parametrize("stage", ["queue", "stream"])
async def test_probe_does_not_swallow_caller_cancellation(stage):
    import anyio

    world = Mock(spec=w.World)
    world.create_run_id.return_value = "correlation"
    closed = []

    async def enqueue(*args, **kwargs):
        if stage == "queue":
            await anyio.sleep_forever()

    async def read(*args):
        try:
            await anyio.sleep_forever()
            yield b"unreachable"
        finally:
            closed.append(True)

    world.queue = AsyncMock(side_effect=enqueue)
    world.streams_get.side_effect = read
    with anyio.move_on_after(0.01) as scope:
        await deployment._probe(world, "target", "run", None)
        pytest.fail("Cancellation should propagate")
    assert scope.cancelled_caught
    assert closed == ([True] if stage == "stream" else [])
    assert ("target", None) not in deployment._cache[world]
