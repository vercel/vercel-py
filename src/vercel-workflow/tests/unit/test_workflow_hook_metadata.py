"""Metadata attached to a hook at ``wait()``, and how a resumer reads it back.

``createHook({ metadata })`` records arbitrary data *on the hook*, not on its
payload, and whoever resumes the hook reads it off the hook entity to decide
what to send. It is often the only channel a run has to that resumer, so the
value has to survive the whole way: authoring API -> ``hook_created`` event ->
hook entity -> ``get_hook_by_token()``.

Most of this drives that path against a real ``LocalWorld``, from a body that
really suspends, so both halves are asserted where they are actually used --
the bytes on the entity, which is what a TypeScript resumer reads, and the
decoded value this SDK's own resumer gets.
"""

from __future__ import annotations

import os

import pydantic
import pytest

from vercel.workflow import BaseHook, HookNotFoundError, get_hook_by_token
from vercel.workflow._internal import core, runtime, serialization as ser, world as w
from vercel.workflow._internal.worlds import local as local_mod

TOKEN = "order-42"
METADATA = {"customData": "orange"}

registry = core.Workflows(as_vercel_job=False)


class Approval(BaseHook, pydantic.BaseModel):
    decision: str


@registry.workflow
async def approvals() -> str:
    """Module level, because the sandbox re-imports the body by qualname."""
    approval = await Approval.wait(token=TOKEN, metadata=METADATA)
    return approval.decision if approval is not None else "disposed"


class _RecordingLocalWorld(local_mod.LocalWorld):
    """Real storage; the outbound queue and the run key are the only stubs.

    ``queue`` would otherwise stand up the embedded queue service, and counting
    lookups is how the "pass the hook back" path is checked for what it claims
    to save.
    """

    def __init__(self, run_key: bytes | None = None) -> None:
        super().__init__()
        self.queued: list[tuple[str, w.QueuePayload]] = []
        self.lookups: list[str] = []
        self._run_key = run_key

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs) -> str:
        self.queued.append((queue_name, message))
        return "msg_test"

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        self.lookups.append(token)
        return await super().hooks_get_by_token(token)

    async def run_key(self, run_id: str, *, deployment_id: str | None = None) -> bytes | None:
        return self._run_key


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


@pytest.fixture
async def registered(tmp_path, monkeypatch) -> _RecordingLocalWorld:
    """A run of `approvals` suspended on its hook, on a real ``LocalWorld``.

    Nothing about the hook is hand-written: the body ran, the flush wrote the
    ``hook_created`` event, and the world registered the token from it.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = _RecordingLocalWorld()
    w.set_world(world)

    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="",
            workflowName=approvals.workflow_id,
            input=ser.dehydrate([]),
        ).into_event(),
    )
    assert created.run is not None
    await runtime.workflow_handler(
        w.WorkflowInvokePayload(runId=created.run.run_id).model_dump(by_alias=True),
        attempt=1,
        queue_name=w.get_queue_name(approvals.workflow_id),
        message_id="msg_1",
        registry=registry,
    )
    world.lookups.clear()
    return world


def _context() -> runtime.WorkflowOrchestratorContext:
    return runtime.WorkflowOrchestratorContext(
        [],
        run_id="wrun_test",
        seed="wrun_test",
        started_at=0,
        registry=registry,
    )


# ── writing it ─────────────────────────────────────────────────────────────


def test_metadata_is_encoded_when_the_hook_is_created() -> None:
    ctx = _context()

    event = ctx.create_hook(TOKEN, Approval, metadata=METADATA)

    hook = ctx.hooks[event._correlation_id]
    assert hook.metadata == ser.dehydrate(METADATA)


def test_an_unserializable_value_fails_at_the_wait_call() -> None:
    """Rather than in the flush, where nothing names the offending hook."""
    ctx = _context()

    with pytest.raises(ser.SerializationError):
        ctx.create_hook(TOKEN, Approval, metadata=object())


def test_a_hook_without_metadata_omits_the_field() -> None:
    """There is no way to record a null, matching `createHook()` treating
    `undefined` as absent -- and an omitted field is what TypeScript writes."""
    ctx = _context()

    event = ctx.create_hook(TOKEN, Approval)

    hook = ctx.hooks[event._correlation_id]
    assert hook.metadata is None
    dumped = w.HookCreatedEventData(token=TOKEN).model_dump(by_alias=True)
    assert "metadata" not in dumped


async def test_metadata_reaches_the_hook_entity(registered) -> None:
    """The bytes a TypeScript resumer reads: one serialized payload on the
    entity, in the same format every other payload uses."""
    hook = await w.get_world().hooks_get_by_token(TOKEN)

    assert hook.metadata is not None
    assert hook.metadata.startswith(ser.DEVALUE_V1)
    assert ser.hydrate(hook.metadata, what="the metadata of the hook") == METADATA


# ── reading it back ────────────────────────────────────────────────────────


async def test_the_resumer_reads_the_metadata_back(registered) -> None:
    hook = await get_hook_by_token(TOKEN)

    assert hook.metadata == METADATA
    assert hook.token == TOKEN
    assert hook.run_id.startswith("wrun_")
    assert hook.hook_id.startswith("hook_")
    assert hook.created_at == (await registered.hooks_get_by_token(TOKEN)).created_at


async def test_a_hook_without_metadata_reads_as_none(registered) -> None:
    world = w.get_world()
    await world.events_create(
        (await get_hook_by_token(TOKEN)).run_id,
        w.HookCreatedEventData(token="plain").into_event("hook_plain"),
    )

    assert (await get_hook_by_token("plain")).metadata is None


async def test_an_unregistered_token_is_not_found(registered) -> None:
    """Same error a disposed hook gives: nothing distinguishes them, so a token
    cannot be probed for whether it was ever real."""
    with pytest.raises(HookNotFoundError):
        await get_hook_by_token("no-such-token")


async def test_encrypted_metadata_is_opened_with_the_run_key(tmp_path, monkeypatch) -> None:
    """Metadata written by a deployment that encrypts. The key costs an API
    round trip on the Vercel world, so it is resolved only for a payload that
    needs one -- which the plaintext tests above cover from the other side."""
    # Imported here, not at module scope: the sandbox re-executes this module
    # to reach the workflow body above, and `cryptography` cannot be imported
    # inside it.
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    key = bytes(range(32))
    world = _RecordingLocalWorld(run_key=key)
    w.set_world(world)
    nonce = os.urandom(12)
    sealed = ser.ENCRYPTED + nonce + AESGCM(key).encrypt(nonce, ser.dehydrate(METADATA), None)

    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="dpl_1", workflowName="wf", input=ser.dehydrate([])
        ).into_event(),
    )
    assert created.run is not None
    await world.events_create(
        created.run.run_id,
        w.HookCreatedEventData(token=TOKEN, metadata=sealed).into_event("hook_0"),
    )

    assert (await get_hook_by_token(TOKEN)).metadata == METADATA


async def test_resuming_with_the_hook_skips_a_second_lookup(registered) -> None:
    """What the public `Hook` is for: the resumer that read the metadata to
    decide what to send does not pay for the hook twice."""
    hook = await get_hook_by_token(TOKEN)
    registered.lookups.clear()

    resumed = await Approval(decision="ok").resume(hook)

    assert registered.lookups == []
    assert resumed == hook
    # The payload really did land on the run's log, addressed to the hook.
    events = (await registered.events_list(hook.run_id)).data
    (received,) = [e for e in events if isinstance(e, w.HookReceivedEvent)]
    assert received.correlation_id == hook.hook_id
    assert ser.hydrate(received.event_data.payload, what="the payload") == {"decision": "ok"}


async def test_resuming_with_a_token_still_answers_with_the_hook(registered) -> None:
    resumed = await Approval(decision="ok").resume(TOKEN)

    assert registered.lookups == [TOKEN]
    assert resumed.metadata == METADATA
