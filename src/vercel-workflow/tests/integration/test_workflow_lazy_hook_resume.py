"""Lazy hook resume end to end, against a producer that races us.

Nothing is faked. `LocalWorld` runs its embedded queue service in-process and
`workflow_entrypoint` subscribes the real combined handler to it, so a run here
goes through the same replay, flush and step dispatch a `vercel dev` run does.

What the tests supply is the *producer*: `resume_parallel` publishes the workflow
queue message carrying `hookInput` and only then writes the `hook_received`
event, which is the order upstream's `resumeHook()` fast path can land them in.
This SDK's own `resume_hook` cannot reproduce it -- it writes the event first and
enqueues second -- which is why the defect was invisible from this side until a
TypeScript driver drove the same app.

The two properties are opposites and both matter:

* the payload is delivered when the consumer wins the race (before the fix the
  run suspended here with nothing left to wake it -- the producer publishes
  exactly one message);
* it is delivered *once* when the producer's write lands too, because both
  writers converge on the `(runId, resumeId)` claim. Two events would hand the
  payload to the body twice, which is worse than the stall.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
from collections.abc import AsyncIterator

import pytest

import vercel.queue as vqs
from vercel.queue.testing import clear_subscriptions
from vercel.workflow._internal import core, runtime, serialization as ser, ulid, world as w
from vercel.workflow._internal.worlds import local as local_mod

RUN_DEADLINE_SECONDS = 30
TOKEN = "resume-token"
FINALLY_TOKEN = "finally-resume-token"
FINALLY_STEP_TOKEN = "finally-step-resume-token"
CANCELLED_ERROR_TOKEN = "cancelled-error-token"
REPLACED_CANCELLED_ERROR_TOKEN = "replaced-cancelled-error-token"

# Module level, not function level: replay re-imports the workflow's defining
# module by name inside the sandbox, so it has to live somewhere importable.
registry = core.Workflows(as_vercel_job=False)


@dataclasses.dataclass
class Ping(core.BaseHook):
    """Loose in the fields the producer leaves out, as a hook payload type has to
    be: the resumes below send `{n}` and then `{done}`."""

    n: int | None = None
    done: bool | None = None


@registry.step
async def double(*, n: int) -> int:
    return n * 2


@registry.workflow
async def collect() -> list[int]:
    """A step after every payload, which is the shape that stalled: the driver
    waits on the step, so a lost delivery is never repaired by a later one."""
    hook = Ping.wait(token=TOKEN)
    seen: list[int] = []
    async for payload in hook:
        if payload.done:
            break
        assert payload.n is not None
        seen.append(await double(n=payload.n))
    hook.dispose()
    return seen


@registry.workflow
async def receive_one_with_finally() -> int:
    hook = Ping.wait(token=FINALLY_TOKEN)
    try:
        payload = await hook
        assert payload is not None and payload.n is not None
        return payload.n
    finally:
        hook.dispose()


@registry.workflow
async def receive_one_with_finally_step() -> int:
    hook = Ping.wait(token=FINALLY_STEP_TOKEN)
    try:
        payload = await hook
        assert payload is not None and payload.n is not None
        return payload.n
    finally:
        await double(n=21)


@registry.workflow
async def catch_cancelled_error() -> int:
    hook = Ping.wait(token=CANCELLED_ERROR_TOKEN)
    try:
        payload = await hook
    except asyncio.CancelledError:
        return -1
    assert payload is not None and payload.n is not None
    return payload.n


@registry.workflow
async def replace_cancelled_error() -> int:
    hook = Ping.wait(token=REPLACED_CANCELLED_ERROR_TOKEN)
    try:
        payload = await hook
    except asyncio.CancelledError:
        raise RuntimeError("replaced workflow cancellation") from None
    assert payload is not None and payload.n is not None
    return payload.n


@pytest.fixture(autouse=True)
def _reset_world():
    # Queue subscriptions are process-global and refuse a second registration on
    # the same topic pattern, so this brackets the test rather than only
    # following it: everything in this package shares one worker process, and a
    # neighbour that leaked one would otherwise fail us instead of itself.
    saved = vqs.get_subscriptions()
    clear_subscriptions()
    try:
        yield
    finally:
        w.set_world(None)
        clear_subscriptions()
        for subscription in saved:
            vqs.subscribe(
                topic=subscription.topic,
                consumer_group=subscription.consumer_group,
                retry_after=subscription.retry_after_seconds,
                initial_delay=subscription.initial_delay_seconds,
                max_concurrency=subscription.max_concurrency,
                max_attempts=subscription.max_attempts,
            )(subscription.func)


@contextlib.asynccontextmanager
async def running_world(tmp_path, monkeypatch) -> AsyncIterator[local_mod.LocalWorld]:
    """A world whose embedded queue service this task opens and closes.

    Both halves have to happen in one task, which is why this is a context
    manager the test enters rather than a fixture: anyio refuses to exit a cancel
    scope from a task other than the one that entered it, and a fixture's
    teardown is not reliably the setup's task. `aclose()` in a fixture `finally`
    passes on 3.11+ and fails on 3.10.

    The queue client is opened here for the same reason -- otherwise the first
    delivery to publish opens it, from a task of its own.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    w.set_world(world)
    runtime.workflow_entrypoint(registry)
    await world._get_queue_client()
    try:
        yield world
    finally:
        await world.aclose()


@dataclasses.dataclass(frozen=True)
class Resume:
    """One resume as its producer sees it, so its two writes can be issued
    separately and in either order."""

    hook: w.Hook
    payload: bytes
    resume_id: str
    digest: str

    def as_input(self, deployment_id: str | None = None) -> w.HookResumeInput:
        """What both of this resume's writes carry."""
        return w.HookResumeInput(
            resume_id=self.resume_id,
            hook_id=self.hook.hook_id,
            token=self.hook.token,
            payload=self.payload,
            payload_digest=self.digest,
            deployment_id=deployment_id,
        )

    @classmethod
    def of(cls, hook: w.Hook, payload: dict) -> Resume:
        dehydrated = ser.dehydrate(payload)
        return cls(
            hook=hook,
            payload=dehydrated,
            resume_id=ulid.monotonic_factory()(None),
            digest=hashlib.sha256(dehydrated).hexdigest(),
        )


async def publish(world: local_mod.LocalWorld, resume: Resume) -> None:
    """The fast path's queue publish, carrying the payload."""
    run = await world.runs_get(resume.hook.run_id)
    await world.queue(
        w.get_queue_name(run.workflow_name, None),
        w.WorkflowInvokePayload(
            run_id=resume.hook.run_id,
            hook_input=resume.as_input(run.deployment_id),
        ),
    )


async def write_event(world: local_mod.LocalWorld, resume: Resume) -> None:
    """The fast path's direct `hook_received` write, carrying the same identity."""
    run = await world.runs_get(resume.hook.run_id)
    event = w.HookReceivedEvent(
        correlation_id=resume.hook.hook_id,
        event_data=w.HookReceivedEventData(payload=resume.payload, token=resume.hook.token),
        spec_version=run.spec_version or w.SPEC_VERSION_CURRENT,
    )
    event._queue_input = resume.as_input()
    await world.events_create(resume.hook.run_id, event)


async def wait_for_hook(world: local_mod.LocalWorld, token: str) -> w.Hook:
    """Until the run's first suspension has flushed its `hook_created`."""
    for _ in range(200):
        try:
            return await world.hooks_get_by_token(token)
        except w.HookNotFoundError:
            await asyncio.sleep(0.05)
    raise AssertionError(f"hook {token!r} was never registered")


async def wait_for_hook_without_run_finishing(
    world: local_mod.LocalWorld, run_id: str, token: str
) -> w.Hook:
    for _ in range(200):
        try:
            return await world.hooks_get_by_token(token)
        except w.HookNotFoundError:
            run = await world.runs_get(run_id)
            if run.status in {"completed", "failed", "cancelled"}:
                raise AssertionError(
                    f"run finished as {run.status!r} before hook {token!r} was registered"
                ) from None
            await asyncio.sleep(0.05)
    raise AssertionError(f"hook {token!r} was never registered")


async def event_types(world: local_mod.LocalWorld, run_id: str) -> list[str]:
    return [e.event_type for e in (await world.events_list(run_id)).data]


async def wait_for_event(
    world: local_mod.LocalWorld, run_id: str, event_type: str, *, count: int = 1
) -> None:
    for _ in range(RUN_DEADLINE_SECONDS * 10):
        types = await event_types(world, run_id)
        if types.count(event_type) >= count:
            return
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"run {run_id} never recorded {count} {event_type} event(s); "
        f"log is {await event_types(world, run_id)}"
    )


async def test_a_run_completes_on_carried_payloads_alone(tmp_path, monkeypatch) -> None:
    """The message is the only copy: the producer's direct write never lands.

    Before the fix this stalled on the first payload with
    ``run_created, run_started, hook_created`` and nothing after it, because the
    producer publishes exactly one message and this delivery consumed it.
    """
    async with running_world(tmp_path, monkeypatch) as world:
        run = await runtime.start(collect)
        hook = await wait_for_hook(world, TOKEN)

        await publish(world, Resume.of(hook, {"n": 1}))
        # The step is what proves the *body* saw the payload, not just the log.
        await wait_for_event(world, run.run_id, "step_completed")

        hook = await world.hooks_get_by_token(TOKEN)
        await publish(world, Resume.of(hook, {"n": 2}))
        await wait_for_event(world, run.run_id, "step_completed", count=2)

        hook = await world.hooks_get_by_token(TOKEN)
        await publish(world, Resume.of(hook, {"done": True}))

        result = await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS)
        assert result == [2, 4]
        assert (await event_types(world, run.run_id)).count("hook_received") == 3


async def test_dispose_in_finally_does_not_dispose_a_suspended_hook(tmp_path, monkeypatch) -> None:
    async with running_world(tmp_path, monkeypatch) as world:
        run = await runtime.start(receive_one_with_finally)
        hook = await wait_for_hook(world, FINALLY_TOKEN)

        await publish(world, Resume.of(hook, {"n": 7}))

        assert await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS) == 7


async def test_a_step_in_finally_runs_after_the_hook_resumes(tmp_path, monkeypatch) -> None:
    async with running_world(tmp_path, monkeypatch) as world:
        run = await runtime.start(receive_one_with_finally_step)
        hook = await wait_for_hook(world, FINALLY_STEP_TOKEN)

        await publish(world, Resume.of(hook, {"n": 7}))

        assert await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS) == 7
        assert (await event_types(world, run.run_id)).count("step_completed") == 1


async def test_catching_cancelled_error_cannot_complete_a_suspended_workflow(
    tmp_path, monkeypatch
) -> None:
    async with running_world(tmp_path, monkeypatch) as world:
        run = await runtime.start(catch_cancelled_error)
        hook = await wait_for_hook_without_run_finishing(world, run.run_id, CANCELLED_ERROR_TOKEN)

        await publish(world, Resume.of(hook, {"n": 7}))

        assert await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS) == 7


async def test_replacing_cancelled_error_cannot_fail_a_suspended_workflow(
    tmp_path, monkeypatch
) -> None:
    async with running_world(tmp_path, monkeypatch) as world:
        run = await runtime.start(replace_cancelled_error)
        hook = await wait_for_hook_without_run_finishing(
            world, run.run_id, REPLACED_CANCELLED_ERROR_TOKEN
        )

        await publish(world, Resume.of(hook, {"n": 7}))

        assert await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS) == 7


async def test_the_producers_own_write_converges_on_the_re_ensured_event(
    tmp_path, monkeypatch
) -> None:
    """Sequenced rather than raced, so the assertion is not a coin flip: the
    consumer's re-ensure lands first and the producer's direct write follows.
    Both carry one identity, so there must still be one event -- two would hand
    the payload to the body twice, which is worse than the stall."""
    async with running_world(tmp_path, monkeypatch) as world:
        run = await runtime.start(collect)
        hook = await wait_for_hook(world, TOKEN)
        resume = Resume.of(hook, {"n": 1})

        await publish(world, resume)
        await wait_for_event(world, run.run_id, "step_completed")
        await write_event(world, resume)

        assert (await event_types(world, run.run_id)).count("hook_received") == 1

        hook = await world.hooks_get_by_token(TOKEN)
        await publish(world, Resume.of(hook, {"done": True}))
        assert await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS) == [2]


async def test_the_re_ensured_event_records_the_resume_identity(tmp_path, monkeypatch) -> None:
    """Which is what a later delivery of the same resume recognizes, and what the
    claim is keyed on."""
    async with running_world(tmp_path, monkeypatch) as world:
        run = await runtime.start(collect)
        hook = await wait_for_hook(world, TOKEN)
        resume = Resume.of(hook, {"n": 7})

        await publish(world, resume)
        await wait_for_event(world, run.run_id, "step_completed")

        (received,) = [
            e for e in (await world.events_list(run.run_id)).data if e.event_type == "hook_received"
        ]
        assert received.server_props is not None
        assert received.server_props.resume_id == resume.resume_id
        assert isinstance(received, w.HookReceivedEvent)
        # The token the producer would have written, so the two writers of one resume
        # cannot disagree on the event body.
        assert received.event_data.token == TOKEN
