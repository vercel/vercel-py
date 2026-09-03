"""The consumer half of lazy hook resume, at the handler.

``resumeHook()``'s parallel fast path writes ``hook_received`` and publishes the
workflow queue message concurrently, so this delivery routinely arrives before
the event is durable. The payload rides the message for that case. What makes it
load-bearing rather than an optimization: the producer publishes exactly *one*
message, so a delivery that replays a log without the payload suspends the run
with nothing left to wake it -- the resume is lost, not delayed.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any

import pytest

from tests.payloads import PLAIN_ENCODER
from vercel._internal.core.polyfills import UTC
from vercel.workflow._internal import core, runtime, world as w

from ..world_stubs import NoStreams

NOW = datetime(2026, 1, 1, tzinfo=UTC)
RUN_ID = "wrun_test"
TOKEN = "tok-abc"
# A real ULID off a captured delivery; it encodes 2026-08-14T03:12:03.156Z.
RESUME_ID = "01KZZ42P2MR25DYSW1MTMMACA3"
OTHER_RESUME_ID = "01KZZ42P2MR25DYSW1MTMMACA4"
DIGEST = "66c534074063d7f2dd180074e1f622f2190f830094837ab86d056913eb42560a"
PAYLOAD = b'devl[{"type":1},"subscribe"]'

_REGISTRY = core.Workflows(as_vercel_job=False)


@dataclasses.dataclass
class _Payload(core.BaseHook):
    type: str | None = None


@_REGISTRY.workflow
async def _hook_wf() -> str:
    payload = await _Payload.wait(token=TOKEN)
    return "got" if payload is not None else "none"


WORKFLOW_NAME = _hook_wf.workflow_id
WORKFLOW_QUEUE = w.get_queue_name(WORKFLOW_NAME)


def _minted_correlation_id(kind: str) -> str:
    """The id the body above will mint for its first suspension.

    Correlation ids are positional and seeded from the run id, so a fabricated
    `hook_created` has to be the one the body issues. One that is not stalls the
    delivery outright rather than failing it -- `resume()` yields to let the body
    catch up with a recorded event, and a body that never issues that call means
    it yields forever.
    """
    ctx = runtime.WorkflowOrchestratorContext(
        [],
        run_id=RUN_ID,
        seed=RUN_ID,
        started_at=int(NOW.timestamp() * 1000),
        registry=_REGISTRY,
    )
    return f"{kind}_{ctx.generate_ulid()}"


HOOK_ID = _minted_correlation_id("hook")


def _run(status: str = "running", **overrides: Any) -> w.WorkflowRun:
    fields: dict[str, Any] = {
        "runId": RUN_ID,
        "deploymentId": "dpl_1",
        "workflowName": WORKFLOW_NAME,
        "status": status,
        "specVersion": 6,
        "input": PLAIN_ENCODER.encode([]),
        "createdAt": NOW,
        "updatedAt": NOW,
        "startedAt": NOW,
    }
    return w.WorkflowRunAdaptor.from_wire(fields | overrides)


def _hook_input(**overrides: Any) -> dict[str, Any]:
    return {
        "resumeId": RESUME_ID,
        "hookId": HOOK_ID,
        "token": TOKEN,
        "payload": PAYLOAD,
        "payloadDigest": DIGEST,
        "deploymentId": "dpl_1",
    } | overrides


def _stamp(event: w.Event, *, event_id: str, resume_id: str | None = None) -> w.Event:
    props: dict[str, Any] = {"runId": RUN_ID, "eventId": event_id, "createdAt": NOW}
    if resume_id is not None:
        props["resumeId"] = resume_id
    return event.model_copy(update={"server_props": w.ServerProps.from_wire(props)})


def _hook_created() -> w.Event:
    return _stamp(w.HookCreatedEventData(token=TOKEN).into_event(HOOK_ID), event_id="evnt_1")


def _hook_received(resume_id: str | None, *, event_id: str = "evnt_2") -> w.Event:
    return _stamp(
        w.HookReceivedEventData(payload=PAYLOAD, token=TOKEN).into_event(HOOK_ID),
        event_id=event_id,
        resume_id=resume_id,
    )


class FakeWorld(NoStreams, w.World):
    """In-memory world recording what the handler wrote, and with what identity."""

    def __init__(
        self,
        *,
        events: list[w.Event] | None = None,
        hook_received_error: Exception | None = None,
    ) -> None:
        self.events: list[w.Event] = list(events or [])
        self.hook_received_error = hook_received_error
        self.resumes: list[w.HookReceivedEvent] = []
        self.queued: list[tuple[str, Any]] = []

    async def get_deployment_id(self) -> str:
        return "dpl_1"

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        self.queued.append((queue_name, message))
        return "msg_fake"

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        raise NotImplementedError

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        return _run()

    async def steps_get(self, run_id: str, step_id: str) -> w.WorkflowStep:
        raise NotImplementedError

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        raise NotImplementedError

    async def events_list(self, run_id: str, *, pagination: Any = None) -> Any:
        return w.PaginatedResult(data=list(self.events), cursor=None, has_more=False)

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        if data.event_type == "run_started":
            return w.EventResult(run=_run())
        if isinstance(data, w.HookReceivedEvent):
            self.resumes.append(data)
            if self.hook_received_error is not None:
                raise self.hook_received_error
            # A real world reads the carried input the same way, and stores the
            # resume id it names on the row.
            carried = data._queue_input
            self.events.append(
                _stamp(
                    data,
                    event_id=f"evnt_{len(self.events) + 1}",
                    resume_id=carried.resume_id if carried else None,
                )
            )
            return w.EventResult(event=self.events[-1])
        self.events.append(data)
        return w.EventResult()


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


async def _invoke(*, hook_input: dict[str, Any] | None = None) -> w.QueueContinuation | None:
    message: dict[str, Any] = {"runId": RUN_ID}
    if hook_input is not None:
        message["hookInput"] = hook_input
    return await runtime.workflow_handler(
        message,
        attempt=1,
        queue_name=WORKFLOW_QUEUE,
        message_id="msg_1",
        registry=_REGISTRY,
    )


# ── the write ──────────────────────────────────────────────────────────────


async def test_a_carried_payload_is_materialized_before_replay() -> None:
    """The whole fix. Without it the body replays over a log with no payload in
    it, suspends, and nothing is left to wake the run."""
    fake = FakeWorld(events=[_hook_created()])
    w.set_world(fake)

    await _invoke(hook_input=_hook_input())

    assert len(fake.resumes) == 1
    event = fake.resumes[0]
    assert event.correlation_id == HOOK_ID
    assert event.event_data.payload == PAYLOAD
    # The token the producer would have written, so the two writers of one
    # resume cannot disagree on the event body.
    assert event.event_data.token == TOKEN
    # The whole carried input rides along, which is how the world learns which
    # resume this write is. What each world does with it is its own test.
    assert event._queue_input is not None
    assert event._queue_input.resume_id == RESUME_ID
    assert event._queue_input.payload_digest == DIGEST


async def test_the_event_carries_the_runs_spec_version() -> None:
    """We are materializing another writer's event, whose payload is encoded to
    the run's version -- labelling it with ours would misdescribe those bytes."""
    fake = FakeWorld(events=[_hook_created()])
    w.set_world(fake)

    await _invoke(hook_input=_hook_input())

    event = fake.resumes[0]
    assert event.spec_version == 6


# ── when not to write ──────────────────────────────────────────────────────


async def test_the_producers_write_having_landed_skips_the_re_ensure() -> None:
    """A round trip for nothing otherwise, on what is the common case whenever
    the producer's write wins the race."""
    fake = FakeWorld(events=[_hook_created(), _hook_received(RESUME_ID)])
    w.set_world(fake)

    await _invoke(hook_input=_hook_input())

    assert fake.resumes == []


async def test_another_resume_in_the_log_does_not_count_as_this_one() -> None:
    """A hook iterated as a stream takes many payloads. Matching on the hook
    rather than the resume would deliver only the first."""
    fake = FakeWorld(events=[_hook_created(), _hook_received(OTHER_RESUME_ID)])
    w.set_world(fake)

    await _invoke(hook_input=_hook_input())

    assert len(fake.resumes) == 1


async def test_a_payload_less_delivery_writes_nothing() -> None:
    """Every ordinary replay -- a step completion, a wait, a sequential resume
    -- reaches here with no `hookInput` and must be untouched."""
    fake = FakeWorld(events=[_hook_created()])
    w.set_world(fake)

    await _invoke()

    assert fake.resumes == []


# ── the two error outcomes, which are opposites ────────────────────────────


@pytest.mark.parametrize(
    "error",
    [w.HookNotFoundError(hook_id=HOOK_ID), w.RunExpiredError("run expired", status=410)],
)
async def test_a_dead_resume_is_consumed_not_retried(error: Exception) -> None:
    """The hook is gone or the run went terminal, so this resume's eligibility
    ended before we got here. Retrying could not change that; the message is
    acked and the delivery stops without replaying."""
    fake = FakeWorld(events=[_hook_created()], hook_received_error=error)
    w.set_world(fake)

    assert await _invoke(hook_input=_hook_input()) is None
    # Nothing was replayed: a `hook_created` would have been flushed otherwise.
    assert [e.event_type for e in fake.events] == ["hook_created"]


async def test_a_conflict_is_left_for_a_redelivery() -> None:
    """The constraint exists but its event is not observable yet -- the
    producer's parallel write is still in flight. Transient, and the one case
    where acking would throw away the only copy of the payload, so it has to
    come out of the handler unswallowed even though every other write in the
    module swallows it."""
    fake = FakeWorld(
        events=[_hook_created()], hook_received_error=w.EntityConflictError("in flight")
    )
    w.set_world(fake)

    with pytest.raises(w.EntityConflictError):
        await _invoke(hook_input=_hook_input())


async def test_a_resume_id_that_means_something_else_is_not_retried() -> None:
    """The opposite outcome to the one above, for the conflict a redelivery cannot
    resolve."""
    fake = FakeWorld(
        events=[_hook_created()],
        hook_received_error=w.HookResumeConflictError("already recorded for a different hook"),
    )
    w.set_world(fake)

    assert await _invoke(hook_input=_hook_input()) is None
    # Nothing replayed: a `hook_created` would have been flushed otherwise.
    assert [e.event_type for e in fake.events] == ["hook_created"]


async def test_the_permanent_conflict_still_reads_as_a_conflict() -> None:
    """Handlers elsewhere catch `EntityConflictError` and must keep catching it."""
    assert issubclass(w.HookResumeConflictError, w.EntityConflictError)
