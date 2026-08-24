"""Sealed-log `noop` events (``specVersion`` 7).

Spec 7 hands a writer its slot position *before* the write that fills it, so
concurrent writers never race for a position -- and a writer that takes a
position and dies leaves a hole. The World's backend fills those holes with a
`noop` so the log a reader sees is still a dense prefix. Nothing else about
spec 7 reaches this SDK: we do not pre-assign positions and so never seal
anything, which leaves exactly one obligation, the reader half.

That half is two claims, and every test here is one of them:

* a seal parses -- it is a legal row of any spec-7 log, and a reader that
  cannot read the row cannot read the log;
* a seal is invisible to the run -- nothing is offered it, and above all its
  ``createdAt`` never becomes a time the workflow observed. That timestamp is
  the *sealer's* wall clock, which can postdate every real event around it, so
  a log whose hole was sealed has to replay identically to the same log whose
  hole its own writer filled. ``SEAL_TIME`` below is a minute into the future
  for that reason: every clock assertion would pass by accident if seals were
  stamped alongside the events they sit between.

Ports the client half of vercel/workflow#3634.
"""

from __future__ import annotations

import typing
from datetime import datetime, timedelta
from typing import Any

import pydantic
import pytest

from vercel._internal.core.polyfills import UTC
from vercel.workflow._internal import core, runtime, serialization as ser, world as w
from vercel.workflow._internal.worlds import local as local_mod

from ..world_stubs import NoStreams

RUN_ID = "wrun_test"
NOW = datetime(2026, 1, 1, tzinfo=UTC)
# Whoever seals a hole does it long after the writer that abandoned the
# position gave up, so a seal's clock can sit past every real event in the log.
SEAL_TIME = NOW + timedelta(seconds=60)


def _slot(position: int) -> str:
    """A slot number, zero-padded the way an event id carries it."""
    return f"{position:026d}"


def _seal_row(position: int, **overrides: Any) -> dict[str, Any]:
    """A seal as the backend writes it.

    Both ids are pure functions of the position -- that is what lets two
    sealers racing for one hole mint byte-identical rows -- so they are derived
    here rather than passed in.
    """
    slot = _slot(position)
    return {
        "eventType": "noop",
        "correlationId": f"noop_{slot}",
        "eventData": {"sealed": True},
        "specVersion": 7,  # SPEC_VERSION_SUPPORTS_SEALED_LOG
        "runId": RUN_ID,
        "eventId": f"evnt_{slot}",
        "createdAt": SEAL_TIME,
    } | overrides


def _seal(position: int, **overrides: Any) -> w.Event:
    return w.EventAdaptor.validate_python(_seal_row(position, **overrides))


# ── the row ────────────────────────────────────────────────────────────────


def test_a_seal_parses_out_of_the_read_union() -> None:
    event = _seal(6)

    assert isinstance(event, w.NoopEvent)
    assert event.event_type == "noop"
    assert event.correlation_id == f"noop_{_slot(6)}"
    assert event.event_data is not None and event.event_data.sealed is True
    assert event.server_props is not None
    # It holds a real slot: lengths, cursors and pagination all count it, so
    # its id is the position, not a synthetic one.
    assert event.server_props.event_id == f"evnt_{_slot(6)}"
    assert event.server_props.created_at == SEAL_TIME


def test_a_seal_parses_without_event_data() -> None:
    """`eventData` carries nothing anyone reads, so a backend omitting it still
    means the same thing and must not fail the whole page."""
    row = _seal_row(6)
    del row["eventData"]

    event = w.EventAdaptor.validate_python(row)

    assert isinstance(event, w.NoopEvent)
    assert event.event_data is None


def test_a_seal_keeps_event_data_fields_it_has_not_heard_of() -> None:
    """The shape belongs to whichever backend sealed the slot. A reader whose
    only interest is skipping the row has no business rejecting it."""
    event = _seal(6, eventData={"sealed": True, "sealedBy": "reader-page-fill"})

    assert isinstance(event, w.NoopEvent)
    assert event.event_data is not None
    assert event.event_data.model_extra == {"sealedBy": "reader-page-fill"}


def test_a_seal_is_not_something_this_sdk_can_create() -> None:
    """Server-originated: the backend's read path writes it and rejects a POST
    of one. Leaving it out of the create union is how that is said here."""
    assert w.NoopEvent in typing.get_args(typing.get_args(w.Event)[0])
    assert w.NoopEvent not in typing.get_args(w.CreateEventRequest)


def test_the_read_ceiling_is_the_sealed_log_version() -> None:
    """Reading spec 7 is the point: `@workflow/world-local` and
    `@workflow/world-vercel` both stamp it now, so a run created by a
    TypeScript driver arrives labelled 7 whether or not it contains a seal."""
    assert w.SPEC_VERSION_MAX_SUPPORTED == 7
    assert w.RunStartedEvent(specVersion=7).spec_version == 7

    with pytest.raises(pydantic.ValidationError, match="less than or equal to 7"):
        w.RunStartedEvent(specVersion=8)


# ── storage: a seal is a row like any other ────────────────────────────────


async def test_a_sealed_slot_round_trips_through_the_local_world(tmp_path, monkeypatch) -> None:
    """The local world never seals -- it allocates each id at the commit that
    fills it, so it has no holes -- but a spec-7 log is a legal resident of any
    world's storage, and a run may be handed to it by something that does.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()

    result = await world.events_create(
        None,
        w.RunCreatedEvent(
            eventData=w.RunCreatedEventData(
                deploymentId="dpl_1",
                workflowName="workflow//./src/wf//main",
                input=ser.dehydrate([]),
            ),
            specVersion=7,
        ),
    )
    assert result.run is not None
    run_id = result.run.run_id
    await world.events_create(run_id, _seal(2))
    await world.events_create(run_id, w.RunStartedEvent(specVersion=7))

    stored = (await world.events_list(run_id)).data

    # Ordering continues straight past it, which is the property sealing exists
    # to preserve.
    assert [event.event_type for event in stored] == ["run_created", "noop", "run_started"]
    assert isinstance(stored[1], w.NoopEvent)
    assert stored[1].correlation_id == f"noop_{_slot(2)}"


# ── the log the replay reads ───────────────────────────────────────────────


class _PagedEventsWorld(NoStreams, w.World):
    """A world that hands back canned pages of a run's log."""

    def __init__(self, pages: list[list[w.Event]], *, cursors: list[str | None]) -> None:
        self.pages = pages
        self.cursors = cursors
        self.requested: list[str | None] = []

    async def get_deployment_id(self) -> str:
        return "dpl_1"

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

    async def events_list(self, run_id: str, *, pagination: Any = None) -> Any:
        index = len(self.requested)
        self.requested.append(pagination.cursor if pagination else None)
        return w.PaginatedResult(
            data=list(self.pages[index]),
            cursor=self.cursors[index],
            hasMore=index + 1 < len(self.pages),
        )

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        raise NotImplementedError


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


def _stamp(event: w.Event, position: int, *, created_at: datetime = NOW) -> w.Event:
    return event.model_copy(
        update={
            "server_props": w.ServerProps(
                runId=RUN_ID, eventId=f"evnt_{_slot(position)}", createdAt=created_at
            )
        }
    )


async def test_the_loader_drops_seals_and_keeps_everything_else_in_order() -> None:
    """Seals at the head, in the middle (consecutively) and at the tail: none of
    them reaches the replay, and the events around them keep their order."""
    real = [
        _stamp(w.RunStartedEvent(), 2),
        _stamp(w.WaitCreatedEventData(resumeAt=NOW).into_event("wait_1"), 5),
        _stamp(w.WaitCompletedEvent(correlationId="wait_1"), 8),
    ]
    world = _PagedEventsWorld(
        [[_seal(1), real[0], _seal(3), _seal(4), real[1], real[2], _seal(9)]],
        cursors=["evnt_last"],
    )
    w.set_world(world)

    loaded = await runtime.get_all_workflow_run_events(RUN_ID)

    assert [event.event_type for event in loaded.events] == [
        "run_started",
        "wait_created",
        "wait_completed",
    ]
    # The cursor is the world's and covers every row that was read, sealed
    # positions included -- dropping rows here must not rewind pagination.
    assert loaded.cursor == "evnt_last"


async def test_a_page_of_nothing_but_seals_still_pages_on() -> None:
    real = _stamp(w.RunStartedEvent(), 4)
    world = _PagedEventsWorld(
        [[_seal(1), _seal(2)], [_seal(3), real]],
        cursors=["evnt_page1", "evnt_page2"],
    )
    w.set_world(world)

    loaded = await runtime.get_all_workflow_run_events(RUN_ID)

    assert [event.event_type for event in loaded.events] == ["run_started"]
    assert world.requested == [None, "evnt_page1"]
    assert loaded.cursor == "evnt_page2"


# ── replay: a sealed log is indistinguishable from an unsealed one ─────────
#
# The claim the rest of this file exists to support, driven end to end through
# `workflow_handler`: one log, twice, differing only in the seals -- same
# output, same clock readings, same events written back.

_REGISTRY = core.Workflows(as_vercel_job=False)


@_REGISTRY.workflow
async def _clock_wf() -> list[str]:
    """Reads the deterministic clock on either side of each suspension.

    `now()` is the observable that a seal would corrupt: it dates the run from
    the last event the replay consumed, so a seal left in the log would hand
    the body the sealer's wall clock -- and then, because the clock only moves
    forward, every later reading too.
    """
    seen = [core.now().isoformat()]
    await core.sleep(0)
    seen.append(core.now().isoformat())
    await core.sleep(0)
    seen.append(core.now().isoformat())
    return seen


WORKFLOW_NAME = _clock_wf.workflow_id
WORKFLOW_QUEUE = w.get_queue_name(WORKFLOW_NAME)
STARTED_AT_MS = int(NOW.timestamp() * 1000)


def _wait_ids() -> list[str]:
    """The correlation ids `_clock_wf` will mint, in order.

    Positional and seeded from the run id, so the recorded log has to name the
    ids the body itself issues; one that does not stalls the replay rather than
    failing it.
    """
    ctx = runtime.WorkflowOrchestratorContext(
        [], run_id=RUN_ID, seed=RUN_ID, started_at=STARTED_AT_MS, registry=_REGISTRY
    )
    return [f"wait_{ctx.generate_ulid()}" for _ in range(2)]


def _run() -> w.WorkflowRun:
    return w.WorkflowRunAdaptor.validate_python(
        {
            "runId": RUN_ID,
            "deploymentId": "dpl_1",
            "workflowName": WORKFLOW_NAME,
            "status": "running",
            "specVersion": 7,
            "input": ser.dehydrate([]),
            "createdAt": NOW,
            "updatedAt": NOW,
            "startedAt": NOW,
        }
    )


def _recorded_log() -> list[w.Event]:
    """A completed two-sleep run, with each row stamped a minute apart.

    The gaps are what make the clock readings distinguishable: three different
    timestamps for three `now()` calls, none of them ``SEAL_TIME``.
    """
    first, second = _wait_ids()
    resume_at = NOW - timedelta(seconds=1)
    return [
        _stamp(
            w.RunCreatedEventData(
                deploymentId="dpl_1",
                workflowName=WORKFLOW_NAME,
                input=ser.dehydrate([]),
            ).into_event(),
            1,
            created_at=NOW,
        ),
        _stamp(
            w.WaitCreatedEventData(resumeAt=resume_at).into_event(first),
            2,
            created_at=NOW + timedelta(minutes=1),
        ),
        _stamp(
            w.WaitCompletedEvent(correlationId=first),
            3,
            created_at=NOW + timedelta(minutes=2),
        ),
        _stamp(
            w.WaitCreatedEventData(resumeAt=resume_at).into_event(second),
            4,
            created_at=NOW + timedelta(minutes=3),
        ),
        _stamp(
            w.WaitCompletedEvent(correlationId=second),
            5,
            created_at=NOW + timedelta(minutes=4),
        ),
    ]


def _sealed(log: list[w.Event]) -> list[w.Event]:
    """The same log with holes sealed at the head, the middle and the tail.

    Two of them consecutive, and one splitting a create/complete pair -- the
    adjacency a replay resolves within a single pass. A seal that merely *cost*
    a pass would go unnoticed by a log that suspends between every event, and
    would still be wrong: passes are what a concurrent body's wake order is
    made of.
    """
    return [
        _seal(0),
        log[0],
        _seal(6),
        _seal(7),
        log[1],
        _seal(8),
        log[2],
        log[3],
        log[4],
        _seal(9),
    ]


class _ReplayWorld(NoStreams, w.World):
    """Serves a fixed log and records everything the handler writes back."""

    def __init__(self, events: list[w.Event]) -> None:
        self.log = events
        self.written: list[w.Event] = []

    async def get_deployment_id(self) -> str:
        return "dpl_1"

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        raise AssertionError("a completed replay enqueues nothing")

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
        return w.PaginatedResult(data=list(self.log), cursor=None, hasMore=False)

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        self.written.append(data)
        if data.event_type == "run_started":
            return w.EventResult(run=_run())
        return w.EventResult()


async def _replay(log: list[w.Event]) -> _ReplayWorld:
    world = _ReplayWorld(log)
    w.set_world(world)
    await runtime.workflow_handler(
        {"runId": RUN_ID},
        attempt=1,
        queue_name=WORKFLOW_QUEUE,
        message_id="msg_1",
        registry=_REGISTRY,
    )
    return world


def _completion(world: _ReplayWorld) -> list[str]:
    completed = [e for e in world.written if isinstance(e, w.RunCompletedEvent)]
    assert len(completed) == 1, f"expected one run_completed, got {len(completed)}"
    return ser.hydrate(completed[0].event_data.output, what="the output")


async def test_a_sealed_log_replays_exactly_like_the_log_without_the_seals() -> None:
    plain = await _replay(_recorded_log())
    sealed = await _replay(_sealed(_recorded_log()))

    assert _completion(sealed) == _completion(plain)
    assert [e.event_type for e in sealed.written] == [e.event_type for e in plain.written]


async def test_the_clock_never_reads_the_sealers_wall_time() -> None:
    """The readings are pinned literally, not just against the unsealed twin: a
    change that broke both logs the same way would slip past the comparison."""
    sealed = await _replay(_sealed(_recorded_log()))

    assert _completion(sealed) == [
        NOW.isoformat(),
        (NOW + timedelta(minutes=2)).isoformat(),
        (NOW + timedelta(minutes=4)).isoformat(),
    ]
    assert SEAL_TIME.isoformat() not in _completion(sealed)


async def test_a_log_that_is_all_seals_before_the_run_is_still_replayable() -> None:
    """The degenerate shape: every position ahead of the run's own events was
    abandoned. The first `now()` has to find the first *real* event, not the
    seal sitting at position zero.
    """
    log = _recorded_log()
    sealed = await _replay([_seal(0), _seal(1), _seal(2), *log])

    assert _completion(sealed)[0] == NOW.isoformat()
