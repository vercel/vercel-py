"""Tests for the handler's setup path: `run_started` first, no pre-read.

``FakeWorld.runs_get`` raises, which is what pins the property down: the setup
path must never read the run.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pytest

from vercel._internal.core.polyfills import UTC
from vercel.workflow._internal import core, runtime, serialization as ser, world as w

from ..world_stubs import NoStreams

NOW = datetime(2026, 1, 1, tzinfo=UTC)
RUN_ID = "wrun_test"


_REGISTRY = core.Workflows(as_vercel_job=False)


@_REGISTRY.workflow
async def _wf() -> str:
    return "done"


# The handler resolves the workflow from the run's `workflowName`, and the id is
# derived from the function, so the run rows below have to name this one.
WORKFLOW_NAME = _wf.workflow_id
WORKFLOW_QUEUE = w.get_queue_name(WORKFLOW_NAME)


def _run(status: str = "running", **overrides: Any) -> w.WorkflowRun:
    fields: dict[str, Any] = {
        "runId": RUN_ID,
        "deploymentId": "dpl_1",
        "workflowName": WORKFLOW_NAME,
        "status": status,
        "specVersion": 6,
        "input": ser.dehydrate([]),
        "createdAt": NOW,
        "updatedAt": NOW,
        "startedAt": NOW,
    }
    return w.WorkflowRunAdaptor.from_wire(fields | overrides)


def _run_input(**overrides: Any) -> dict[str, Any]:
    return {
        "input": ser.dehydrate([]),
        "deploymentId": "dpl_1",
        "workflowName": WORKFLOW_NAME,
        "specVersion": 6,
    } | overrides


class FakeWorld(NoStreams, w.World):
    """In-memory world for the setup path.

    ``runs_get`` raises: a test that passes only because the handler read the run
    is a test of the bug this replaced.
    """

    def __init__(
        self,
        *,
        started_run: w.WorkflowRun | None = None,
        start_error: Exception | None = None,
        environment: str | None = None,
    ) -> None:
        self.started_run = started_run if started_run is not None else _run()
        self.start_error = start_error
        self.environment = environment
        self.events: list[w.Event] = []
        self.queued: list[tuple[str, Any]] = []
        self.runs_get_calls = 0
        self.events_list_calls: list[str] = []

    async def get_deployment_id(self) -> str:
        return "dpl_1"

    def get_environment(self) -> str | None:
        return self.environment

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        self.queued.append((queue_name, message))
        return "msg_fake"

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        raise NotImplementedError

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        self.runs_get_calls += 1
        raise AssertionError("the setup path must not read the run")

    async def steps_get(self, run_id: str, step_id: str) -> w.WorkflowStep:
        raise NotImplementedError

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        raise NotImplementedError

    async def events_list(self, run_id: str, *, pagination: Any = None) -> Any:
        self.events_list_calls.append(run_id)
        return w.PaginatedResult(data=list(self.events), cursor=None, has_more=False)

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        if data.event_type == "run_started":
            if self.start_error is not None:
                raise self.start_error
            self.events.append(data)
            return w.EventResult(run=self.started_run)
        self.events.append(data)
        return w.EventResult()


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


@pytest.fixture
def registry() -> core.Workflows:
    return _REGISTRY


async def _invoke(
    registry: core.Workflows, *, run_input: dict[str, Any] | None = None
) -> w.QueueContinuation | None:
    message: dict[str, Any] = {"runId": RUN_ID}
    if run_input is not None:
        message["runInput"] = run_input
    return await runtime.workflow_handler(
        message,
        attempt=1,
        queue_name=WORKFLOW_QUEUE,
        message_id="msg_1",
        registry=registry,
    )


def _started(fake: FakeWorld) -> w.RunStartedEvent:
    started = [e for e in fake.events if isinstance(e, w.RunStartedEvent)]
    assert len(started) == 1, f"expected one run_started, got {len(started)}"
    return started[0]


async def test_setup_never_reads_the_run(registry: core.Workflows) -> None:
    """The whole fix: no `runs_get` before replay, so a row that has not landed
    yet cannot fail the delivery."""
    fake = FakeWorld()
    w.set_world(fake)

    await _invoke(registry, run_input=_run_input())

    assert fake.runs_get_calls == 0
    assert _started(fake) is not None


async def test_run_started_carries_the_queued_run_input(registry: core.Workflows) -> None:
    """This is what lets the world create the run when `run_created` never
    landed; dropping it would leave the 404 unfixed."""
    fake = FakeWorld()
    w.set_world(fake)
    run_input = _run_input(
        executionContext={"workflowCoreVersion": "5.0.0"},
        attributes={"tier": "pro"},
        allowReservedAttributes=True,
        encryptionPublicKey="cHVibGljLWtleQ==",
    )

    await _invoke(registry, run_input=run_input)

    data = _started(fake).event_data
    assert data is not None
    assert data.input == run_input["input"]
    assert data.deployment_id == "dpl_1"
    assert data.workflow_name == WORKFLOW_NAME
    assert data.execution_context == {"workflowCoreVersion": "5.0.0"}
    assert data.attributes == {"tier": "pro"}
    assert data.allow_reserved_attributes is True
    assert data.encryption_public_key == "cHVibGljLWtleQ=="


async def test_run_started_uses_the_creators_spec_version(registry: core.Workflows) -> None:
    """A run this path creates must not be relabelled to our CURRENT (2)."""
    fake = FakeWorld()
    w.set_world(fake)

    await _invoke(registry, run_input=_run_input(specVersion=6))

    assert _started(fake).spec_version == 6


async def test_re_enqueue_sends_no_event_data(registry: core.Workflows) -> None:
    """A re-enqueue carries no `runInput`. Sending an empty `eventData` would ask
    the world to create a run from nothing."""
    fake = FakeWorld()
    w.set_world(fake)

    await _invoke(registry)

    event = _started(fake)
    assert event.event_data is None
    assert "eventData" not in event.model_dump()
    assert event.spec_version == w.SPEC_VERSION_CURRENT


async def test_setup_takes_the_run_entity_from_the_response(registry: core.Workflows) -> None:
    """Reaching the event-log load means setup accepted the returned entity and
    went on to replay, having never read the run.

    Replay stops shortly after: the runtime re-imports the workflow's defining
    module by name, and a test module is not importable that way. Running the
    body end to end is the e2e suite's job.
    """
    fake = FakeWorld(started_run=_run(status="running", deployment_id="dpl_from_response"))
    w.set_world(fake)

    await _invoke(registry, run_input=_run_input())

    assert fake.runs_get_calls == 0
    assert fake.events_list_calls == [RUN_ID]


async def test_started_at_comes_from_the_response(registry: core.Workflows) -> None:
    """The missing-timestamp check reads the response entity, so this pins where
    the handler gets the run from."""
    fake = FakeWorld(started_run=_run(status="running", startedAt=None))
    w.set_world(fake)

    with pytest.raises(RuntimeError, match='no "startedAt"'):
        await _invoke(registry, run_input=_run_input())

    assert fake.runs_get_calls == 0
    assert fake.events_list_calls == []


@pytest.mark.parametrize(
    "error",
    [
        w.EntityConflictError("already finished"),
        w.RunExpiredError("expired", status=410),
    ],
)
async def test_already_finished_acks_without_replaying(
    registry: core.Workflows, error: Exception
) -> None:
    """Both mean the run went terminal. Nothing to do, and nothing to retry —
    re-raising would make the queue redeliver a run that can never advance."""
    fake = FakeWorld(start_error=error)
    w.set_world(fake)

    assert await _invoke(registry, run_input=_run_input()) is None
    assert not any(e.event_type == "run_completed" for e in fake.events)


async def test_unexpected_world_error_still_propagates(registry: core.Workflows) -> None:
    """Only the two terminal signals are swallowed; a 500 must still retry."""
    fake = FakeWorld(start_error=w.WorkflowWorldError("boom", status=500))
    w.set_world(fake)

    with pytest.raises(w.WorkflowWorldError):
        await _invoke(registry, run_input=_run_input())


async def test_cancelled_run_is_not_replayed(registry: core.Workflows) -> None:
    """A world that hands back the cancelled run rather than raising."""
    fake = FakeWorld(started_run=_run(status="cancelled", completedAt=NOW, startedAt=NOW))
    w.set_world(fake)

    assert await _invoke(registry, run_input=_run_input()) is None
    assert not any(e.event_type == "run_completed" for e in fake.events)


async def test_run_cancelled_before_it_started_is_not_an_error(registry: core.Workflows) -> None:
    """Cancellation can beat the first delivery, leaving no `startedAt` at all.
    That has to ack, not raise the missing-timestamp error meant for a running
    run — which is why the status is checked before the timestamp."""
    fake = FakeWorld(started_run=_run(status="cancelled", completedAt=NOW, startedAt=None))
    w.set_world(fake)

    assert await _invoke(registry, run_input=_run_input()) is None
    assert not any(e.event_type == "run_completed" for e in fake.events)


class TestCrossEnvironmentGuard:
    """Resilient start creates the run under *our* tenant, so a message from
    another environment would fork the run id across both. Refuse before the
    write that would do it.
    """

    async def test_mismatch_is_refused_before_run_started(
        self, registry: core.Workflows, caplog
    ) -> None:
        fake = FakeWorld(environment="preview")
        w.set_world(fake)

        with caplog.at_level(logging.ERROR, logger="vercel.workflow"):
            assert await _invoke(registry, run_input=_run_input(environment="production")) is None

        # Refusing after the write would be too late — that is the fork.
        assert fake.events == []
        assert fake.runs_get_calls == 0
        assert "production" in caplog.text and "preview" in caplog.text

    async def test_match_proceeds(self, registry: core.Workflows) -> None:
        fake = FakeWorld(environment="production")
        w.set_world(fake)

        await _invoke(registry, run_input=_run_input(environment="production"))

        assert _started(fake) is not None

    async def test_unknown_creator_environment_proceeds(self, registry: core.Workflows) -> None:
        """Older clients stamp none, so the check has to skip rather than refuse."""
        fake = FakeWorld(environment="production")
        w.set_world(fake)

        await _invoke(registry, run_input=_run_input())

        assert _started(fake) is not None

    async def test_unknown_local_environment_proceeds(self, registry: core.Workflows) -> None:
        """The local world has no environment dimension."""
        fake = FakeWorld(environment=None)
        w.set_world(fake)

        await _invoke(registry, run_input=_run_input(environment="production"))

        assert _started(fake) is not None

    async def test_re_enqueue_is_never_refused(self, registry: core.Workflows) -> None:
        """No `runInput` means no environment to compare, and a re-enqueue is for
        a run that already exists here anyway."""
        fake = FakeWorld(environment="preview")
        w.set_world(fake)

        await _invoke(registry)

        assert _started(fake) is not None


def test_refuse_helper_skips_when_either_side_is_unknown() -> None:
    """Called directly, because the honest answer to "unknown" is to proceed."""
    fake = FakeWorld(environment="preview")

    assert not runtime.refuse_cross_environment_delivery(fake, None, RUN_ID)
    assert not runtime.refuse_cross_environment_delivery(
        fake, w.RunInput.from_wire(_run_input()), RUN_ID
    )
    assert runtime.refuse_cross_environment_delivery(
        fake,
        w.RunInput.from_wire(_run_input(environment="production")),
        RUN_ID,
    )
