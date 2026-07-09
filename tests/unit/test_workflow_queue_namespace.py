from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from vercel._internal.polyfills import UTC
from vercel._internal.workflow import core, runtime, world as w
from vercel.workflow import Workflows

NOW = datetime(2026, 1, 1, tzinfo=UTC)
RUN_ID = "wrun_test"
WORKFLOW_NAME = "workflow//tests.example"


def _run(execution_context: dict[str, Any] | None = None) -> w.WorkflowRun:
    return w.NonFinalWorkflowRun(
        runId=RUN_ID,
        status="running",
        deploymentId="dpl_test",
        workflowName=WORKFLOW_NAME,
        executionContext=execution_context,
        createdAt=NOW,
        updatedAt=NOW,
    )


def _hook() -> w.Hook:
    return w.Hook(
        runId=RUN_ID,
        hookId="hook_test",
        token="hook-token",
        ownerId="team_test",
        projectId="prj_test",
        environment="development",
        createdAt=NOW,
    )


class FakeWorld(w.World):
    def __init__(
        self,
        *,
        start_error: Exception | None = None,
        run: w.WorkflowRun | None = None,
    ) -> None:
        self.prefixes: list[str] = []
        self.queued: list[tuple[str, w.QueuePayload]] = []
        self.events: list[w.Event] = []
        self.start_error = start_error
        self.run = run or _run()

    async def get_deployment_id(self) -> str:
        return "dpl_test"

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        self.queued.append((queue_name, message))
        return "msg_test"

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        self.prefixes.append(queue_name_prefix)

        async def http_handler(request: w.HTTPRequest) -> w.HTTPResponse:
            return w.HTTPResponse(status=200, body=b"", headers={})

        return http_handler

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        return self.run

    async def steps_get(self, run_id: str, step_id: str) -> w.WorkflowStep:
        raise NotImplementedError

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        raise NotImplementedError

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        if data.event_type == "step_started" and self.start_error is not None:
            raise self.start_error
        self.events.append(data)
        if run_id is None:
            return w.EventResult(run=self.run)
        return w.EventResult()

    async def events_list(
        self,
        run_id: str,
        *,
        pagination: w.PaginationOptions | None = None,
    ) -> w.PaginatedResult[w.Event]:
        raise NotImplementedError


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


def _run_created_contexts(world: FakeWorld) -> list[dict[str, Any] | None]:
    return [
        event.event_data.execution_context
        for event in world.events
        if isinstance(event, w.RunCreatedEvent)
    ]


def test_queue_names_default_to_unnamespaced_prefixes() -> None:
    assert w.get_queue_topic_prefix("workflow") == "__wkf_workflow_"
    assert w.get_queue_topic_prefix("step") == "__wkf_step_"
    assert w.get_queue_name("workflow", "example") == "__wkf_workflow_example"


def test_queue_names_accept_explicit_namespace() -> None:
    assert w.get_queue_name("workflow", "example", "python2") == "__python2_wkf_workflow_example"
    assert w.get_queue_name("step", "example", "python2") == "__python2_wkf_step_example"


@pytest.mark.parametrize("namespace", ["", "123abc", "Custom", "my-framework", "my_namespace"])
def test_invalid_queue_namespaces_are_rejected(namespace: str) -> None:
    with pytest.raises(ValueError, match="Invalid queue namespace"):
        core.Workflows(namespace=namespace, as_vercel_job=False)


def test_workflow_namespace_accepts_positional_argument() -> None:
    wf = Workflows("billing", as_vercel_job=False)
    assert wf.namespace == "billing"


def test_registries_subscribe_to_distinct_namespaced_topics() -> None:
    world = FakeWorld()
    w.set_world(world)

    core.Workflows("python1")
    core.Workflows("python2")

    assert world.prefixes == [
        "__python1_wkf_workflow_",
        "__python1_wkf_step_",
        "__python2_wkf_workflow_",
        "__python2_wkf_step_",
    ]


def test_step_queue_rejects_mismatched_namespace() -> None:
    with pytest.raises(ValueError, match="expected prefix"):
        runtime._step_name_from_queue("__wkf_step_step//tests.add", "python")


async def test_step_handler_requeues_on_namespaced_workflow_topic() -> None:
    world = FakeWorld(start_error=w.EntityConflictError("already finished"))
    w.set_world(world)
    registry = core.Workflows(namespace="python", as_vercel_job=False)

    @registry.step
    async def add() -> None:
        pass

    await runtime.step_handler(
        w.StepInvokePayload(
            workflowName=WORKFLOW_NAME,
            workflowRunId=RUN_ID,
            workflowStartedAt=0,
            stepId="step_test",
        ).model_dump(by_alias=True),
        attempt=1,
        queue_name=f"__python_wkf_step_{add.name}",
        message_id="msg_test",
        registry=registry,
        namespace=registry.namespace,
    )

    assert world.queued[0][0] == f"__python_wkf_workflow_{WORKFLOW_NAME}"


async def test_start_without_namespace_uses_unnamespaced_topic() -> None:
    world = FakeWorld()
    w.set_world(world)
    registry = core.Workflows(as_vercel_job=False)

    @registry.workflow
    async def example() -> None:
        pass

    await runtime.start(example)

    assert world.queued[0][0] == f"__wkf_workflow_{example.workflow_id}"
    assert _run_created_contexts(world) == [None]


async def test_start_routes_each_registry_to_its_namespace() -> None:
    world = FakeWorld()
    w.set_world(world)
    first_registry = core.Workflows(namespace="first", as_vercel_job=False)
    second_registry = core.Workflows(namespace="second", as_vercel_job=False)

    @first_registry.workflow
    async def first_workflow() -> None:
        pass

    @second_registry.workflow
    async def second_workflow() -> None:
        pass

    await runtime.start(first_workflow)
    await runtime.start(second_workflow)

    assert [queue_name for queue_name, _ in world.queued] == [
        f"__first_wkf_workflow_{first_workflow.workflow_id}",
        f"__second_wkf_workflow_{second_workflow.workflow_id}",
    ]
    assert _run_created_contexts(world) == [
        {"queueNamespace": "first"},
        {"queueNamespace": "second"},
    ]


async def test_resume_hook_uses_stored_namespace() -> None:
    world = FakeWorld(run=_run({"queueNamespace": "python"}))
    w.set_world(world)

    await runtime.resume_hook(_hook(), '{"ok": true}')

    assert world.queued[0][0] == f"__python_wkf_workflow_{WORKFLOW_NAME}"


async def test_resume_run_without_namespace_uses_unnamespaced_topic() -> None:
    world = FakeWorld()
    w.set_world(world)

    await runtime.resume_hook(_hook(), '{"ok": true}')

    assert world.queued[0][0] == f"__wkf_workflow_{WORKFLOW_NAME}"
