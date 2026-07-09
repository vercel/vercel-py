from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from vercel._internal.workflow import core, runtime, world as w
from vercel.workflow import Workflows


class _RecordingWorld:
    def __init__(
        self,
        *,
        event_error: Exception | None = None,
        run: Any | None = None,
    ) -> None:
        self.prefixes: list[str] = []
        self.queued: list[tuple[str, w.QueuePayload, dict[str, Any]]] = []
        self.events: list[w.Event] = []
        self.event_error = event_error
        self.run = run or SimpleNamespace(
            workflow_name="workflow//tests.example",
            deployment_id="dpl_test",
            execution_context=None,
        )

    async def get_deployment_id(self) -> str:
        return "dpl_test"

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        self.queued.append((queue_name, message, kwargs))
        return "msg_test"

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        self.prefixes.append(queue_name_prefix)

        async def http_handler(request: w.HTTPRequest) -> w.HTTPResponse:
            return w.HTTPResponse(status=200, body=b"", headers={})

        return http_handler

    async def runs_get(self, run_id: str) -> Any:
        return self.run

    async def steps_get(self, run_id: str, step_id: str) -> Any:
        raise NotImplementedError

    async def hooks_get_by_token(self, token: str) -> Any:
        raise NotImplementedError

    async def events_create(self, run_id: str | None, data: w.Event) -> Any:
        if self.event_error is not None:
            raise self.event_error
        self.events.append(data)
        if run_id is None:
            return SimpleNamespace(run=SimpleNamespace(run_id="wrun_test"))
        return SimpleNamespace()

    async def events_list(self, run_id: str, *, pagination: Any = None) -> Any:
        raise NotImplementedError


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


def _set_world(world: _RecordingWorld) -> None:
    w.set_world(cast(w.World, world))


def _run_created_contexts(world: _RecordingWorld) -> list[dict[str, Any] | None]:
    return [
        cast(w.RunCreatedEvent, event).event_data.execution_context
        for event in world.events
        if event.event_type == "run_created"
    ]


def test_queue_names_default_to_legacy_prefixes() -> None:
    assert w.get_queue_topic_prefix("workflow") == "__wkf_workflow_"
    assert w.get_queue_topic_prefix("step") == "__wkf_step_"
    assert w.get_queue_name("workflow", "example") == "__wkf_workflow_example"


def test_queue_names_accept_explicit_namespace() -> None:
    assert w.get_queue_name("workflow", "example", "python2") == ("__python2_wkf_workflow_example")
    assert w.get_queue_name("step", "example", "python2") == "__python2_wkf_step_example"


@pytest.mark.parametrize("namespace", ["", "123abc", "Custom", "my-framework", "my_namespace"])
def test_invalid_queue_namespaces_are_rejected(namespace: str) -> None:
    with pytest.raises(ValueError, match="Invalid queue namespace"):
        core.Workflows(namespace=namespace, as_vercel_job=False)


def test_workflow_namespace_is_positional_and_read_only() -> None:
    wf = Workflows("billing", as_vercel_job=False)
    assert wf.namespace == "billing"
    with pytest.raises(AttributeError):
        cast(Any, wf).namespace = "email"


def test_registries_subscribe_to_distinct_namespaced_topics() -> None:
    world = _RecordingWorld()
    _set_world(world)

    core.Workflows("python1")
    core.Workflows("python2")

    assert world.prefixes == [
        "__python1_wkf_workflow_",
        "__python1_wkf_step_",
        "__python2_wkf_workflow_",
        "__python2_wkf_step_",
    ]


def test_namespaced_step_queue_is_parsed_with_matching_namespace() -> None:
    assert (
        runtime._step_name_from_queue("__python_wkf_step_step//tests.add", "python")
        == "step//tests.add"
    )

    with pytest.raises(ValueError, match="expected prefix"):
        runtime._step_name_from_queue("__wkf_step_step//tests.add", "python")


async def test_step_handler_requeues_on_namespaced_workflow_topic() -> None:
    world = _RecordingWorld(event_error=w.EntityConflictError("already finished"))
    _set_world(world)
    registry = core.Workflows(namespace="python", as_vercel_job=False)

    @registry.step
    async def add() -> None:
        pass

    await runtime.step_handler(
        w.StepInvokePayload(
            workflowName="workflow//tests.example",
            workflowRunId="wrun_test",
            workflowStartedAt=0,
            stepId="step_test",
        ).model_dump(by_alias=True),
        attempt=1,
        queue_name=f"__python_wkf_step_{add.name}",
        message_id="msg_test",
        registry=registry,
        namespace=registry.namespace,
    )

    assert world.queued[0][0] == "__python_wkf_workflow_workflow//tests.example"


async def test_start_uses_registry_namespace_and_persists_it() -> None:
    world = _RecordingWorld()
    _set_world(world)
    registry = core.Workflows(namespace="python", as_vercel_job=False)

    @registry.workflow
    async def example() -> None:
        pass

    run = await runtime.start(example)

    assert run.run_id == "wrun_test"
    assert world.queued[0][0] == f"__python_wkf_workflow_{example.workflow_id}"
    assert _run_created_contexts(world) == [{"queueNamespace": "python"}]


async def test_start_without_namespace_uses_legacy_topic() -> None:
    world = _RecordingWorld()
    _set_world(world)
    registry = core.Workflows(as_vercel_job=False)

    @registry.workflow
    async def example() -> None:
        pass

    await runtime.start(example)

    assert world.queued[0][0] == f"__wkf_workflow_{example.workflow_id}"
    assert _run_created_contexts(world) == [None]


async def test_multiple_registries_start_on_their_own_topics() -> None:
    world = _RecordingWorld()
    _set_world(world)
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

    assert [queue_name for queue_name, _, _ in world.queued] == [
        f"__first_wkf_workflow_{first_workflow.workflow_id}",
        f"__second_wkf_workflow_{second_workflow.workflow_id}",
    ]
    assert _run_created_contexts(world) == [
        {"queueNamespace": "first"},
        {"queueNamespace": "second"},
    ]


async def test_resume_hook_uses_stored_namespace() -> None:
    world = _RecordingWorld(
        run=SimpleNamespace(
            workflow_name="workflow//tests.example",
            deployment_id="dpl_original",
            execution_context={"queueNamespace": "python"},
        )
    )
    _set_world(world)
    hook = cast(w.Hook, SimpleNamespace(run_id="wrun_test", hook_id="hook_test"))

    await runtime.resume_hook(hook, '{"ok": true}')

    assert world.queued[0][0] == "__python_wkf_workflow_workflow//tests.example"


async def test_resume_legacy_run_uses_default_namespace() -> None:
    world = _RecordingWorld(
        run=SimpleNamespace(
            workflow_name="workflow//tests.example",
            deployment_id="dpl_legacy",
            execution_context=None,
        )
    )
    _set_world(world)
    hook = cast(w.Hook, SimpleNamespace(run_id="wrun_test", hook_id="hook_test"))

    await runtime.resume_hook(hook, '{"ok": true}')

    assert world.queued[0][0] == "__wkf_workflow_workflow//tests.example"
