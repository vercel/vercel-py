import contextvars
import dataclasses
import json
from datetime import datetime
from typing import Any, ClassVar, ParamSpec, Self, TypeVar

from . import core, world as w

P = ParamSpec("P")
T = TypeVar("T")


@dataclasses.dataclass(kw_only=True)
class WorkflowRunContext:
    world: w.World = dataclasses.field(default_factory=w.get_world)
    run: w.WorkflowRun
    _ctx_tokens: list[contextvars.Token[Self]] = dataclasses.field(init=False, default_factory=list)
    _ctx: ClassVar[contextvars.ContextVar[Self]] = contextvars.ContextVar("WorkflowContext")

    @classmethod
    def current(cls) -> Self:
        return cls._ctx.get()

    def __enter__(self: Self) -> Self:
        self._ctx_tokens.append(self._ctx.set(self))
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._ctx.reset(self._ctx_tokens.pop())

    async def run_workflow(self, workflow_run: w.WorkflowRun, events: list[w.Event]) -> None:
        workflow = core.get_workflow(workflow_run.workflow_name)
        if not workflow_run.input.startswith(b"json"):
            raise RuntimeError(f"Unsupported workflow input encoding for run {workflow_run.run_id}")
        args, kwargs = json.loads(workflow_run.input[len(b"json"):].decode())
        await workflow.func(*args, **kwargs)

    async def run_step[**P, T](self, step: core.Step[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        args_json = json.dumps((args, kwargs), sort_keys=True)
        sus = Suspension(ulid=str(self.ulid_counter), step=step, args_json=args_json)
        self.suspensions[sus.ulid] = sus
        self.ulid_counter += 1
        if self.resume_handle is None:
            self.resume_handle = asyncio.get_running_loop().call_soon(self.resume)
        return await sus.future

    def resume(self) -> None:
        self.resume_handle = None
        while self.replay_index < len(self.event_log) and self.suspensions:
            event = self.event_log[self.replay_index]
            self.replay_index += 1

            sus = self.suspensions.pop(event.ulid)
            if event.name == sus.step.name and event.args_json == sus.args_json:
                sus.future.set_result(json.loads(event.result_json))
            else:
                raise RuntimeError("Workflow event log does not match execution")

        if self.suspensions:
            loop = asyncio.get_running_loop()
            for sus in self.suspensions.values():
                loop.create_task(sus.resume())
            self.suspensions.clear()


async def workflow_handler(
    message: Any,
    *,
    attempt: int,
    queue_name: str,
    message_id: str,
) -> float | None:
    world = w.get_world()
    run_id = message["runId"]
    workflow_run = await world.runs_get(run_id)
    if workflow_run.status == "pending":
        result = await world.events_create(run_id, w.RunStartedEvent())
        assert result.run is not None
        workflow_run = result.run

    # At this point, the workflow is "running" and `startedAt` should
    # definitely be set.
    if not workflow_run.started_at:
        raise RuntimeError(f'Workflow run "{run_id}" has no "startedAt" timestamp')
    # workflow_started_at = workflow_run.started_at

    if workflow_run.status != "running":
        # Workflow has already completed or failed, so we can skip it
        return None

    # Load all events into memory before running
    events = await get_all_workflow_run_events(run_id)

    # Check for any elapsed waits and create wait_completed events
    now = datetime.now()

    # Pre-compute completed correlation IDs for O(n) lookup instead of O(n²)
    completed_wait_ids = {e.correlation_id for e in events if e.event_type == "wait_completed"}

    # Collect all waits that need completion
    waits_to_complete = [
        e
        for e in events
        if e.event_type == "wait_created"
        and e.correlation_id not in completed_wait_ids
        and now >= e.event_data.resume_at
    ]

    # Create all wait_completed events
    for wait_event in waits_to_complete:
        result = await world.events_create(
            run_id,
            w.WaitCompletedEvent(correlationId=wait_event.correlation_id)
        )
        # Add the event to the events array so the workflow can see it
        assert result.event is not None
        events.append(result.event)

    with WorkflowRunContext(world=world, run=workflow_run) as ctx:
        await ctx.run_workflow(workflow_run, events)

    raise NotImplementedError()


async def step_handler(
    message: Any,
    *,
    attempt: int,
    queue_name: str,
    message_id: str,
) -> float | None:
    raise NotImplementedError()


def workflow_entrypoint() -> w.HTTPHandler:
    return w.get_world().create_queue_handler(
        "__wkf_workflow_",
        workflow_handler,
    )


def step_entrypoint() -> w.HTTPHandler:
    return w.get_world().create_queue_handler(
        "__wkf_step_",
        step_handler,
    )


async def get_all_workflow_run_events(run_id: str) -> list[w.Event]:
    all_events = []
    cursor: str | None = None
    has_more = True

    world = w.get_world()
    while has_more:
        response = await world.events_list(
            run_id,
            pagination=w.PaginationOptions(
                cursor=cursor,
                sort_order="asc",  # Required: events must be in chronological order for replay
            ),
        )
        all_events.extend(response.data)
        has_more = response.has_more
        cursor = response.cursor
    return all_events


@dataclasses.dataclass
class Run:
    run_id: str


async def start[**P, T](wf: core.Workflow[P, T], *args: P.args, **kwargs: P.kwargs) -> Run:
    world = w.get_world()
    deployment_id = await world.get_deployment_id()
    input_data = b"json" + json.dumps([args, kwargs]).encode()
    data = w.RunCreatedEventData(
        deploymentId=deployment_id, workflowName=wf.workflow_id, input=input_data
    )
    result = await world.events_create(None, data.into_event())

    # Assert that the run was created
    if not result.run:
        raise RuntimeError("Missing 'run' in server response for 'run_created' event")

    run_id = result.run.run_id
    await world.queue(
        f"__wkf_workflow_{wf.workflow_id}",
        w.WorkflowInvokePayload(run_id=run_id),
        deployment_id=deployment_id,
    )

    return Run(run_id)
