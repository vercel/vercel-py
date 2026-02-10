import contextvars
import dataclasses
import json
from typing import Any, ClassVar, ParamSpec, Self, TypeVar

from . import core, world as w

P = ParamSpec("P")
T = TypeVar("T")


@dataclasses.dataclass
class WorkflowContext:
    world: w.World = dataclasses.field(default_factory=w.get_world)
    _ctx_tokens: list[contextvars.Token[Self]] = dataclasses.field(init=False, default_factory=list)
    _ctx: ClassVar[contextvars.ContextVar[Self]] = contextvars.ContextVar("WorkflowContext")

    @classmethod
    def current(cls) -> Self:
        return cls._ctx.get()

    def __enter__(self: Self) -> None:
        self._ctx_tokens.append(self._ctx.set(self))

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._ctx.reset(self._ctx_tokens.pop())

    async def workflow_handler(
        self,
        message: Any,
        *,
        attempt: int,
        queue_name: str,
        message_id: str,
    ) -> float | None:
        run_id = message["runId"]
        workflow_run = await self.world.runs_get(run_id)
        if workflow_run.status == "pending":
            result = await self.world.events_create(run_id, w.RunStartedEvent())
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
        # events = await get_all_workflow_run_events(run_id)

        """
                      // Pre-compute completed correlation IDs for O(n) lookup instead of O(n²)
              const completedWaitIds = new Set(
                events
                  .filter((e) => e.eventType === 'ewait_completed')
                  .map((e) => e.correlationId)
              );

              // Collect all waits that need completion
              const waitsToComplete = events
                .filter(
                  (e): e is typeof e & { correlationId: string } =>
                    e.eventType === 'wait_created' &&
                    e.correlationId !== undefined &&
                    !completedWaitIds.has(e.correlationId) &&
                    now >= (e.eventData.resumeAt as Date).getTime()
                )
                .map((e) => ({
                  eventType: 'wait_completed' as const,
                  specVersion: SPEC_VERSION_CURRENT,
                  correlationId: e.correlationId,
                }));

              // Create all wait_completed events
              for (const waitEvent of waitsToComplete) {
                const result = await world.events.create(runId, waitEvent);
                // Add the event to the events array so the workflow can see it
                events.push(result.event!);
              }
"""
        # Pre-compute completed correlation IDs for O(n) lookup instead of O(n²)

        raise NotImplementedError()

    async def step_handler(
        self,
        message: Any,
        *,
        attempt: int,
        queue_name: str,
        message_id: str,
    ) -> float | None:
        raise NotImplementedError()

    def workflow_entrypoint(self) -> w.HTTPHandler:
        return self.world.create_queue_handler(
            "__wkf_workflow_",
            self.workflow_handler,
        )

    def step_entrypoint(self) -> w.HTTPHandler:
        return self.world.create_queue_handler(
            "__wkf_step_",
            self.step_handler,
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
    ctx = WorkflowContext.current()
    deployment_id = await ctx.world.get_deployment_id()
    input_data = b"json" + json.dumps([args, kwargs]).encode()
    data = w.RunCreatedEventData(
        deploymentId=deployment_id, workflowName=wf.workflow_id, input=input_data
    )
    result = await ctx.world.events_create(None, data.into_event())

    # Assert that the run was created
    if not result.run:
        raise RuntimeError("Missing 'run' in server response for 'run_created' event")

    run_id = result.run.run_id
    await ctx.world.queue(
        f"__wkf_workflow_{wf.workflow_id}",
        w.WorkflowInvokePayload(run_id=run_id),
        deployment_id=deployment_id,
    )

    return Run(run_id)
