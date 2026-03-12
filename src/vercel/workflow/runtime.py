import asyncio
import contextvars
import dataclasses
import functools
import json
import random
import traceback
from datetime import UTC, datetime
from typing import Any, ParamSpec, Self, TypeVar

from . import core, ulid, world as w

P = ParamSpec("P")
T = TypeVar("T")
SUSPENDED_MESSAGE = "<WORKFLOW SUSPENDED>"


@dataclasses.dataclass
class Suspension[T]:
    correlation_id: str
    step: core.Step[Any, T]
    input: bytes
    future: asyncio.Future[T] = dataclasses.field(default_factory=asyncio.Future)
    has_created_event: bool = False


class WorkflowOrchestratorContext:
    _ctx: contextvars.ContextVar[Self] = contextvars.ContextVar("WorkflowContext")

    def __init__(self, events: list[w.Event], *, seed: str, started_at: int):
        self.events = events
        self.replay_index = 0
        prng = random.Random(seed)
        self.generate_ulid = functools.partial(ulid.monotonic_factory(prng.random), started_at)
        self._fut: asyncio.Future[Any] | None = None
        self.suspensions: dict[str, Suspension[Any]] = {}
        self.resume_handle: asyncio.Handle | None = None

    @classmethod
    def current(cls) -> Self:
        return cls._ctx.get()

    async def run_workflow(self: Self, workflow_run: w.WorkflowRun) -> Any:
        workflow = core.get_workflow(workflow_run.workflow_name)
        if not workflow_run.input[0].startswith(b"json"):
            raise RuntimeError(f"Unsupported workflow input encoding for run {workflow_run.run_id}")
        args, kwargs = json.loads(workflow_run.input[0][len(b"json") :].decode())
        token = self._ctx.set(self)
        try:
            self._fut = asyncio.ensure_future(workflow.func(*args, **kwargs))
        finally:
            self._ctx.reset(token)
        return await self._fut

    async def run_step[**P, T](self, step: core.Step[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        input_data = b"json" + json.dumps((args, kwargs), sort_keys=True).encode()
        sus = Suspension(correlation_id=f"step_{self.generate_ulid()}", step=step, input=input_data)
        self.suspensions[sus.correlation_id] = sus
        if self.resume_handle is None:
            self.resume_handle = asyncio.get_running_loop().call_soon(self.resume)
        return await sus.future

    def resume(self) -> None:
        self.resume_handle = None

        if self._fut is None:
            return

        while self.replay_index < len(self.events) and self.suspensions:
            event = self.events[self.replay_index]
            self.replay_index += 1

            match event:
                case w.StepCreatedEvent():
                    self.suspensions[event.correlation_id].has_created_event = True

                case w.StepCompletedEvent(event_data=w.StepCompletedEventData(result=data)):
                    sus = self.suspensions.pop(event.correlation_id)
                    if data[0].startswith(b"json"):
                        result = json.loads(data[0][len(b"json") :].decode())
                    else:
                        self._fut.cancel(
                            f"Unsupported step result encoding for "
                            f"correlation ID {event.correlation_id}"
                        )
                        return
                    sus.future.set_result(result)

                case w.StepFailedEvent(event_data=w.StepFailedEventData(error=e)):
                    sus = self.suspensions.pop(event.correlation_id)
                    sus.future.set_exception(RuntimeError(e))

        if self.suspensions:
            self._fut.cancel(SUSPENDED_MESSAGE)


async def workflow_handler(
    message: Any,
    *,
    attempt: int,
    queue_name: str,
    message_id: str,
) -> float | None:
    world = w.get_world()
    run_id = w.WorkflowInvokePayload.model_validate(message).run_id
    workflow_run = await world.runs_get(run_id)
    if workflow_run.status == "pending":
        result = await world.events_create(run_id, w.RunStartedEvent())
        assert result.run is not None
        workflow_run = result.run
    elif workflow_run.status == "cancelled":
        return

    # At this point, the workflow is "running" and `startedAt` should
    # definitely be set.
    if not workflow_run.started_at:
        raise RuntimeError(f'Workflow run "{run_id}" has no "startedAt" timestamp')
    workflow_started_at = int(workflow_run.started_at.timestamp() * 1000)

    if workflow_run.status != "running":
        # Workflow has already completed or failed, so we can skip it
        return None

    # Load all events into memory before running
    events = await get_all_workflow_run_events(run_id)

    # Check for any elapsed waits and create wait_completed events
    now = datetime.now(UTC)

    # Pre-compute completed correlation IDs for O(n) lookup instead of O(n²)
    completed_wait_ids = {e.correlation_id for e in events if e.event_type == "wait_completed"}

    # Collect all waits that need completion
    waits_to_complete: list[w.WaitCreatedEvent] = []
    for e in events:
        if (
            e.event_type == "wait_created"
            and e.correlation_id not in completed_wait_ids
            and now >= e.event_data.resume_at
        ):
            waits_to_complete.append(e)

    # Create all wait_completed events
    for wait_event in waits_to_complete:
        result = await world.events_create(
            run_id, w.WaitCompletedEvent(correlationId=wait_event.correlation_id)
        )
        # Add the event to the events array so the workflow can see it
        assert result.event is not None
        events.append(result.event)

    context = WorkflowOrchestratorContext(events, seed=run_id, started_at=workflow_started_at)
    try:
        result = await context.run_workflow(workflow_run)
        output = b"json" + json.dumps(result).encode()
    except BaseException as e:
        if isinstance(e, asyncio.CancelledError) and e.args and e.args[0] == SUSPENDED_MESSAGE:
            for sus in context.suspensions.values():
                if not sus.has_created_event:
                    data = w.StepCreatedEventData(stepName=sus.step.name, input=[sus.input])
                    await world.events_create(run_id, data.into_event(sus.correlation_id))
                    await world.queue(
                        f"__wkf_step_{sus.step.name}",
                        w.StepInvokePayload(
                            workflowName=workflow_run.workflow_name,
                            workflowRunId=run_id,
                            workflowStartedAt=workflow_started_at,
                            stepId=sus.correlation_id,
                            requestedAt=datetime.now(UTC),
                        ),
                    )
        elif isinstance(e, Exception):
            await world.events_create(
                run_id,
                w.RunFailedEventData(error=str(e)).into_event(),
            )
        else:
            raise
    else:
        await world.events_create(
            run_id,
            w.RunCompletedEventData(result=[output]).into_event(),
        )

    return None


async def step_handler(
    message: Any,
    *,
    attempt: int,
    queue_name: str,
    message_id: str,
) -> float | None:
    world = w.get_world()
    req = w.StepInvokePayload.model_validate(message)

    # Get the step entity
    step_run = await world.steps_get(req.workflow_run_id, req.step_id)
    step = core.get_step(step_run.step_name)

    # Check if retry_after timestamp hasn't been reached yet
    now = datetime.now(UTC)
    if step_run.retry_after and step_run.retry_after > now:
        timeout_seconds = max(1, int((step_run.retry_after - now).total_seconds()))
        return timeout_seconds

    # Check max retries FIRST before any state changes
    # step.attempt tracks how many times step_started has been called
    # Use > here (not >=) because this guards against re-invocation AFTER all attempts are used
    if step_run.attempt > step.max_retries + 1:
        retry_count = step_run.attempt - 1
        error_message = (
            f"Step '{step.name}' exceeded max retries "
            f"({retry_count} {'retry' if retry_count == 1 else 'retries'})"
        )
        print(f"[Workflows] '{req.workflow_run_id}' - {error_message}")

        # Fail the step via event
        await world.events_create(
            req.workflow_run_id,
            w.StepFailedEventData(
                error=error_message, stack=step_run.error.stack if step_run.error else None
            ).into_event(req.step_id),
        )

        # Re-invoke the workflow to handle the failed step
        await world.queue(
            f"__wkf_workflow_{req.workflow_name}",
            w.WorkflowInvokePayload(runId=req.workflow_run_id, requestedAt=datetime.now(UTC)),
        )
        return None

    try:
        # Check step status
        if step_run.status not in ["pending", "running"]:
            print(
                f"[Workflows] '{req.workflow_run_id}' - Step invoked erroneously, "
                f"expected status 'pending' or 'running', got '{step_run.status}' instead, "
                f"skipping execution"
            )

            # Re-enqueue workflow if step is in terminal state
            is_terminal_step = step_run.status in ["completed", "failed", "cancelled"]
            if is_terminal_step:
                await world.queue(
                    f"__wkf_workflow_{req.workflow_name}",
                    w.WorkflowInvokePayload(runId=req.workflow_run_id),
                )
            return None

        # Start the step via event (increments attempt counter)
        start_result = await world.events_create(
            req.workflow_run_id,
            w.StepStartedEvent(correlationId=req.step_id),
        )

        # Use the step entity from the event response
        if not start_result.step:
            raise RuntimeError(f"step_started event for '{req.step_id}' did not return step entity")
        step_run = start_result.step

        current_attempt = step_run.attempt

        if not step_run.started_at:
            raise RuntimeError(f"Step '{req.step_id}' has no 'startedAt' timestamp")

        # Deserialize step input
        if not step_run.input[0].startswith(b"json"):
            raise RuntimeError(f"Unsupported step input encoding for step {req.step_id}")
        args, kwargs = json.loads(step_run.input[0][len(b"json") :].decode())

        # Execute the step function
        result = await step.func(*args, **kwargs)

        # Serialize the result
        output = b"json" + json.dumps(result).encode()

        # Complete the step via event
        await world.events_create(
            req.workflow_run_id,
            w.StepCompletedEventData(result=[output]).into_event(req.step_id),
        )

    except Exception as e:
        # TODO: Check if this is a fatal error (would need FatalError class)
        # For now, treat all errors as potentially retryable

        # step.attempt was incremented by step_started
        current_attempt = step_run.attempt

        # Check if max retries reached
        if current_attempt >= step.max_retries + 1:
            # Max retries reached
            retry_count = step_run.attempt - 1
            error_message = (
                f"Step '{step.name}' failed after {step.max_retries} "
                f"{'retry' if step.max_retries == 1 else 'retries'}: {str(e)}"
            )
            print(
                f"[Workflows] '{req.workflow_run_id}' - Encountered Error "
                f"while executing step '{step.name}' (attempt {step_run.attempt}, "
                f"{retry_count} {'retry' if retry_count == 1 else 'retries'}): "
                f"{str(e)}\n\n  Max retries reached\n  Bubbling error to parent workflow"
            )

            # Fail the step via event
            error_stack = traceback.format_exc()
            await world.events_create(
                req.workflow_run_id,
                w.StepFailedEventData(error=error_message, stack=error_stack).into_event(
                    req.step_id
                ),
            )
        else:
            # Not at max retries yet - retry the step
            print(
                f"[Workflows] '{req.workflow_run_id}' - Encountered Error "
                f"while executing step '{step.name}' (attempt {current_attempt}): "
                f"{str(e)}\n\n  This step has failed but will be retried"
            )

            # Set step to pending for retry
            error_stack = traceback.format_exc()
            await world.events_create(
                req.workflow_run_id,
                w.StepRetryingEventData(error=str(e), stack=error_stack).into_event(req.step_id),
            )

            # Return timeout to keep message visible for retry
            return 1.0

    # Re-invoke the workflow to continue execution
    await world.queue(
        f"__wkf_workflow_{req.workflow_name}",
        w.WorkflowInvokePayload(runId=req.workflow_run_id, requestedAt=datetime.now(UTC)),
    )
    return None


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
    input_data = b"json" + json.dumps([args, kwargs], sort_keys=True).encode()
    data = w.RunCreatedEventData(
        deploymentId=deployment_id, workflowName=wf.workflow_id, input=[input_data]
    )
    result = await world.events_create(None, data.into_event())

    # Assert that the run was created
    if not result.run:
        raise RuntimeError("Missing 'run' in server response for 'run_created' event")

    run_id = result.run.run_id
    await world.queue(
        f"__wkf_workflow_{wf.workflow_id}",
        w.WorkflowInvokePayload(runId=run_id),
        deployment_id=deployment_id,
    )

    return Run(run_id)
