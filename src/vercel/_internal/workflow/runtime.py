import asyncio
import contextvars
import dataclasses
import functools
import importlib
import json
import random
import re
import traceback
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Generic, Literal, ParamSpec, TypeVar

import anyio
import pydantic

from vercel._internal.polyfills import UTC, Self

from . import core, nanoid, ulid, world as w
from .py_sandbox import workflow_sandbox

P = ParamSpec("P")
T = TypeVar("T")
SUSPENDED_MESSAGE = "<WORKFLOW SUSPENDED>"


@dataclasses.dataclass(kw_only=True)
class BaseSuspension:
    correlation_id: str
    has_created_event: bool = False


@dataclasses.dataclass(kw_only=True)
class Suspension(BaseSuspension, Generic[T]):
    step: core.Step[Any, T]
    input: bytes
    future: asyncio.Future[T] = dataclasses.field(default_factory=asyncio.Future)


@dataclasses.dataclass(kw_only=True)
class Wait(BaseSuspension):
    resume_at: datetime
    future: asyncio.Future[None] = dataclasses.field(default_factory=asyncio.Future)


@dataclasses.dataclass(kw_only=True)
class Hook(BaseSuspension, Generic[T]):
    token: str
    disposed: bool = False
    futures: deque[asyncio.Future[T]] = dataclasses.field(default_factory=deque)
    hook_cls: type[T]

    def set_result(self, raw_data: Any) -> None:
        if dataclasses.is_dataclass(self.hook_cls):
            res = self.hook_cls(**raw_data)
        elif issubclass(self.hook_cls, pydantic.BaseModel):
            res = self.hook_cls.model_validate(raw_data)
        else:
            raise RuntimeError(f"Invalid hook type for {self.hook_cls}")
        self.futures.popleft().set_result(res)


class WorkflowOrchestratorContext:
    _ctx: contextvars.ContextVar[Self] = contextvars.ContextVar("WorkflowContext")

    def __init__(
        self, events: list[w.Event], *, seed: str, started_at: int, registry: core.Workflows
    ):
        self.events = events
        self.replay_index = 0
        prng = random.Random(seed)
        self.generate_ulid = functools.partial(ulid.monotonic_factory(prng.random), started_at)
        self.generate_nanoid = nanoid.custom_random(nanoid.URL_ALPHABET, 21, prng.random)
        self._fut: asyncio.Future[Any] | None = None
        self.suspensions: dict[str, BaseSuspension] = {}
        self.hooks: dict[str, Hook] = {}
        self.resume_handle: asyncio.Handle | None = None
        self.registry = registry

    @classmethod
    def current(cls) -> Self:
        return cls._ctx.get()

    async def run_workflow(self: Self, workflow_run: w.WorkflowRun) -> Any:
        wf = self.registry.get_workflow(workflow_run.workflow_name)
        if not workflow_run.input or not isinstance(workflow_run.input, list):
            raise RuntimeError(f"Invalid workflow input for run {workflow_run.run_id}")
        if not workflow_run.input[0].startswith(b"json"):
            raise RuntimeError(f"Unsupported workflow input encoding for run {workflow_run.run_id}")
        args, kwargs = json.loads(workflow_run.input[0][len(b"json") :].decode())

        with workflow_sandbox(random_seed=workflow_run.run_id):
            mod = importlib.import_module(wf.module)

            # Resolve the sandboxed Workflow by qualname from the
            # re-imported module.
            obj: Any = mod
            for attr in wf.qualname.split("."):
                obj = getattr(obj, attr)

            token = self._ctx.set(self)
            try:
                self._fut = asyncio.ensure_future(obj.func(*args, **kwargs))
            finally:
                self._ctx.reset(token)
            return await self._fut

    async def run_step(self, step: core.Step[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        input_data = b"json" + json.dumps((args, kwargs), sort_keys=True).encode()
        sus = Suspension(correlation_id=f"step_{self.generate_ulid()}", step=step, input=input_data)
        self.suspensions[sus.correlation_id] = sus
        if self.resume_handle is None:
            self.resume_handle = asyncio.get_running_loop().call_soon(self.resume)
        return await sus.future

    async def run_wait(self, param: int | float | datetime | str) -> None:
        wait = Wait(
            correlation_id=f"wait_{self.generate_ulid()}",
            resume_at=(parse_duration_to_date(param)),
        )
        self.suspensions[wait.correlation_id] = wait
        if self.resume_handle is None:
            self.resume_handle = asyncio.get_running_loop().call_soon(self.resume)
        await wait.future

    def create_hook(self, token: str | None, hook_cls: type[T]) -> core.HookEvent[T]:
        hook = Hook(
            correlation_id=f"hook_{self.generate_ulid()}",
            token=token or self.generate_nanoid(),
            hook_cls=hook_cls,
        )
        self.hooks[hook.correlation_id] = hook
        return core.HookEvent(correlation_id=hook.correlation_id, token=hook.token)

    async def run_hook(self, *, correlation_id: str) -> T:
        hook = self.hooks[correlation_id]
        if hook.disposed:
            raise StopAsyncIteration
        self.suspensions[hook.correlation_id] = hook
        fut = asyncio.Future[T]()
        hook.futures.append(fut)
        if self.resume_handle is None:
            self.resume_handle = asyncio.get_running_loop().call_soon(self.resume)
        return await fut

    def dispose_hook(self, *, correlation_id: str) -> None:
        hook = self.hooks[correlation_id]
        hook.disposed = True
        while hook.futures:
            hook.futures.popleft().set_exception(StopAsyncIteration)

    def resume(self) -> None:
        self.resume_handle = None

        if self._fut is None:
            return

        while self.replay_index < len(self.events) and self.suspensions:
            event = self.events[self.replay_index]
            self.replay_index += 1

            match event:
                case w.StepCreatedEvent() | w.HookCreatedEvent() | w.WaitCreatedEvent():
                    self.suspensions[event.correlation_id].has_created_event = True

                case w.StepCompletedEvent(event_data=w.StepCompletedEventData(result=data)):
                    sus = self.suspensions.pop(event.correlation_id)
                    assert isinstance(sus, Suspension)
                    if data[0].startswith(b"json"):
                        result = json.loads(data[0][len(b"json") :].decode())
                    else:
                        self._fut.cancel(
                            f"Unsupported step result encoding for "
                            f"correlation ID {event.correlation_id}"
                        )
                        return
                    sus.future.set_result(result)

                case w.WaitCompletedEvent():
                    wait = self.suspensions.pop(event.correlation_id)
                    assert isinstance(wait, Wait)
                    wait.future.set_result(None)

                case w.StepFailedEvent(event_data=w.StepFailedEventData(error=e)):
                    sus = self.suspensions.pop(event.correlation_id)
                    assert isinstance(sus, Suspension)
                    sus.future.set_exception(RuntimeError(e))

                case w.HookConflictEvent(event_data=w.HookConflictEventData(token=token)):
                    hook = self.suspensions.pop(event.correlation_id, None)
                    if hook is not None:
                        assert isinstance(hook, Hook)
                        while hook.futures:
                            hook.futures.popleft().set_exception(
                                RuntimeError(
                                    f'Hook token "{token}" is already in use by another workflow'
                                )
                            )

                case w.HookReceivedEvent(event_data=w.HookReceivedEventData(payload=data)):
                    hook = self.suspensions[event.correlation_id]
                    assert isinstance(hook, Hook)
                    if data[0].startswith(b"json"):
                        result = json.loads(data[0][len(b"json") :].decode())
                    else:
                        self._fut.cancel(
                            f"Unsupported step result encoding for "
                            f"correlation ID {event.correlation_id}"
                        )
                        return
                    hook.set_result(result)
                    if not hook.futures:
                        self.suspensions.pop(event.correlation_id)

                case w.HookDisposedEvent():
                    self.suspensions.pop(event.correlation_id, None)
                    self.hooks.pop(event.correlation_id)

        if self.suspensions:
            self._fut.cancel(SUSPENDED_MESSAGE)


async def workflow_handler(
    message: Any,
    *,
    attempt: int,
    queue_name: str,
    message_id: str,
    registry: core.Workflows,
) -> float | None:
    world = w.get_world()
    run_id = w.WorkflowInvokePayload.model_validate(message).run_id
    workflow_run = await world.runs_get(run_id)
    if workflow_run.status == "pending":
        result = await world.events_create(run_id, w.RunStartedEvent())
        assert result.run is not None
        workflow_run = result.run
    elif workflow_run.status == "cancelled":
        return None

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

    context = WorkflowOrchestratorContext(
        events, seed=run_id, started_at=workflow_started_at, registry=registry
    )
    try:
        result = await context.run_workflow(workflow_run)
        output = b"json" + json.dumps(result).encode()
    except BaseException as e:
        if isinstance(e, asyncio.CancelledError) and e.args and e.args[0] == SUSPENDED_MESSAGE:
            # Workflow suspended, continue outside the try..except block
            pass
        elif isinstance(e, Exception):
            await world.events_create(
                run_id,
                w.RunFailedEventData(error=str(e)).into_event(),
            )
            return None
        else:
            raise
    else:
        await world.events_create(
            run_id,
            w.RunCompletedEventData(output=[output]).into_event(),
        )
        return None

    async with anyio.create_task_group() as tg:
        for sus in context.suspensions.values():
            if sus.has_created_event:
                pass
            elif isinstance(sus, Suspension):
                step_data = w.StepCreatedEventData(stepName=sus.step.name, input=[sus.input])
                tg.start_soon(world.events_create, run_id, step_data.into_event(sus.correlation_id))
                tg.start_soon(
                    world.queue,
                    f"__wkf_step_{sus.step.name}",
                    w.StepInvokePayload(
                        workflowName=workflow_run.workflow_name,
                        workflowRunId=run_id,
                        workflowStartedAt=workflow_started_at,
                        stepId=sus.correlation_id,
                        requestedAt=datetime.now(UTC),
                    ),
                )
            elif isinstance(sus, Wait):
                wait_data = w.WaitCreatedEventData(resumeAt=sus.resume_at)
                tg.start_soon(world.events_create, run_id, wait_data.into_event(sus.correlation_id))
            elif isinstance(sus, Hook):
                hook_data = w.HookCreatedEventData(token=sus.token)
                tg.start_soon(world.events_create, run_id, hook_data.into_event(sus.correlation_id))

        for hook in context.hooks.values():
            if hook.disposed:
                tg.start_soon(
                    world.events_create,
                    run_id,
                    w.HookDisposedEvent(correlationId=hook.correlation_id),
                )

    now = datetime.now(UTC)
    min_timeout_seconds = -1.0
    for sus in context.suspensions.values():
        if isinstance(sus, Wait):
            seconds = (sus.resume_at - now).total_seconds()
            if min_timeout_seconds < 0:
                min_timeout_seconds = seconds
            else:
                min_timeout_seconds = min(min_timeout_seconds, seconds)
    return None if min_timeout_seconds < 0 else min_timeout_seconds


async def step_handler(
    message: Any,
    *,
    attempt: int,
    queue_name: str,
    message_id: str,
    registry: core.Workflows,
) -> float | None:
    world = w.get_world()
    req = w.StepInvokePayload.model_validate(message)

    # Get the step entity
    step_run = await world.steps_get(req.workflow_run_id, req.step_id)
    step = registry.get_step(step_run.step_name)

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
            w.WorkflowInvokePayload(
                runId=req.workflow_run_id,
                requestedAt=datetime.now(UTC),
            ),
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
        if not step_run.input:
            raise RuntimeError(f"Step '{req.step_id}' has no input")
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
        w.WorkflowInvokePayload(
            runId=req.workflow_run_id,
            requestedAt=datetime.now(UTC),
        ),
    )
    return None


def workflow_entrypoint(registry: core.Workflows) -> w.HTTPHandler:
    return w.get_world().create_queue_handler(
        "__wkf_workflow_",
        functools.partial(workflow_handler, registry=registry),
    )


def step_entrypoint(registry: core.Workflows) -> w.HTTPHandler:
    return w.get_world().create_queue_handler(
        "__wkf_step_",
        functools.partial(step_handler, registry=registry),
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


duration_re = re.compile(
    r"(-?\d+(?:\.\d+)?)\s*(ms|s|seconds?|m|minutes?|h|hours?|d|days?|w|weeks?)",
    re.IGNORECASE,
)
duration_units = {
    "ms": 1,
    "s": 1_000,
    "second": 1_000,
    "seconds": 1_000,
    "m": 60 * 1_000,
    "minute": 60 * 1_000,
    "minutes": 60 * 1_000,
    "h": 60 * 60 * 1_000,
    "hour": 60 * 60 * 1_000,
    "hours": 60 * 60 * 1_000,
    "d": 24 * 60 * 60 * 1_000,
    "day": 24 * 60 * 60 * 1_000,
    "days": 24 * 60 * 60 * 1_000,
    "w": 7 * 24 * 60 * 60 * 1_000,
    "week": 7 * 24 * 60 * 60 * 1_000,
    "weeks": 7 * 24 * 60 * 60 * 1_000,
}


def parse_duration_to_date(param: int | float | datetime | str) -> datetime:
    if isinstance(param, str):
        items = [float(v) * duration_units[u] for v, u in duration_re.findall(param)]
        if not items:
            raise RuntimeError(f"Invalid duration parameter: {param}")
        ms = sum(items)
        if ms < 0:
            raise RuntimeError(f"Duration parameter must be non-negative: {param}")
        return datetime.now(UTC) + timedelta(milliseconds=ms)

    elif isinstance(param, (int, float)):
        if param < 0:
            raise RuntimeError(f"Duration parameter must be non-negative: {param}")
        return datetime.now(UTC) + timedelta(milliseconds=param)

    elif isinstance(param, datetime):
        if param.tzinfo is None:
            raise RuntimeError("Duration parameter must have tzinfo")
        return param

    else:
        raise RuntimeError(f"Invalid duration parameter: {param}")


class Run:
    def __init__(self, run_id: str) -> None:
        self._run_id = run_id
        self._world = w.get_world()

    @property
    def run_id(self) -> str:
        return self._run_id

    async def status(self) -> Literal["pending", "running", "completed", "failed", "cancelled"]:
        run = await self._world.runs_get(self._run_id)
        return run.status

    async def return_value(self) -> Any:
        while True:
            run = await self._world.runs_get(self._run_id)
            if run.status == "completed":
                if not run.output:
                    raise RuntimeError(f"Completed workflow {run.run_id} has no output")
                if not run.output[0].startswith(b"json"):
                    raise RuntimeError(f"Unsupported workflow output encoding for {run.run_id}")
                return json.loads(run.output[0][len(b"json") :].decode())

            elif run.status == "cancelled":
                raise RuntimeError("workflow cancelled")

            elif run.status == "failed":
                raise RuntimeError("workflow failed")

            else:
                await asyncio.sleep(1)


async def start(wf: core.Workflow[P, T], *args: P.args, **kwargs: P.kwargs) -> Run:
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


async def resume_hook(token_or_hook: str | w.Hook, payload_json: str) -> w.Hook:
    world = w.get_world()
    if isinstance(token_or_hook, str):
        hook = await world.hooks_get_by_token(token_or_hook)
    else:
        hook = token_or_hook
    run = await world.runs_get(hook.run_id)
    payload = b"json" + payload_json.encode()
    data = w.HookReceivedEventData(payload=[payload])
    await world.events_create(hook.run_id, data.into_event(hook.hook_id))
    await world.queue(
        f"__wkf_workflow_{run.workflow_name}",
        w.WorkflowInvokePayload(runId=hook.run_id),
    )
    return hook
