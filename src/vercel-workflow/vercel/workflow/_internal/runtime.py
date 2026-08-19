import asyncio
import contextlib
import contextvars
import dataclasses
import functools
import importlib
import json
import logging
import math
import os
import random
import re
import sys
import traceback
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine
from datetime import datetime, timedelta
from typing import Any, Generic, Literal, ParamSpec, TypeVar
from urllib.parse import parse_qsl, urlsplit

import anyio
import pydantic

from vercel._internal.core.polyfills import UTC, Self

from . import core, errors, loop, nanoid, serialization as ser, streams, ulid, world as w
from .py_sandbox import workflow_sandbox

P = ParamSpec("P")
T = TypeVar("T")
logger = logging.getLogger("vercel.workflow")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Wait-continuation dispatch — mirrors @workflow/core's wait-continuation.ts.
# The delayed re-enqueue that wakes a run when a pending wait elapses is keyed on
# the wait's correlation id, so repeated suspension passes over the same pending
# wait dedupe to a single timer. Long waits chain in <=23h hops (suffixed with the
# hop index so the chain advances while same-hop re-observations still dedupe);
# near-elapsed waits get a per-second bucket so an early delivery can re-enqueue.
WAIT_CONTINUATION_MAX_DELAY_SECONDS = 82_800
NEAR_ELAPSED_WAIT_THRESHOLD_SECONDS = 2


def _wait_continuation_dispatch(
    timeout_seconds: int, wait_correlation_id: str, now: datetime
) -> tuple[float, str]:
    if timeout_seconds <= NEAR_ELAPSED_WAIT_THRESHOLD_SECONDS:
        return timeout_seconds, f"{wait_correlation_id}:{int(now.timestamp())}"
    hop = math.ceil(timeout_seconds / WAIT_CONTINUATION_MAX_DELAY_SECONDS)
    delay = min(timeout_seconds, WAIT_CONTINUATION_MAX_DELAY_SECONDS)
    key = wait_correlation_id if hop == 1 else f"{wait_correlation_id}:hop-{hop}"
    return delay, key


class _SuspendException(BaseException):
    """Raised to trigger suspending of the workflow."""


class NondeterminismError(Exception):
    """Raised when a workflow's replay diverges from its recorded event log.

    Correlation IDs are assigned positionally (the Nth step/sleep/hook of a body
    run gets the Nth seeded ID), so if the body issues operations in a different
    order or with different arguments on replay, recorded results would be
    matched onto the wrong calls. This is raised instead, failing the run.
    """


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
    has_dispose_event: bool = False
    futures: deque[asyncio.Future[T]] = dataclasses.field(default_factory=deque)
    hook_cls: type[T]
    metadata: bytes | None = None

    def set_result(self, raw_data: Any) -> None:
        if dataclasses.is_dataclass(self.hook_cls):
            res = self.hook_cls(**raw_data)
        elif issubclass(self.hook_cls, pydantic.BaseModel):
            res = self.hook_cls.model_validate(raw_data)
        else:
            raise RuntimeError(f"Invalid hook type for {self.hook_cls}")
        while self.futures:
            fut = self.futures.popleft()
            # The future might be cancelled by the user
            if not fut.done():
                fut.set_result(res)
                break
        else:
            logger.warning("Hook %r resumed but no handler needs it; ignoring: %r", self.token, res)


def _correlation_kind(correlation_id: str) -> str:
    """The kind prefix of a correlation ID (``step`` / ``wait`` / ``hook``)."""
    return correlation_id.split("_", 1)[0]


def _correlation_ulid(correlation_id: str) -> str:
    """The positional ULID of a correlation ID, stripped of its kind prefix.

    Correlation IDs are ``<kind>_<ulid>`` where the ULID is assigned positionally
    from a run-seeded monotonic factory. Two calls at the same body position share
    a ULID regardless of kind, so this is the slot identity used to spot a
    step/wait/hook swap during replay.
    """
    return correlation_id.split("_", 1)[-1]


@dataclasses.dataclass(frozen=True)
class StepInfo:
    """Metadata about the step currently executing.

    Mirrors the JS SDK's ``getStepMetadata()`` return value. ``step_id`` is stable
    across retries and unique per logical step call, which makes it a good
    idempotency key for non-idempotent side effects (payments, emails, queue
    sends) performed inside a step body.
    """

    run_id: str
    step_id: str
    step_name: str
    attempt: int


_step_ctx: contextvars.ContextVar[StepInfo] = contextvars.ContextVar("WorkflowStepContext")


@dataclasses.dataclass
class _StepStreams:
    """The stream writers one step invocation opened.

    Kept out of :class:`StepInfo` because that is public, frozen metadata; this
    is mutable bookkeeping the handler owns.
    """

    run_id: str
    writers: dict[tuple[str, str], streams.WorkflowStreamWriter] = dataclasses.field(
        default_factory=dict
    )
    _task_group: anyio.abc.TaskGroup | None = None

    @contextlib.asynccontextmanager
    async def dispatching(self) -> AsyncIterator[None]:
        """Own the writers' background sends for the duration of the block.

        A writer sends from a task rather than from `write()`, so those tasks
        need a group whose lifetime covers every write *and* the flush that
        follows. Leaving the block waits for them, so nothing is still in
        flight once it returns.

        It has to open before the step's input is hydrated, not just around the
        body: a stream the workflow passed in becomes a writer while the input
        is being read, and a writer with no group behind it cannot send.

        If the block raises, the chunks the step did manage to stream are
        flushed first: those are as real as any other, and a reader tailing the
        run should see the progress that led up to the failure. Best-effort by
        construction -- the step is already failing, and a flush error here
        would replace the cause with a symptom. (`@workflow/core` does not do
        this at all: its `ops` flush lives only on the success path, so a
        throwing step's buffered chunks are never awaited.)

        The error comes back out as itself rather than wrapped in the task
        group's exception group, because callers up the stack switch on
        `FatalError` and count retries.
        """
        error: BaseException | None = None
        async with anyio.create_task_group() as task_group:
            self._task_group = task_group
            try:
                yield
            except Exception as exc:
                error = exc
                await self.drain_quietly()
            except BaseException as exc:
                # Cancellation, or the interpreter going down. Draining would
                # need a shield and could stall the teardown it is racing.
                error = exc
            finally:
                self._task_group = None
        if error is not None:
            raise error

    def writer(
        self, namespace: str | None, *, reentrant_ctx_on_err: bool = True
    ) -> streams.WorkflowStreamWriter:
        """The writer for this run's *namespace* stream."""
        return self.writer_for(
            self.run_id,
            streams.workflow_run_stream_id(self.run_id, namespace),
            reentrant_ctx_on_err=reentrant_ctx_on_err,
        )

    def writer_for(
        self, run_id: str, name: str, *, reentrant_ctx_on_err: bool = True
    ) -> streams.WorkflowStreamWriter:
        """The writer for one stream, created on first use.

        One writer per stream per step, deliberately. Handing out a fresh writer
        each call would give each its own buffer over the same stream, and two
        buffers flushing independently interleave their chunks by whichever
        request happens to win -- so `get_writable()` twice in a loop would
        scramble the order the caller wrote in. Sharing one serial sink makes
        that pattern correct instead of subtly wrong, and it is why a handle the
        workflow passed in resolves to the same writer the step would have got
        by asking for the stream itself.
        """
        if self._task_group is None:
            raise RuntimeError("stream writers are only available while a step is running")
        key = (run_id, name)
        writer = self.writers.get(key)
        if writer is None:
            writer = streams.WorkflowStreamWriter(
                world=w.get_world(),
                run_id=run_id,
                name=name,
                task_group=self._task_group,
                reentrant_ctx_on_err=reentrant_ctx_on_err,
            )
            self.writers[key] = writer
        return writer

    async def drain(self) -> None:
        for writer in self.writers.values():
            await writer.drain()

    async def drain_quietly(self) -> None:
        for writer in self.writers.values():
            try:
                await writer.drain()
            except Exception:
                logger.debug(
                    "[Workflows] '%s' - could not flush stream %r while failing",
                    self.run_id,
                    writer.name,
                    exc_info=True,
                )


_step_streams_ctx: contextvars.ContextVar[_StepStreams] = contextvars.ContextVar(
    "WorkflowStepStreams"
)


def get_step_metadata() -> StepInfo:
    """Return metadata for the step currently executing.

    Must be called from within a step body; raises ``RuntimeError`` otherwise.
    """
    try:
        return _step_ctx.get()
    except LookupError:
        raise RuntimeError("get_step_metadata() can only be called inside a step") from None


def get_writable(
    *,
    namespace: str | None = None,
    reentrant_ctx_on_err: bool = True,
) -> streams.WorkflowWritable:
    """The run's writable stream, for streaming output while the run works.

    Chunks are readable straight away, through :meth:`Run.readable`, the
    TypeScript SDK, the dashboard or ``workflow inspect stream`` -- no waiting
    for the step or the run to finish.

    In a step this is a :class:`~.streams.WorkflowStreamWriter`, ready to write.
    In a workflow body it is a :class:`~.streams.WorkflowStreamHandle`, which
    refers to the stream but cannot write to it: that body re-executes on every
    replay and its sandbox has no network. Pass the handle to a step and it
    arrives as the writer -- the same one the step would get by asking for the
    stream itself, so ordering holds however the step obtained it.

    Pass *namespace* to write to a second, independent stream on the same run.

    Nothing closes the stream implicitly. Call ``close()`` on the writer when
    the run has nothing more to say, or readers will wait until the run expires.

    When used as an asynchronous context in ``async with`` statements, the stream
    will be closed on a clean exit by default. Exceptions will not close the
    stream on exit of the context, unless ``reentrant_ctx_on_err`` is ``False``.
    """
    try:
        state = _step_streams_ctx.get()
    except LookupError:
        pass
    else:
        return state.writer(namespace, reentrant_ctx_on_err=reentrant_ctx_on_err)

    try:
        ctx = WorkflowOrchestratorContext.current()
    except LookupError:
        raise RuntimeError(
            "get_writable() can only be called inside a workflow or a step"
        ) from None
    return ctx.stream_handle(namespace)


def open_writable(run_id: str | None, name: str) -> streams.WorkflowWritable:
    """Turn a serialized stream reference back into something writable.

    The reviving half of the ``WritableStream`` tag, so a handle a workflow put
    in a step's arguments arrives as something the step can write to. Inside a
    step it is a writer registered with that step, which is what gets it
    drained before the step is recorded complete.

    Outside one -- a client hydrating a payload that happens to carry a stream
    -- it stays a handle. A writer sends from a task in the step handler's
    group, and there is no such group here, so there would be nothing to own
    the sends or to guarantee they finished.
    """
    try:
        state = _step_streams_ctx.get()
    except LookupError:
        if run_id is None:
            raise ser.SerializationError(
                f"stream {name!r} arrived without a run id and there is no step to take it from"
            ) from None
        return streams.WorkflowStreamHandle(run_id, name)
    return state.writer_for(run_id or state.run_id, name)


if sys.version_info >= (3, 11):

    def _run_in_loop(
        coro: Coroutine[Any, Any, T],
        *,
        loop_factory: Callable[[], asyncio.AbstractEventLoop],
    ) -> T:
        with asyncio.Runner(loop_factory=loop_factory) as runner:
            return runner.run(coro)

else:

    def _cancel_all_tasks(loop: asyncio.AbstractEventLoop) -> None:
        tasks = asyncio.all_tasks(loop)
        if not tasks:
            return

        for task in tasks:
            task.cancel()

        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

        for task in tasks:
            if task.cancelled():
                continue
            exception = task.exception()
            if exception is not None:
                loop.call_exception_handler(
                    {
                        "message": "unhandled exception during workflow loop shutdown",
                        "exception": exception,
                        "task": task,
                    }
                )

    def _run_in_loop(
        coro: Coroutine[Any, Any, T],
        *,
        loop_factory: Callable[[], asyncio.AbstractEventLoop],
    ) -> T:
        """Python 3.10 backport of the lifecycle used by ``asyncio.Runner``."""
        loop = loop_factory()
        try:
            return loop.run_until_complete(coro)
        finally:
            try:
                _cancel_all_tasks(loop)
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
            finally:
                loop.close()


def _run_isolated(
    coro: Coroutine[Any, Any, T],
    *,
    loop_factory: Callable[[], asyncio.AbstractEventLoop],
) -> T:
    # The workflow is async, but it is not actually allowed to perform
    # any IO-full operations, and resume/resume_wrapper require that it
    # run in an isolated loop.
    #
    # So hide our existing loop and run everything in a fresh loop.
    old_loop = asyncio.get_running_loop()
    current_task = asyncio.current_task()
    if current_task:
        asyncio._leave_task(old_loop, current_task)
    asyncio._set_running_loop(None)
    try:
        return _run_in_loop(coro, loop_factory=loop_factory)
    finally:
        asyncio._set_running_loop(old_loop)
        if current_task:
            asyncio._enter_task(old_loop, current_task)


class WorkflowOrchestratorContext:
    _ctx: contextvars.ContextVar[Self] = contextvars.ContextVar("WorkflowContext")

    def __init__(
        self,
        events: list[w.Event],
        *,
        run_id: str,
        seed: str,
        started_at: int,
        registry: core.Workflows,
        run_key: bytes | None = None,
    ):
        self.run_id = run_id
        self.events = events
        # The key is per-run, so it is resolved once and reused for every
        # payload in the replay.
        self.run_key = run_key
        # List of Out-of-order HookReceivedEvent: such events may arrive at any time unexpectedly,
        # so we stash them separately for delayed consumption
        self.ooo_hook_received_events: deque[w.HookReceivedEvent] = deque()
        self.replay_index = 0
        prng = random.Random(seed)
        self.generate_ulid = functools.partial(ulid.monotonic_factory(prng.random), started_at)
        self.generate_nanoid = nanoid.custom_random(nanoid.URL_ALPHABET, 21, prng.random)
        self._user_random = random.Random(f"{seed}:random")
        self.suspensions: dict[str, BaseSuspension] = {}
        self.hooks: dict[str, Hook] = {}
        self.registry = registry

    @classmethod
    def current(cls) -> Self:
        return cls._ctx.get()

    def run_workflow(self: Self, workflow_run: w.WorkflowRun) -> bytes:
        """Run the body inside the sandbox, returning its result serialized there."""
        wf = self.registry._get_workflow(workflow_run.workflow_name)
        if not workflow_run.input:
            raise RuntimeError(f"Invalid workflow input for run {workflow_run.run_id}")

        with workflow_sandbox(policy=self.registry._sandbox_policy):
            mod = importlib.import_module(wf.module)

            # Resolve the sandboxed Workflow by qualname from the
            # re-imported module.
            obj: Any = mod
            for attr in wf.qualname.split("."):
                obj = getattr(obj, attr)

            what = f"the input of run {workflow_run.run_id}"
            args, kwargs = ser.call_arguments(
                ser.hydrate(workflow_run.input, what=what, key=self.run_key), what=what
            )

            token = self._ctx.set(self)
            try:
                return ser.dehydrate(
                    _run_isolated(
                        obj.func(*args, **kwargs),
                        loop_factory=lambda: loop.WorkflowLoop(idle_hook=self.resume),
                    )
                )
            finally:
                self._ctx.reset(token)

    async def run_step(self, step: core.Step[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        # Bound to the step's own signature, so the recorded bytes depend on it
        # rather than on how the body spelled the call -- see
        # `core._bind_arguments`, which the determinism check relies on.
        bound_args, bound_kwargs = step.bind_arguments(args, kwargs)
        input_data = ser.dehydrate(ser.step_arguments(bound_args, bound_kwargs))
        sus = Suspension(correlation_id=f"step_{self.generate_ulid()}", step=step, input=input_data)
        self.suspensions[sus.correlation_id] = sus
        return await sus.future

    async def run_wait(self, param: int | float | datetime | str) -> None:
        wait = Wait(
            correlation_id=f"wait_{self.generate_ulid()}",
            resume_at=(parse_duration_to_date(param)),
        )
        self.suspensions[wait.correlation_id] = wait
        await wait.future

    def now(self) -> datetime:
        if not self.events:
            raise RuntimeError("now() requires at least one event in the run's event log")
        event = self.events[max(self.replay_index - 1, 0)]
        assert event.server_props is not None
        return event.server_props.created_at

    def time_ns(self) -> int:
        delta = self.now() - _EPOCH
        return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000

    def random(self) -> random.Random:
        return self._user_random

    def stream_handle(self, namespace: str | None) -> streams.WorkflowStreamHandle:
        """A reference to one of this run's streams, for a step to write to.

        Deterministic, so a replay of the body produces the same handle rather
        than pointing a later attempt at a different stream.
        """
        return streams.WorkflowStreamHandle(
            self.run_id, streams.workflow_run_stream_id(self.run_id, namespace)
        )

    def create_hook(
        self, token: str | None, hook_cls: type[T], *, metadata: Any = None
    ) -> core.HookEvent[T]:
        hook = Hook(
            correlation_id=f"hook_{self.generate_ulid()}",
            token=token or self.generate_nanoid(),
            hook_cls=hook_cls,
            metadata=None if metadata is None else ser.dehydrate(metadata),
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
        return await fut

    def dispose_hook(self, *, correlation_id: str) -> None:
        hook = self.hooks[correlation_id]
        hook.disposed = True
        while hook.futures:
            fut = hook.futures.popleft()
            if not fut.done():
                fut.set_exception(StopAsyncIteration)
        self.suspensions.pop(correlation_id, None)

    def _fail_suspension(self, sus: BaseSuspension, exc: Exception) -> None:
        """Surface ``exc`` on whichever future(s) a suspension is awaiting.

        The error propagates through the body's ``await`` of the step/wait/hook,
        out of ``run_workflow``, and into the run-failed path.
        """
        if isinstance(sus, Hook):
            while sus.futures:
                fut = sus.futures.popleft()
                if not fut.done():
                    fut.set_exception(exc)
        elif isinstance(sus, (Suspension, Wait)) and not sus.future.done():
            sus.future.set_exception(exc)

    def resume(self) -> None:
        """Run over the the event log and try to apply an event.

        The idea is that resume() will be run whenever everything else in the
        async event loop has paused.

        The core invariant of how resume() interacts with the workflow
        is that the execution needs to appear identical to the events
        arriving one at a time (because when they originally arrive
        they are one at a time, and it needs to be indistinguishable
        from that.)

        Resolves at most one suspension before breaking.

        If the event log is exhausted, cancel the future and suspend
        the workflow.
        """

        # NOTE: resume() does single-step delivery, so we resolve at most
        # one suspension per invocation of resume().
        # As an optimization for processing StepCreatedEvent and similar,
        # we still structure this as a loop, though.
        # The cases that do *not* invoke a suspension will continue,
        # and the main bottom of the loop will break.
        #
        # This makes sure that resume() gets interleaved directly one
        # to one with event deliveries, instead of sometimes having
        # multiple deliveries bunched up before a resume(), which can
        # lead to mismatches between a recording trace and a replaying
        # one.
        while (
            self.replay_index < len(self.events) or self.ooo_hook_received_events
        ) and self.suspensions:
            event: w.Event | None = None
            if self.ooo_hook_received_events:
                event = self.ooo_hook_received_events[0]
                if event.correlation_id in self.suspensions:
                    self.ooo_hook_received_events.popleft()
                else:
                    event = None
            if event is None:
                if self.replay_index < len(self.events):
                    event = self.events[self.replay_index]
                    if event.correlation_id not in self.suspensions:
                        match event:
                            # In case of multitasking, one task may progress twice in a row,
                            # when we see the replayed event before actually having the
                            # suspension from the task. So we yield here and let the task
                            # suspend and resume again in next iteration.
                            case w.StepCreatedEvent() | w.HookCreatedEvent() | w.WaitCreatedEvent():
                                # ...unless the body already registered a different-kind
                                # call at this positional slot (same ULID, different
                                # prefix). A same-kind match would have hit the dict
                                # lookup above, so a ULID collision here is a step/wait/
                                # hook swap -- the body is non-deterministic. Fail loudly
                                # instead of yielding forever (the matching ID will never
                                # appear, so plain `return` would deadlock the run).
                                pos = _correlation_ulid(event.correlation_id)
                                for sus in self.suspensions.values():
                                    if _correlation_ulid(sus.correlation_id) == pos:
                                        self._fail_suspension(
                                            sus,
                                            NondeterminismError(
                                                f"workflow replay diverged at position "
                                                f"{pos}: recorded a "
                                                f"{_correlation_kind(event.correlation_id)!r} "
                                                f"call, but the body now issues a "
                                                f"{_correlation_kind(sus.correlation_id)!r} "
                                                "call. The workflow body is "
                                                "non-deterministic."
                                            ),
                                        )
                                        return
                                return
                            # HookReceivedEvent is not created from workflows, it may arrive
                            # at any time out of order. At this momemnt we don't need one,
                            # so we just stash it and continue with the event log.
                            case w.HookReceivedEvent():
                                self.ooo_hook_received_events.append(event)
                                self.replay_index += 1
                                continue
                    self.replay_index += 1
                else:
                    break

            match event:
                case w.StepCreatedEvent(
                    event_data=w.StepCreatedEventData(step_name=name, input=recorded_input)
                ):
                    sus = self.suspensions[event.correlation_id]
                    assert isinstance(sus, Suspension)
                    # The recorded step at this (positional) correlation ID must be
                    # the same call the body just issued; a mismatch means the body
                    # is non-deterministic.
                    if sus.step.name != name or sus.input != recorded_input:
                        self._fail_suspension(
                            sus,
                            NondeterminismError(
                                f"workflow replay diverged at {event.correlation_id}: "
                                f"recorded step {name!r}, but the body now calls "
                                f"{sus.step.name!r} with different arguments. The workflow "
                                "body is non-deterministic."
                            ),
                        )
                        return
                    sus.has_created_event = True
                    continue

                case w.HookCreatedEvent() | w.WaitCreatedEvent():
                    self.suspensions[event.correlation_id].has_created_event = True
                    continue

                case w.StepCompletedEvent(event_data=w.StepCompletedEventData(result=data)):
                    sus = self.suspensions.pop(event.correlation_id)
                    assert isinstance(sus, Suspension)
                    result = ser.hydrate(
                        data,
                        what=f"the result of step {event.correlation_id}",
                        key=self.run_key,
                    )
                    if not sus.future.cancelled():
                        sus.future.set_result(result)

                case w.WaitCompletedEvent():
                    wait = self.suspensions.pop(event.correlation_id)
                    assert isinstance(wait, Wait)
                    if not wait.future.cancelled():
                        wait.future.set_result(None)

                case w.StepFailedEvent(event_data=w.StepFailedEventData(error=e)):
                    sus = self.suspensions.pop(event.correlation_id)
                    assert isinstance(sus, Suspension)
                    if not sus.future.cancelled():
                        sus.future.set_exception(RuntimeError(e))

                case w.HookConflictEvent(event_data=w.HookConflictEventData(token=token)):
                    hook = self.suspensions.pop(event.correlation_id, None)
                    if hook is not None:
                        assert isinstance(hook, Hook)
                        conflict = f'Hook token "{token}" is already in use by another workflow'
                        while hook.futures:
                            future = hook.futures.popleft()
                            if not future.cancelled():
                                future.set_exception(RuntimeError(conflict))

                case w.HookReceivedEvent(event_data=w.HookReceivedEventData(payload=data)):
                    hook = self.suspensions[event.correlation_id]
                    assert isinstance(hook, Hook)
                    result = ser.hydrate(
                        data,
                        what=f"the payload of hook {event.correlation_id}",
                        key=self.run_key,
                    )
                    hook.set_result(result)
                    if not hook.futures:
                        self.suspensions.pop(event.correlation_id)

                case w.HookDisposedEvent():
                    self.hooks[event.correlation_id].has_dispose_event = True
                    self.dispose_hook(correlation_id=event.correlation_id)

            # One wake per pass: yield to the body so it drains to its next
            # suspension before we deliver the next recorded event.
            break

        # If we did not break out of the loop, that means that the
        # event log exhausted without doing any work.
        # Suspend.
        else:
            raise _SuspendException()


# ── lazy hook resume ───────────────────────────────────────────────────────
#
# A hook resume takes two writes: the `hook_received` event, and the queue
# message that wakes the run. `resumeHook()` can do both at once, which means a
# delivery can get here before the event exists. It copies the payload onto the
# message so we can write the event ourselves, which is what the code below
# does, before the run is replayed.
#
# Mirrors the same step in `@workflow/core`'s runtime.ts.


def _has_resume_event(events: list[w.Event], hook_input: w.HookResumeInput) -> bool:
    return any(
        event.event_type == "hook_received"
        and event.server_props is not None
        and event.server_props.resume_id == hook_input.resume_id
        for event in events
    )


def _resume_occurred_at(resume_id: str) -> datetime | None:
    """When the resume happened, read out of the id the producer minted for it.

    Resume ids are ULIDs, which carry the time they were made. Using it means the
    event is dated when the payload was sent rather than when this delivery got
    around to writing it. Ids that are not ULIDs (older producers, hand-written
    ones in tests) have no time to read, and the world dates the event itself.
    """
    try:
        return datetime.fromtimestamp(ulid.decode_time(resume_id) / 1000, UTC)
    except ValueError:
        return None


async def _ensure_hook_received(
    world: w.World, run: w.WorkflowRun, hook_input: w.HookResumeInput
) -> bool:
    """Write the resume's ``hook_received`` event. ``False`` means stop here.

    Safe to call when the other writer of this resume also wrote the event, or is
    writing it right now: the world is told which resume this is
    (:class:`~world.HookResume`) and collapses the two into one event.

    Note what it does *not* catch. Every other write in this module swallows
    ``EntityConflictError``; here it is allowed out. A conflict means the other
    writer has claimed this resume but its event is not readable yet, which is
    temporary -- so the message goes unacked and a later delivery finds the
    event. Swallowing it would ack a message that may hold the only copy of the
    payload.
    """
    # The producer's bytes, passed along rather than re-encoded, so the payload
    # digests on both writes match.
    #
    # The event is labelled with the run's spec version, not ours: this is
    # another writer's event and its payload is encoded to that version. Same
    # reason `run_started` carries the version of whoever created the run.
    event = w.HookReceivedEvent(
        correlationId=hook_input.hook_id,
        eventData=w.HookReceivedEventData(payload=hook_input.payload, token=hook_input.token),
        specVersion=run.spec_version or w.SPEC_VERSION_CURRENT,
    )
    try:
        await world.events_create(
            run.run_id,
            event,
            resume=w.HookResume(
                resume_id=hook_input.resume_id,
                payload_digest=hook_input.payload_digest,
                occurred_at=_resume_occurred_at(hook_input.resume_id),
            ),
        )
    except (w.HookNotFoundError, w.RunExpiredError):
        # The hook is gone, or the run has finished, so this payload can never be
        # delivered to anything. Retrying cannot change that, so ack the message
        # and stop rather than replaying a run that is not waiting for us.
        logger.debug(
            "Hook %r of run %r can no longer receive a payload; dropping resume %r",
            hook_input.hook_id,
            run.run_id,
            hook_input.resume_id,
        )
        return False
    return True


async def _resolve_run_key(
    world: w.World, run: w.WorkflowRun, events: list[w.Event]
) -> bytes | None:
    """The key this run's payloads need, or ``None`` when none of them is encrypted."""
    encrypted = ser.is_encrypted(run.input) or any(
        ser.is_encrypted(payload) for event in events for payload in event.payloads()
    )
    if not encrypted:
        return None
    return await world.run_key(run.run_id, deployment_id=run.deployment_id)


# ── health checks ──────────────────────────────────────────────────────────
#
# `workflow health`, `workflow inspect run` and a cross-deployment `start()`
# probe a workflow deployment by publishing a probe message onto the workflow
# queue. The workflow deployment then answers over a stream.
#
# Mirrors `handleHealthCheckMessage` in `@workflow/core`'s runtime/helpers.ts.


def _health_check_stream_name(correlation_id: str) -> str:
    return f"__health_check__{correlation_id}"


def _health_check_run_id(correlation_id: str) -> str:
    """The synthetic run the answer is stored under.

    No such run exists. The name only has to agree with the prober, which
    derives it the same way, because worlds scope stream reads by run.
    """
    return f"wrun_hc_{correlation_id}"


def _parse_health_check(message: Any) -> w.HealthCheckPayload | None:
    try:
        return w.HealthCheckPayload.model_validate(message)
    except pydantic.ValidationError:
        # The main non-health-check path: see workflow_handler() below
        return None


def _health_check_response(correlation_id: str) -> dict[str, Any]:
    return {
        "healthy": True,
        "correlationId": correlation_id,
        "specVersion": w.SPEC_VERSION_CURRENT,
        "hookResumeInputVersion": w.HOOK_RESUME_INPUT_VERSION,
        "timestamp": int(datetime.now(UTC).timestamp() * 1000),
    }


async def _answer_health_check(world: w.World, health: w.HealthCheckPayload) -> None:
    run_id = _health_check_run_id(health.correlation_id)
    name = _health_check_stream_name(health.correlation_id)
    body = json.dumps(_health_check_response(health.correlation_id)).encode()
    try:
        await world.streams_write(run_id, name, body)
        await world.streams_close(run_id, name)
    except NotImplementedError:
        logger.warning(
            "Health check %r cannot be answered: %s has no streams",
            health.correlation_id,
            type(world).__name__,
        )


def refuse_cross_environment_delivery(
    world: w.World, run_input: w.RunInput | None, run_id: str
) -> bool:
    """runtime.ts, refuseCrossEnvironmentDelivery.

    Resiliently creating the run would put a second copy of the same run id in
    this environment, so this has to be caught before ``run_started`` — the
    write that would fork it. Skipped when either side is unknown.
    """
    creator = run_input.environment if run_input is not None else None
    if not creator:
        return False
    current = world.get_environment()
    if not current or current == creator:
        return False

    logger.error(
        "[Workflows] '%s' - refusing to run this workflow: it was created in the "
        '"%s" environment but this deployment runs in "%s". Executing it here '
        "would create a second copy of the same run id in both environments — "
        "one pending forever, one running — so the queue message is being "
        "discarded without executing and without retrying. The client that "
        "called start() wrote the run to its own environment but addressed the "
        "queue message to a deployment in another one. Check that the "
        "environment that client authenticates as matches the environment of "
        'the deployment it targets. The run it created is still pending in "%s" '
        "and will not run.",
        run_id,
        creator,
        current,
        creator,
    )
    return True


async def workflow_handler(
    message: Any,
    *,
    attempt: int,
    queue_name: str,
    message_id: str,
    registry: core.Workflows,
    namespace: str | None = None,
) -> w.QueueContinuation | None:
    world = w.get_world()

    # Before the invoke payload is parsed, matching `workflowEntrypoint` in
    # `@workflow/core`: a probe carries no `runId`, so parsing it as an invoke
    # would raise, leave the message unacked, and redeliver it forever.
    # Deliberately unauthenticated -- answering discloses only that the
    # endpoint is reachable, onto a stream named after the caller's own id.
    health = _parse_health_check(message)
    if health is not None:
        await _answer_health_check(world, health)
        return None

    req = w.WorkflowInvokePayload.model_validate(message)
    if req.step_id is not None:
        return await _execute_step(req, queue_name=queue_name, registry=registry)

    run_id = req.run_id
    if refuse_cross_environment_delivery(world, req.run_input, run_id):
        return None

    # Write `run_started` rather than read the run first: it transitions the run
    # *and* hands back the entity, and on a run whose `run_created` has not
    # landed it is the write that creates it. Reading first turned that legal
    # state into a permanent failure, since the queue does not redeliver a 404.
    # Relies on the world contract that this event is idempotent for a run
    # already running.
    run_input = req.run_input
    started = w.RunStartedEvent(
        eventData=(
            w.RunStartedEventData.from_run_input(run_input) if run_input is not None else None
        ),
        # The creating client's version, so a run created here is not relabelled.
        specVersion=(run_input.spec_version if run_input is not None else w.SPEC_VERSION_CURRENT),
    )
    try:
        result = await world.events_create(run_id, started)
    except (w.EntityConflictError, w.RunExpiredError):
        # Concurrently completed, failed or cancelled — either during setup or
        # before we got here. Nothing to do and nothing to retry.
        logger.debug(f"Workflow run {run_id} is already finished or started")
        return None
    assert result.run is not None
    workflow_run = result.run

    if workflow_run.status == "cancelled":
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
    loaded = await get_all_workflow_run_events(run_id)
    events = loaded.events
    events_cursor = loaded.cursor

    # This message carries a hook payload whose event may not exist yet, so write
    # it before replaying.
    if req.hook_input is not None and not _has_resume_event(events, req.hook_input):
        if not await _ensure_hook_received(world, workflow_run, req.hook_input):
            return None
        # Load the log again rather than appending the new event to what we have:
        # where it belongs in the order is the world's decision, not ours, and
        # this path is rare enough that one extra read is cheaper than getting
        # that wrong.
        loaded = await get_all_workflow_run_events(run_id)
        events = loaded.events
        events_cursor = loaded.cursor
        if _has_terminal_run_event(events, run_id):
            return None

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
        try:
            await world.events_create(
                run_id, w.WaitCompletedEvent(correlationId=wait_event.correlation_id)
            )
        except w.EntityConflictError:
            # Another concurrent invocation already completed this wait
            logger.debug(f"Wait {wait_event.correlation_id!r} is already completed")
            continue

    if waits_to_complete:
        # Reload events after wait completions. Try incremental load first
        # (using cursor), fall back to full reload if the incremental result
        # doesn't contain all the wait_completed events we just created.
        if events_cursor:
            delta = await get_all_workflow_run_events(run_id, after_cursor=events_cursor)
            delta_completed_ids = {
                e.correlation_id for e in delta.events if e.event_type == "wait_completed"
            }
            saw_all = all(w_.correlation_id in delta_completed_ids for w_ in waits_to_complete)
            if saw_all:
                # Merge delta into existing events, deduplicating by eventId
                existing_ids = {e.server_props.event_id for e in events if e.server_props}
                for event in delta.events:
                    eid = event.server_props.event_id if event.server_props else None
                    if eid not in existing_ids:
                        if eid:
                            existing_ids.add(eid)
                        events.append(event)
                events_cursor = delta.cursor or events_cursor
            else:
                loaded = await get_all_workflow_run_events(run_id)
                events = loaded.events
                events_cursor = loaded.cursor
        else:
            loaded = await get_all_workflow_run_events(run_id)
            events = loaded.events
            events_cursor = loaded.cursor

        # A concurrent handler may have written a terminal run event after
        # the initial snapshot. If so, this delivery is done.
        if _has_terminal_run_event(events, run_id):
            return None

    context = WorkflowOrchestratorContext(
        events,
        run_id=run_id,
        seed=run_id,
        started_at=workflow_started_at,
        registry=registry,
        run_key=await _resolve_run_key(world, workflow_run, events),
    )
    try:
        output = context.run_workflow(workflow_run)
    except _SuspendException:
        # Workflow suspended, continue outside the try..except block.
        pass
    except Exception as e:
        error_message = "".join(traceback.format_exception_only(type(e), e)).strip()
        logger.exception("[Workflows] '%s' - workflow run failed: %s", run_id, error_message)
        try:
            await world.events_create(
                run_id,
                w.RunFailedEventData(
                    error={"message": error_message, "stack": traceback.format_exc()},
                    code=type(e).__name__,
                ).into_event(),
            )
        except w.EntityConflictError:
            logger.warning(f"Workflow run {run_id} was already completed")
        return None
    else:
        try:
            await world.events_create(
                run_id,
                w.RunCompletedEventData(output=output).into_event(),
            )
        except w.EntityConflictError:
            logger.warning(f"Workflow run {run_id} was already completed")
        return None

    # Now that the workflow is fully suspended, we can create all pending events in parallel
    events_created = False
    async with anyio.create_task_group() as tg:
        for sus in context.suspensions.values():
            if sus.has_created_event:
                pass

            elif isinstance(sus, Suspension):

                async def create_step(s=sus):
                    step_data = w.StepCreatedEventData(stepName=s.step.name, input=s.input)
                    try:
                        await world.events_create(run_id, step_data.into_event(s.correlation_id))
                    except w.EntityConflictError:
                        logger.debug(f"Workflow step {s.correlation_id!r} has already been created")
                    # We enqueue the invoke whether or not the
                    # events_create had an EntityConflictError, since
                    # a previous invoker may have crashed between
                    # creating the event and enqueueing.
                    #
                    # Instead we use an idempotency_key to pervent
                    # duplicate queueing.
                    await world.queue(
                        w.get_queue_name(workflow_run.workflow_name, namespace),
                        w.WorkflowInvokePayload(
                            runId=run_id,
                            stepId=s.correlation_id,
                            stepName=s.step.name,
                            requestedAt=datetime.now(UTC),
                        ),
                        idempotency_key=s.correlation_id,
                    )

                tg.start_soon(create_step)
                events_created = True

            elif isinstance(sus, Wait):

                async def create_wait(s=sus):
                    wait_data = w.WaitCreatedEventData(resumeAt=s.resume_at)
                    try:
                        await world.events_create(run_id, wait_data.into_event(s.correlation_id))
                    except w.EntityConflictError:
                        logger.debug(f"Workflow wait {s.correlation_id!r} has already been created")

                tg.start_soon(create_wait)
                events_created = True

            elif isinstance(sus, Hook):

                async def create_hook(s=sus):
                    hook_data = w.HookCreatedEventData(token=s.token, metadata=s.metadata)
                    try:
                        await world.events_create(run_id, hook_data.into_event(s.correlation_id))
                    except w.EntityConflictError:
                        logger.debug(f"Workflow hook {s.correlation_id!r} has already been created")

                tg.start_soon(create_hook)
                events_created = True

        for hook in context.hooks.values():
            if hook.disposed and not hook.has_dispose_event:

                async def dispose_hook(h=hook):
                    try:
                        await world.events_create(
                            run_id,
                            w.HookDisposedEvent(correlationId=h.correlation_id),
                        )
                    except (w.EntityConflictError, w.HookNotFoundError):
                        logger.debug(
                            f"Workflow hook {h.correlation_id!r} has already been disposed"
                        )

                tg.start_soon(dispose_hook)
                events_created = True

    if not context.suspensions and events_created:
        # We captured a _SuspendException but there is no suspension - this is likely caused
        # by a disposed hook that cleared its suspensions. Just retry if event log changed.
        return await workflow_handler(
            message,
            attempt=attempt,
            queue_name=queue_name,
            message_id=message_id,
            registry=registry,
            namespace=namespace,
        )

    now = datetime.now(UTC)
    min_timeout_seconds = -1.0
    soonest_wait_id: str | None = None
    for sus in context.suspensions.values():
        if isinstance(sus, Wait):
            seconds = (sus.resume_at - now).total_seconds()
            if min_timeout_seconds < 0 or seconds < min_timeout_seconds:
                min_timeout_seconds = seconds
                soonest_wait_id = sus.correlation_id
    if soonest_wait_id is None:
        return None
    # Key the delayed wake-up on the soonest pending wait so repeated suspension
    # passes over it collapse to one timer instead of piling up duplicate wake-ups.
    timeout_seconds = max(1, math.ceil(min_timeout_seconds))
    delay, key = _wait_continuation_dispatch(timeout_seconds, soonest_wait_id, now)
    return w.QueueContinuation(delay_seconds=delay, idempotency_key=key)


async def _execute_step(
    req: w.WorkflowInvokePayload,
    *,
    queue_name: str,
    registry: core.Workflows,
) -> w.QueueContinuation | None:
    world = w.get_world()

    if req.step_id is None:
        raise ValueError(f"Step invocation for run '{req.run_id}' is missing 'stepId'")
    if req.step_name is None:
        raise ValueError(f"Step invocation for '{req.step_id}' is missing 'stepName'")
    step = registry._get_step(req.step_name)

    # step_started validates state (terminal -> EntityConflictError, retryAfter
    # not reached -> TooEarlyError), increments the attempt, and returns the
    # updated step entity, so no step pre-read is needed.
    try:
        start_result = await world.events_create(
            req.run_id,
            w.StepStartedEvent(correlationId=req.step_id),
        )
    except w.TooEarlyError as e:
        # retryAfter not reached yet — keep the message visible and retry later.
        timeout_seconds = max(1, e.retry_after or 1)
        logger.debug(
            "[Workflows] '%s' - step '%s' retryAfter not reached, deferring %ds",
            req.run_id,
            step.name,
            timeout_seconds,
        )
        return w.QueueContinuation(delay_seconds=timeout_seconds)
    except w.EntityConflictError:
        # Step already in a terminal state — a duplicate delivery, or a concurrent
        # worker finished it. Re-enqueue the workflow so it observes the outcome,
        # then ack. This is also the crash-recovery path for a step that finished
        # but whose handler died before re-invoking the workflow.
        logger.debug("Tried starting step %r, but it has already finished", req.step_id)
        await world.queue(
            queue_name,
            w.WorkflowInvokePayload(
                runId=req.run_id,
                requestedAt=datetime.now(UTC),
            ),
        )
        return None

    # Use the step entity from the event response
    if not start_result.step:
        raise RuntimeError(f"step_started event for '{req.step_id}' did not return step entity")
    step_run = start_result.step
    current_attempt = step_run.attempt

    # Check max retries AFTER step_started (the attempt was just incremented).
    # Use > here (not >=) because this guards re-invocation AFTER all attempts are used.
    if step_run.attempt > step.max_retries + 1:
        retry_count = step_run.attempt - 1
        error_message = (
            f"Step '{step.name}' exceeded max retries "
            f"({retry_count} {'retry' if retry_count == 1 else 'retries'})"
        )
        logger.error("[Workflows] '%s' - %s", req.run_id, error_message)

        # Fail the step via event
        await world.events_create(
            req.run_id,
            w.StepFailedEventData(
                error=error_message, stack=step_run.error.stack if step_run.error else None
            ).into_event(req.step_id),
        )

        # Re-invoke the workflow to handle the failed step
        await world.queue(
            queue_name,
            w.WorkflowInvokePayload(
                runId=req.run_id,
                requestedAt=datetime.now(UTC),
            ),
        )
        return None

    # Bound before the try so the failure path can flush whatever the step
    # managed to write before it raised.
    step_streams = _StepStreams(run_id=req.run_id)

    # Installed around the whole invocation, not just the body: a stream the
    # workflow passed in is revived while the input is hydrated, and it has to
    # register with this step or nothing would ever drain it.
    streams_token = _step_streams_ctx.set(step_streams)

    try:
        async with step_streams.dispatching():
            if not step_run.started_at:
                raise RuntimeError(f"Step '{req.step_id}' has no 'startedAt' timestamp")

            # Deserialize step input
            if not step_run.input:
                raise RuntimeError(f"Step '{req.step_id}' has no input")
            what = f"the input of step {req.step_id}"
            # No deployment id: the queue message routed this step to the deployment
            # that owns the run, so the key resolves against the current one.
            run_key = await world.run_key(req.run_id) if ser.is_encrypted(step_run.input) else None
            args, kwargs = ser.step_call_arguments(
                ser.hydrate(step_run.input, what=what, key=run_key), what=what
            )

            logger.debug(
                "[Workflows] '%s' - invoking step '%s' (step_id=%s, attempt=%d)",
                req.run_id,
                step.name,
                req.step_id,
                current_attempt,
            )
            token = _step_ctx.set(
                StepInfo(
                    run_id=req.run_id,
                    step_id=req.step_id,
                    step_name=step.name,
                    attempt=current_attempt,
                )
            )
            # Execute the step function
            try:
                result = await step.func(*args, **kwargs)
            finally:
                _step_ctx.reset(token)

            # A stream write returns as soon as it is buffered, so the chunks
            # this step wrote are not durable yet. Force them out before
            # recording the step as complete, so "the step finished" keeps
            # implying "everything it streamed is readable" -- and so a failure
            # to write fails the step rather than being discovered by a reader
            # that never sees the chunk.
            #
            # Stricter than `@workflow/core`, deliberately. Its step executor
            # caps the same flush at 500ms and completes the step regardless,
            # handing the rest to `waitUntil`, because its `ops` include
            # lock-release polling on a `WritableStream` the user may hold open
            # across steps -- that can never settle, so it cannot be awaited
            # unbounded. Nothing here waits on a lock: `drain()` waits only for
            # chunks already handed over, so it always terminates and needs no
            # escape hatch.
            await step_streams.drain()

        # Serialize the result
        output = ser.dehydrate(result)

        # Complete the step via event
        await world.events_create(
            req.run_id,
            w.StepCompletedEventData(result=output).into_event(req.step_id),
        )

    except Exception as e:
        # step.attempt was incremented by step_started
        current_attempt = step_run.attempt
        error_text = "".join(traceback.format_exception_only(type(e), e)).strip()

        fatal = isinstance(e, errors.FatalError)
        if fatal or current_attempt >= step.max_retries + 1:
            if fatal:
                error_message = f"Step '{step.name}' failed: {error_text}"
                logger.exception(
                    "[Workflows] '%s' - Encountered Error "
                    "while executing step '%s' (attempt %d): %s"
                    "\n\n  Error is fatal\n  Bubbling error to parent workflow",
                    req.run_id,
                    step.name,
                    step_run.attempt,
                    e,
                )
            else:
                retry_count = step_run.attempt - 1
                error_message = (
                    f"Step '{step.name}' failed after {step.max_retries} "
                    f"{'retry' if step.max_retries == 1 else 'retries'}: {error_text}"
                )
                logger.exception(
                    "[Workflows] '%s' - Encountered Error "
                    "while executing step '%s' (attempt %d, "
                    "%d %s): %s\n\n  Max retries reached\n  Bubbling error to parent workflow",
                    req.run_id,
                    step.name,
                    step_run.attempt,
                    retry_count,
                    "retry" if retry_count == 1 else "retries",
                    e,
                )

            # Fail the step via event
            error_stack = traceback.format_exc()
            await world.events_create(
                req.run_id,
                w.StepFailedEventData(error=error_message, stack=error_stack).into_event(
                    req.step_id
                ),
            )
        else:
            # Not at max retries yet - retry the step
            logger.warning(
                "[Workflows] '%s' - Encountered Error "
                "while executing step '%s' (attempt %d): "
                "%s\n\n  This step has failed but will be retried",
                req.run_id,
                step.name,
                current_attempt,
                e,
            )

            # Set step to pending for retry
            error_stack = traceback.format_exc()
            await world.events_create(
                req.run_id,
                w.StepRetryingEventData(error=error_text, stack=error_stack).into_event(
                    req.step_id
                ),
            )

            # Return timeout to keep message visible for retry
            return w.QueueContinuation(delay_seconds=1.0)

    finally:
        _step_streams_ctx.reset(streams_token)

    # Re-invoke the workflow to continue execution
    await world.queue(
        queue_name,
        w.WorkflowInvokePayload(
            runId=req.run_id,
            requestedAt=datetime.now(UTC),
        ),
    )
    return None


ENDPOINT_PATH = "/.well-known/workflow/v1/flow"
"""The common route `workflow_entrypoint`'s handler belongs on.

The tools do not discover this path, they hard-code it: `workflow health`
prechecks it and dev-server port discovery probes it. An app serving the
workflow_handler will use this path in the future.
"""

_HEALTH_CHECK_QUERY_PARAM = "__health"

_HEALTH_CHECK_CORS_HEADERS = {
    # So the observability UI can check an endpoint from another origin.
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "POST, OPTIONS, GET, HEAD",
    "access-control-allow-headers": "Content-Type",
}


def _with_health_check(handler: w.HTTPHandler) -> w.HTTPHandler:
    """Add the HTTP health probe to a flow route handler.

    Mirrors `withHealthCheck` in `@workflow/core`, which wraps the flow route
    the same way. This is the other probe transport, and it shares the flow
    route's URL: a `__health` query parameter is the whole difference between
    "is this endpoint alive" and a queue delivery, so the branch belongs inside
    the handler rather than beside it -- one route, mounted once.

    Both of its callers are local-development paths: the CLI's reachability
    precheck before it runs the queue probe (which POSTs) and dev-server port
    discovery (which sends HEAD, and reads only the status).
    """

    async def with_health_check(request: w.HTTPRequest) -> w.HTTPResponse:
        target = urlsplit(request.url)
        # `keep_blank_values`, because the probe is `?__health` with no value at
        # all -- the default parse drops it and every probe reads as a delivery.
        query = parse_qsl(target.query, keep_blank_values=True)
        if not any(name == _HEALTH_CHECK_QUERY_PARAM for name, _ in query):
            return await handler(request)
        if request.method == "OPTIONS":
            return w.HTTPResponse(204, b"", dict(_HEALTH_CHECK_CORS_HEADERS))
        # Same omissions as `_health_check_response`, and `endpoint` in place of
        # the correlation id: taken from the request, as `url.pathname` is
        # upstream, so an app serving the route elsewhere reports where it
        # actually answered.
        response = w.HTTPResponse.json(
            {
                "healthy": True,
                "endpoint": target.path,
                "specVersion": w.SPEC_VERSION_CURRENT,
            }
        )
        response.headers.update(_HEALTH_CHECK_CORS_HEADERS)
        return response

    return with_health_check


def workflow_entrypoint(registry: core.Workflows) -> w.HTTPHandler:
    namespace = registry.namespace
    return _with_health_check(
        w.get_world().create_queue_handler(
            w.get_queue_topic_prefix(namespace),
            functools.partial(workflow_handler, registry=registry, namespace=namespace),
        )
    )


MANIFEST_PATH = "/.well-known/workflow/v1/manifest.json"
MANIFEST_VERSION = "1.0.0"
PUBLIC_MANIFEST_ENV = "WORKFLOW_PUBLIC_MANIFEST"


def _manifest_file(module: str) -> str:
    return f"{module.replace('.', '/')}.py"


def build_manifest(*registries: core.Workflows) -> dict[str, Any]:
    workflows: dict[str, dict[str, Any]] = {}
    steps: dict[str, dict[str, Any]] = {}
    # Several registries when an app namespaces its topics, and the document is
    # the app's rather than any one registry's.
    for registry in registries:
        for workflow in registry._workflows.values():
            by_name = workflows.setdefault(_manifest_file(workflow.module), {})
            by_name[workflow.qualname] = {
                "workflowId": workflow.workflow_id,
                "graph": {"nodes": [], "edges": []},
            }

        for step in registry._steps.values():
            by_name = steps.setdefault(_manifest_file(step.func.__module__), {})
            by_name[step.func.__qualname__] = {"stepId": step.name}

    return {
        "version": MANIFEST_VERSION,
        "steps": steps,
        "workflows": workflows,
        "classes": {},
    }


def manifest_entrypoint(registry: core.Workflows) -> w.HTTPHandler:
    async def handler(request: w.HTTPRequest) -> w.HTTPResponse:
        if os.getenv(PUBLIC_MANIFEST_ENV) != "1":
            return w.HTTPResponse(404, b"", {})
        return w.HTTPResponse.json(build_manifest(registry))

    return handler


class _LoadedEvents:
    __slots__ = ("events", "cursor")

    def __init__(self, events: list[w.Event], cursor: str | None) -> None:
        self.events = events
        self.cursor = cursor


async def get_all_workflow_run_events(
    run_id: str, *, after_cursor: str | None = None
) -> _LoadedEvents:
    all_events: list[w.Event] = []
    cursor: str | None = after_cursor
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
    return _LoadedEvents(all_events, cursor)


def _has_terminal_run_event(events: list[w.Event], run_id: str) -> bool:
    """Check if the event log contains a terminal run event (completed/failed/cancelled)."""
    return any(
        e.server_props is not None
        and e.server_props.run_id == run_id
        and e.event_type in ("run_completed", "run_failed", "run_cancelled")
        for e in events
    )


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
                key = None
                if ser.is_encrypted(run.output):
                    key = await self._world.run_key(run.run_id, deployment_id=run.deployment_id)
                return ser.hydrate(run.output, what=f"the output of run {run.run_id}", key=key)

            elif run.status == "cancelled":
                raise RuntimeError("workflow cancelled")

            elif run.status == "failed":
                raise RuntimeError("workflow failed")

            else:
                await asyncio.sleep(1)

    def readable(
        self, *, namespace: str | None = None, start_index: int | None = None
    ) -> AsyncGenerator[Any, None]:
        """Read what the run's steps stream, as they stream it.

        Yields one value per :meth:`~vercel.workflow.WorkflowStreamWriter.write`,
        in write order, and ends when a step closes the stream. A run that never
        closes its stream leaves this waiting until the run expires.

        *start_index* skips that many chunks; a negative value reads that many
        back from the end. Positive values resume exactly, which is what makes
        this usable behind a reconnecting client -- hand back the index you last
        saw and pass it in next time. A negative one cannot: it resolves against
        wherever the tail was at connect time, so the read is single-shot and
        will not survive a dropped connection.

        A method rather than a property, because each call opens its own read:
        iterating a property twice would quietly start a second one.
        """

        async def values() -> AsyncGenerator[Any, None]:
            name = streams.workflow_run_stream_id(self._run_id, namespace)
            frames = streams.reconnecting_frames(self._world, self._run_id, name, start_index)
            async with contextlib.aclosing(frames):
                index = start_index or 0
                async for payload in frames:
                    yield ser.hydrate(payload, what=f"chunk {index} of stream {name}")
                    index += 1

        return values()

    def readable_bytes(
        self, *, namespace: str | None = None, start_index: int | None = None
    ) -> AsyncGenerator[bytes, None]:
        """:meth:`readable`, for a stream of nothing but ``bytes``.

        The shape an HTTP body wants, so a route can hand this straight to a
        streaming response. A chunk that is not ``bytes`` is an error here
        rather than something for the response layer to trip over.
        """

        async def data() -> AsyncGenerator[bytes, None]:
            source = self.readable(namespace=namespace, start_index=start_index)
            async with contextlib.aclosing(source):
                async for value in source:
                    if not isinstance(value, bytes | bytearray):
                        raise ser.SerializationError(
                            f"Stream chunk is {type(value).__name__}, not bytes; use "
                            f"readable() for a stream of values"
                        )
                    yield bytes(value)

        return data()

    async def stream_info(self, *, namespace: str | None = None) -> w.StreamInfo:
        """The stream's last chunk index and whether it has been closed.

        ``tail_index`` is ``-1`` when nothing has been written yet, so a caller
        deriving a start index from it has to handle that rather than treat it
        as a position.
        """
        name = streams.workflow_run_stream_id(self._run_id, namespace)
        return await self._world.streams_get_info(self._run_id, name)

    async def list_streams(self) -> list[str]:
        """Every stream this run has written to, namespaced ones included."""
        return await self._world.streams_list(self._run_id)


def read_stream(
    run_id: str, name: str, *, start_index: int | None = None
) -> AsyncGenerator[Any, None]:
    """Read one of a run's streams by its full name.

    :meth:`Run.readable` derives the name from the run id and a namespace, so
    it only reaches streams that follow that scheme. This takes the name
    verbatim, which is what :meth:`Run.list_streams` returns and what another
    SDK may have used.
    """

    async def values() -> AsyncGenerator[Any, None]:
        world = w.get_world()
        frames = streams.reconnecting_frames(world, run_id, name, start_index)
        async with contextlib.aclosing(frames):
            index = start_index or 0
            async for payload in frames:
                yield ser.hydrate(payload, what=f"chunk {index} of stream {name}")
                index += 1

    return values()


async def start(wf: core.Workflow[P, T], *args: P.args, **kwargs: P.kwargs) -> Run:
    # Bound before anything is written, so an arity mistake raises here instead
    # of leaving a run behind that fails when its body is invoked.
    bound_args, bound_kwargs = wf.bind_arguments(args, kwargs)
    world = w.get_world()
    deployment_id = await world.get_deployment_id()
    namespace = wf._resolve_queue_namespace()
    input_data = ser.dehydrate(ser.argument_array(bound_args, bound_kwargs))
    execution_context: dict[str, Any] = {"hookResumeInputVersion": w.HOOK_RESUME_INPUT_VERSION}
    if namespace is not None:
        execution_context["queueNamespace"] = namespace
    data = w.RunCreatedEventData(
        deploymentId=deployment_id,
        workflowName=wf.workflow_id,
        input=input_data,
        executionContext=execution_context,
    )
    result = await world.events_create(None, data.into_event())

    # Assert that the run was created
    if not result.run:
        raise RuntimeError("Missing 'run' in server response for 'run_created' event")

    run_id = result.run.run_id
    await world.queue(
        w.get_queue_name(wf.workflow_id, namespace),
        w.WorkflowInvokePayload(runId=run_id),
        deployment_id=deployment_id,
    )

    return Run(run_id)


async def _public_hook(
    world: w.World, hook: w.Hook, *, run: w.WorkflowRun | None = None
) -> core.Hook:
    metadata = hook.metadata
    if metadata is not None:
        key = None
        if ser.is_encrypted(metadata):
            if run is None:
                run = await world.runs_get(hook.run_id)
            key = await world.run_key(run.run_id, deployment_id=run.deployment_id)
        metadata = ser.hydrate(metadata, what=f"the metadata of hook {hook.hook_id}", key=key)
    return core.Hook(
        token=hook.token,
        hook_id=hook.hook_id,
        run_id=hook.run_id,
        created_at=hook.created_at,
        metadata=metadata,
    )


async def get_hook_by_token(token: str) -> core.Hook:
    world = w.get_world()
    return await _public_hook(world, await world.hooks_get_by_token(token))


async def resume_hook(token_or_hook: str | core.Hook, payload: Any) -> core.Hook:
    world = w.get_world()
    if isinstance(token_or_hook, str):
        entity = await world.hooks_get_by_token(token_or_hook)
        run = await world.runs_get(entity.run_id)
        hook = await _public_hook(world, entity, run=run)
    else:
        hook = token_or_hook
        run = await world.runs_get(hook.run_id)
    data = w.HookReceivedEventData(payload=ser.dehydrate(payload))
    await world.events_create(hook.run_id, data.into_event(hook.hook_id))
    execution_context = run.execution_context or {}
    namespace = execution_context.get("queueNamespace")
    if namespace is not None and not isinstance(namespace, str):
        raise RuntimeError("Workflow run has an invalid queue namespace")
    await world.queue(
        w.get_queue_name(run.workflow_name, namespace),
        w.WorkflowInvokePayload(runId=hook.run_id),
    )
    return hook
