import abc
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
import sys
import traceback
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine, Mapping, Sequence
from datetime import datetime
from typing import Any, Generic, Literal, ParamSpec, TypeVar, cast
from urllib.parse import parse_qsl, urlsplit

import anyio
import pydantic

from vercel._internal.core.polyfills import UTC, Self

from . import (
    attributes as attrs,
    core,
    errors,
    loop,
    nanoid,
    serialization as ser,
    signature_codec,
    streams,
    ulid,
    world as w,
)
from .duration import DurationParam, parse_duration_to_date

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


class NondeterminismError(Exception):
    """Raised when a workflow's replay diverges from its recorded event log.

    Correlation IDs are assigned positionally (the Nth step/sleep/hook of a body
    run gets the Nth seeded ID), so if the body issues operations in a different
    order or with different arguments on replay, recorded results would be
    matched onto the wrong calls. This is raised instead, failing the run.
    """


_WORLD_ERROR_CODES = {"PARSE_ERROR", "SCHEMA_VALIDATION", "WORLD_CONTRACT_ERROR"}
_RETRYABLE_WORLD_ERROR_CODES = {"TRANSPORT", "TIMEOUT"}


def classify_run_error(error: Exception) -> str:
    """Return the plaintext ``errorCode`` stored with a failed run."""
    if isinstance(error, NondeterminismError):
        return "REPLAY_DIVERGENCE"
    if isinstance(error, w.WorkflowWorldError):
        if (
            isinstance(error, w.ThrottleError)
            or error.code in _WORLD_ERROR_CODES | _RETRYABLE_WORLD_ERROR_CODES
            or error.status == 0
            or error.status == 429
            or error.status >= 500
            # A world error on a successful response means its body violated
            # the response schema.
            or 200 <= error.status < 300
        ):
            return "WORLD_CONTRACT_ERROR"
    return "USER_ERROR"


@dataclasses.dataclass(kw_only=True)
class BaseSuspension(abc.ABC):
    correlation_id: str
    has_created_event: bool = False

    @abc.abstractmethod
    def fail(self, exc: Exception) -> None:
        """Resume this suspension by raising ``exc`` in its awaiter."""


@dataclasses.dataclass(kw_only=True)
class FutureSuspension(BaseSuspension, Generic[T]):
    future: asyncio.Future[T] = dataclasses.field(default_factory=asyncio.Future)

    def fail(self, exc: Exception) -> None:
        if not self.future.done():
            self.future.set_exception(exc)


@dataclasses.dataclass(kw_only=True)
class Suspension(FutureSuspension[T], Generic[T]):
    step: core.Step[Any, T]
    input: bytes


@dataclasses.dataclass(kw_only=True)
class Cancellation(BaseSuspension):
    """The pending cancellation of a cancellable step.

    Registered in the run's suspensions when the body first cancels the step;
    removed when the recorded ``hook_received`` replays, so a still-registered
    one at flush time is exactly a cancellation left to send.
    """

    token: str
    step_id: str
    reason: str | None = None
    # Has a cancellation been requested ever? Prevents repeat cancellations
    # from being sent.
    requested: bool = False

    def fail(self, exc: Exception) -> None:
        # Cancellation suspensions have no awaiter.
        pass


class CallbackCancelFuture(asyncio.Future[T]):
    """A future whose ``cancel()`` defers to a callback.

    The callback is called on a cancel(); if it returns True, we
    perform a normal future cancel, otherwise we suppress it.

    The callback ought to make some progress towards really cancelling
    the future. (This might seem dodgy, but we already have Task as
    a precedent of a Future that overrides cancel() in a way that
    does not immediately cancel.)
    """

    def __init__(self, *, on_cancel: Callable[[str | None], bool]) -> None:
        super().__init__()
        self._on_cancel = on_cancel

    def cancel(self, msg: str | None = None) -> bool:
        if self.done():
            return False
        if self._on_cancel(msg):
            return super().cancel(msg)
        return True

    def deliver_cancellation(self, msg: str | None = None) -> bool:
        """Really cancel, bypassing the callback: for delivering a step's
        recorded outcome when that outcome *is* the cancellation."""
        return super().cancel(msg)


@dataclasses.dataclass(kw_only=True)
class Wait(FutureSuspension[None]):
    resume_at: datetime


@dataclasses.dataclass(kw_only=True)
class Attributes(FutureSuspension[None]):
    changes: list[w.AttributeChange]
    allow_reserved: bool = False


@dataclasses.dataclass(kw_only=True)
class Hook(BaseSuspension, Generic[T]):
    token: str
    disposed: bool = False
    has_dispose_event: bool = False
    has_conflict_awaiter: bool = False
    futures: deque[asyncio.Future[T]] = dataclasses.field(default_factory=deque)
    conflict_futures: deque[asyncio.Future["Run[Any] | None"]] = dataclasses.field(
        default_factory=deque
    )
    conflict_error: errors.HookConflictError | None = None
    conflicting_run: "Run[Any] | None" = None
    hook_cls: type[T]
    metadata: bytes | None = None

    def fail(self, exc: Exception) -> None:
        while self.futures:
            future = self.futures.popleft()
            if not future.done():
                future.set_exception(exc)
        while self.conflict_futures:
            conflict_future = self.conflict_futures.popleft()
            if not conflict_future.done():
                conflict_future.set_exception(exc)

    def set_result(self, raw_data: Any) -> None:
        res: T
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


def _abort_stream_name(step_correlation_id: str) -> str:
    """The control stream a cancellable step listens on for its cancellation.

    The ``strm_<id>_system_abort`` shape matches ``@workflow/core``'s abort
    streams.
    """
    return f"strm_{_correlation_ulid(step_correlation_id)}_system_abort"


@dataclasses.dataclass(frozen=True)
class StepInfo:
    """Metadata about the step currently executing.

    Mirrors the JS SDK's ``getStepMetadata()`` return value. ``step_id`` is stable
    across retries and unique per logical step call, which makes it a good
    idempotency key for non-idempotent side effects (payments, emails, queue
    sends) performed inside a step body.

    ``step_started_at`` is when the *first* attempt began, not this one. The
    World keeps the timestamp the first ``step_started`` event set and does not
    move it on a retry, so a body can measure how long the step has been going
    in total, including the waits between attempts.
    """

    run_id: str
    step_id: str
    step_name: str
    attempt: int
    step_started_at: datetime


@dataclasses.dataclass(frozen=True)
class WorkflowFeatures:
    """Capabilities active for the current workflow run."""

    encryption: bool
    """Whether step inputs, outputs and other serialized data are encrypted
    at rest for this run."""


@dataclasses.dataclass(frozen=True)
class WorkflowInfo:
    """Metadata about the current workflow run.

    Mirrors the JS SDK's ``getWorkflowMetadata()`` return value, so a body or
    step comparing values across the two SDKs sees the same fields. ``run_id``
    keeps the name :class:`StepInfo` already uses for the same value (JS calls
    it ``workflowRunId``).
    """

    run_id: str
    workflow_name: str
    started_at: datetime | None
    """When the workflow run started.

    ``None`` inside a step.
    """
    url: str
    """The base URL of the deployment serving this run."""
    features: WorkflowFeatures


@dataclasses.dataclass(frozen=True)
class _StepState:
    """The metadata of the step invocation currently executing.

    Workflows are not covered: a body reads its metadata off the orchestrator
    context instead.
    """

    step_info: StepInfo
    workflow_info: WorkflowInfo


_step_state_ctx: contextvars.ContextVar[_StepState] = contextvars.ContextVar("WorkflowStepContext")


def _workflow_url() -> str:
    """The deployment's base URL, matching the JS runtime's derivation."""
    vercel_url = os.environ.get("VERCEL_URL")
    if vercel_url:
        return f"https://{vercel_url}"
    return f"http://localhost:{os.environ.get('PORT', '3000')}"


def _workflow_name_from_queue(queue_name: str) -> str:
    """The workflow name a queue name routes to.

    The queue name identifies the workflow (see ``get_queue_name``): the
    prefix is ``__wkf_workflow_`` or ``__{namespace}_wkf_workflow_``, so
    splitting on the invariant part strips either form — the same derivation
    the JS runtime uses for its step executions.
    """
    return queue_name.split("_wkf_workflow_", 1)[-1]


def get_workflow_metadata() -> WorkflowInfo:
    """Return metadata for the workflow run currently executing.

    Mirrors the JS SDK's ``getWorkflowMetadata()``: callable from a workflow
    body or from a step body, returning the same values in both places.
    Raises ``RuntimeError`` when called outside either.
    """
    try:
        return _step_state_ctx.get().workflow_info
    except LookupError:
        pass
    try:
        info = WorkflowOrchestratorContext.current().workflow_info
    except LookupError:
        info = None
    if info is None:
        raise RuntimeError("get_workflow_metadata() can only be called inside a workflow or a step")
    return info


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
        return _step_state_ctx.get().step_info
    except LookupError:
        raise RuntimeError("get_step_metadata() can only be called inside a step") from None


async def set_attributes(
    attributes: Mapping[str, str] | None = None,
    /,
    *,
    allow_reserved_attributes: bool = False,
    **kwargs: str,
) -> None:
    """Attach plaintext key/value metadata to the current run.

    Takes a mapping, keyword arguments, or both, the way ``dict()`` does::

        await set_attributes(phase="charging", tier=tier)
        await set_attributes({"service.name": name})

    Callable from a workflow body or a step body. The pairs land on the run entity,
    where :meth:`Run.attributes` reads them back, in plaintext -- they are never
    encrypted, so must not carry sensitive information. Keys are capped at 256
    characters (UTF-16 code units), values at 256 bytes (UTF-8 encoded), and a run
    at 64 attributes; an invalid call raises :class:`~vercel.workflow.FatalError`
    before anything is written, so a body can catch it. Nothing is written for an
    empty call.

    Keys starting with ``$`` are reserved for framework and library code.
    Use ``allow_reserved_attributes=True`` to override such limit.
    """
    try:
        merged = dict(attributes or {})
    except (TypeError, ValueError):
        # What `dict()` raises for something that is neither a mapping nor an
        # iterable of pairs: ValueError for a str, TypeError for an int.
        raise errors.FatalError(
            f"set_attributes requires a mapping, got {type(attributes).__name__}"
        ) from None
    merged.update(kwargs)
    await _write_attributes(list(merged.items()), allow_reserved=allow_reserved_attributes)


async def remove_attributes(*keys: str, allow_reserved_attributes: bool = False) -> None:
    """Remove keys from the current run's attributes.

    It's no-op if the key doesn't exist. See also :meth:`set_attributes`.
    """
    await _write_attributes([(key, None) for key in keys], allow_reserved=allow_reserved_attributes)


async def _write_attributes(pairs: list[tuple[str, str | None]], *, allow_reserved: bool) -> None:
    if not pairs:
        return
    try:
        attrs.validate_attribute_changes(pairs, allow_reserved=allow_reserved)
    except attrs.AttributeValidationError as e:
        # Raises `FatalError` as JS `@workflow/core` does
        raise errors.FatalError(str(e)) from None
    changes = [w.AttributeChange(key=key, value=value) for key, value in pairs]

    try:
        ctx = WorkflowOrchestratorContext.current()
    except LookupError:
        pass
    else:
        await ctx.set_attributes(changes, allow_reserved=allow_reserved)
        return

    try:
        step = _step_state_ctx.get().step_info
    except LookupError:
        raise errors.FatalError(
            "set_attributes() can only be called inside a workflow or a step"
        ) from None

    # A step runs in host context, so it writes the event itself instead of
    # parking a suspension.
    await w.get_world().events_create(
        step.run_id,
        w.AttrSetEventData(
            changes=changes,
            writer=w.StepAttributeWriter(step_id=step.step_id, attempt=step.attempt),
            allow_reserved_attributes=True if allow_reserved else None,
        ).into_event(),
    )


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
        workflow_info: WorkflowInfo | None = None,
    ):
        self.run_id = run_id
        self.events = events
        # The key is per-run, so it is resolved once and reused for every
        # payload in the replay.
        self.run_key = run_key
        # What get_workflow_metadata() returns inside the body; None only for
        # contexts built without a run entity (tests, tooling).
        self.workflow_info = workflow_info
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

        self.suspended = False
        # An error the run must fail with regardless of what the body does;
        # raised by run_workflow(). See _fail_nondeterminism.
        self.resume_exception: Exception | None = None

    @classmethod
    def current(cls) -> Self:
        cur = cls._ctx.get()
        # If the workflow is suspended, keep cancelling the task.
        # See comment in suspend, below.
        cur.check_suspended()
        return cur

    def check_suspended(self) -> None:
        if self.suspended:
            raise asyncio.CancelledError("workflow execution is suspended")

    def suspend(self) -> None:
        # When a workflow gets suspended, we would ideally just throw
        # away all of the inflight coroutines and never finishing
        # executing them at all.
        #
        # In particular, we don't actually want exception handlers and
        # finally blocks to run, because from the perspective of the
        # workflow programming model, there is no exception.
        #
        # Unfortunately for our case (though /probably/ fortunately in
        # general), Python insists on always closing
        # generator/coroutine objects by throwing a GeneratorExit into
        # them if they haven't already finished.
        #
        # So instead, when we suspend the workflow, we set the
        # suspended flag, and when current() (above) is called and the
        # workflow is suspended, it raises a CancelledError.
        #
        # All user-facing workflow operations go through current(), and
        # so this means that if a workflow attempts to *do* anything
        # while being suspended (dispose() a hook, call a step, etc),
        # it will immediately fail.
        self.suspended = True
        # Close down the tasks
        for task in asyncio.all_tasks(asyncio.get_event_loop()):
            # If the task is blocked on a future that has already
            # finished with an exception, skip cancelling it.
            #
            # The goal here is to give resume_exception a chance to
            # bring down the workflow itself, so that a useful
            # traceback gets attached to it.
            #
            # If it doesn't work, the task will get cancelled the
            # next time through the resume() loop.
            fut: asyncio.Future[object] | None = task._fut_waiter  # type: ignore[attr-defined]
            if fut and fut.done() and not fut.cancelled() and fut.exception():
                continue

            task.cancel()

    def run_workflow(self: Self, workflow_run: w.WorkflowRun) -> bytes | None:
        """Run the body inside the sandbox, returning its result serialized there.

        Returns None if the workflow execution is suspended."""
        wf = self.registry._get_workflow(workflow_run.workflow_name)
        if not workflow_run.input:
            raise RuntimeError(f"Invalid workflow input for run {workflow_run.run_id}")

        with (
            self.registry._get_sandbox() as sandbox,
            sandbox.enter(),
        ):
            # Hold a lock over the import, to avoid weird init races
            with sandbox.import_lock:
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
            # `obj`, not `wf`: the sandbox re-imported the module, so its codec
            # resolves annotations against the sandbox's globals and builds
            # adapters for the classes the body will actually see.
            args, kwargs = obj.codec.validate_arguments(args, kwargs)

            token = self._ctx.set(self)
            try:
                result = ser.dehydrate(
                    obj.codec.dump_return(
                        _run_isolated(
                            obj.func(*args, **kwargs),
                            loop_factory=lambda: loop.WorkflowLoop(workflow=self),
                        )
                    )
                )
            except BaseException as ex:
                if self.resume_exception is not None:
                    # Since resume_exception actually got raised on a
                    # future, hopefully it has picked up a useful
                    # traceback!
                    raise self.resume_exception from None
                # Turn suspended into a None return regardless of what the
                # task actually did with it.
                if self.suspended:
                    return None
                if isinstance(ex, asyncio.CancelledError):
                    raise RuntimeError("workflow was cancelled") from None
                else:
                    raise
            else:
                if self.resume_exception is not None:
                    raise self.resume_exception
                if self.suspended:
                    return None
            finally:
                self._ctx.reset(token)

            return result

    def run_step(
        self, step: core.Step[P, T], *args: P.args, **kwargs: P.kwargs
    ) -> asyncio.Future[T]:
        # Bound to the step's own signature, so the recorded bytes depend on it
        # rather than on how the body spelled the call -- see
        # `core._bind_arguments`, which the determinism check relies on.
        bound_args, bound_kwargs = step.bind_arguments(args, kwargs)
        dumped_args, dumped_kwargs = step.codec.dump_arguments(bound_args, bound_kwargs)
        input_data = ser.dehydrate(ser.step_arguments(dumped_args, dumped_kwargs))
        ulid = self.generate_ulid()
        sus = Suspension(correlation_id=f"step_{ulid}", step=step, input=input_data)
        self.suspensions[sus.correlation_id] = sus
        if step.cancellable:
            cancellation = Cancellation(
                correlation_id=f"hook_{self.generate_ulid()}",
                token=f"abrt_{ulid}",
                step_id=sus.correlation_id,
            )
            sus.future = CallbackCancelFuture(
                on_cancel=functools.partial(self._on_step_cancel, cancellation)
            )
        return sus.future

    def _on_step_cancel(self, cancellation: Cancellation, msg: object) -> bool:
        """Handle cancel() of a cancellable step's future; see CallbackCancelFuture.

        When cancel() is called during normal execution, we install a
        cancellation suspension for the step, which will get picked up
        when the run suspends and turned into a cancellation.

        We suppress the *actual* cancellation of the future, because
        we want anything waiting on the step to still wait until it has
        actually stopped (just like waiting on a task).

        If the loop is suspended, though, we aren't doing side effects
        and we just want to tear things down, so allow a normal cancel.

        """
        if self.suspended:
            return True
        if not cancellation.requested:
            cancellation.requested = True
            cancellation.reason = str(msg) if msg is not None else None
            self.suspensions[cancellation.correlation_id] = cancellation
        return False

    def run_wait(self, param: DurationParam) -> asyncio.Future[None]:
        wait = Wait(
            correlation_id=f"wait_{self.generate_ulid()}",
            resume_at=(parse_duration_to_date(param)),
        )
        self.suspensions[wait.correlation_id] = wait
        return wait.future

    def set_attributes(
        self, changes: list[w.AttributeChange], *, allow_reserved: bool
    ) -> asyncio.Future[None]:
        attr_sus = Attributes(
            correlation_id=f"attr_{self.generate_ulid()}",
            changes=changes,
            allow_reserved=allow_reserved,
        )
        self.suspensions[attr_sus.correlation_id] = attr_sus
        return attr_sus.future

    def now(self) -> datetime:
        if not self.events:
            raise RuntimeError("now() requires at least one event in the run's event log")
        event = self.events[max(self.replay_index - 1, 0)]
        assert event.server_props is not None
        return event.server_props.created_at

    def time(self) -> float:
        delta = self.now() - _EPOCH
        return delta.total_seconds()

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

    def run_hook(self, *, correlation_id: str) -> asyncio.Future[T]:
        hook = self.hooks[correlation_id]
        if hook.disposed:
            raise StopAsyncIteration
        if hook.conflict_error is not None:
            raise hook.conflict_error
        self.suspensions[hook.correlation_id] = hook
        fut = asyncio.Future[T]()
        hook.futures.append(fut)
        return fut

    def run_hook_conflict(self, *, correlation_id: str) -> asyncio.Future["Run[Any] | None"]:
        hook = self.hooks[correlation_id]
        if hook.has_created_event:
            future = asyncio.Future[Run[Any] | None]()
            future.set_result(None)
            return future
        if hook.conflict_error is not None:
            if hook.conflicting_run is not None:
                future = asyncio.Future[Run[Any] | None]()
                future.set_result(hook.conflicting_run)
                return future
            raise hook.conflict_error
        if hook.disposed:
            raise RuntimeError("cannot call get_conflict() on a disposed hook")

        hook.has_conflict_awaiter = True
        self.suspensions[hook.correlation_id] = hook
        future = asyncio.Future[Run[Any] | None]()
        hook.conflict_futures.append(future)
        return future

    def dispose_hook(self, *, correlation_id: str) -> None:
        hook = self.hooks[correlation_id]
        hook.disposed = True
        while hook.futures:
            fut = hook.futures.popleft()
            if not fut.done():
                fut.set_exception(StopAsyncIteration)
        self.suspensions.pop(correlation_id, None)

    def _fail_nondeterminism(self, sus: BaseSuspension, exc: Exception) -> None:
        """Fail the run with a replay-divergence error the body cannot suppress.

        The diverged suspension may not be what the body is currently blocked
        on, so failing its future alone might never surface anywhere -- and a
        body that is awaiting it could catch the error. So the exception is
        also stashed for ``run_workflow`` to raise, and the run is suspended
        so nothing else executes.
        """
        sus.fail(exc)
        self.resume_exception = exc
        self.suspend()

    def resume(self) -> None:
        """Run over the the event log and try to apply an event.

        The idea is that resume() will be run whenever everything else in the
        async event loop has paused.

        The core invariant of how resume() interacts with the workflow
        is that the execution needs to appear identical to the events
        arriving one at a time (because when they originally arrive
        they are one at a time, and it needs to be indistinguishable
        from that.)

        Resolves at most one suspension.

        If the event log is exhausted, suspend the workflow.
        """

        # Once suspended, replaying further events could only deliver results
        # onto failed or cancelled futures. Just keep cancelling whatever the
        # body is still running until the loop winds down.
        if self.suspended:
            self.suspend()
            return

        # NOTE: resume() does single-step delivery, so we resolve at most
        # one suspension per invocation of resume().
        #
        # This makes sure that resume() gets interleaved directly one
        # to one with event deliveries, instead of sometimes having
        # multiple deliveries bunched up before a resume(), which could
        # lead to mismatches between a recording trace and a replaying
        # one.
        event: w.Event | None = None
        # Look for any out-of-order hooks that can be applied
        for event in self.ooo_hook_received_events:
            if event.correlation_id in self.suspensions:
                self.ooo_hook_received_events.remove(event)
                break
        else:
            event = None

        if event is None and self.replay_index < len(self.events):
            event = self.events[self.replay_index]
            if event.correlation_id not in self.suspensions:
                match event:
                    # A step's attribute write. It answers no call in
                    # this body, so consume it and move on.
                    case (
                        w.AttrSetEvent(correlation_id=None)
                        | w.AttrSetEvent(
                            event_data=w.AttrSetEventData(writer=w.StepAttributeWriter())
                        )
                    ):
                        self.replay_index += 1
                        return
                    case (
                        w.StepCreatedEvent(correlation_id=str() as slot_id)
                        | w.HookCreatedEvent(correlation_id=str() as slot_id)
                        | w.HookConflictEvent(correlation_id=str() as slot_id)
                        | w.WaitCreatedEvent(correlation_id=str() as slot_id)
                        | w.AttrSetEvent(correlation_id=str() as slot_id)
                    ):
                        # Error if body already registered a different-kind
                        # call at this positional slot (same ULID, different
                        # prefix). A same-kind match would have hit the dict
                        # lookup above, so a ULID collision here is a step/wait/
                        # hook swap -- the body is non-deterministic. Fail loudly
                        # instead of yielding forever (the matching ID will never
                        # appear, so plain `return` would deadlock the run).
                        pos = _correlation_ulid(slot_id)
                        for sus in self.suspensions.values():
                            if _correlation_ulid(sus.correlation_id) == pos:
                                self._fail_nondeterminism(
                                    sus,
                                    NondeterminismError(
                                        f"workflow replay diverged at position {pos}: recorded a "
                                        f"{_correlation_kind(slot_id)!r} call, but the body now "
                                        f"issues a {_correlation_kind(sus.correlation_id)!r} call. "
                                        "The workflow body is non-deterministic."
                                    ),
                                )
                                return
                        raise RuntimeError(
                            f"workflow replay cannot deliver {slot_id!r}: "
                            "the workflow body has not registered its suspension"
                        )
                    # HookReceivedEvent is not created from workflows, it may arrive
                    # at any time out of order. At this momemnt we don't need one,
                    # so we just stash it and continue with the event log.
                    case w.HookReceivedEvent():
                        self.ooo_hook_received_events.append(event)
                        self.replay_index += 1
                        return
            self.replay_index += 1

        # No events to process. Suspend.
        if not event:
            self.suspend()
            return

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
                    self._fail_nondeterminism(
                        sus,
                        NondeterminismError(
                            f"workflow replay diverged at {event.correlation_id}: recorded "
                            f"step {name!r}, but the body now calls {sus.step.name!r} with "
                            "different arguments. The workflow body is non-deterministic."
                        ),
                    )
                    return
                sus.has_created_event = True

            case w.HookCreatedEvent():
                hook = self.suspensions[event.correlation_id]
                hook.has_created_event = True
                if isinstance(hook, Hook):
                    while hook.conflict_futures:
                        future = hook.conflict_futures.popleft()
                        if not future.cancelled():
                            future.set_result(None)
                else:
                    assert isinstance(hook, Cancellation)

            case w.WaitCreatedEvent():
                self.suspensions[event.correlation_id].has_created_event = True

            case w.AttrSetEvent(
                correlation_id=str() as attr_id,
                event_data=w.AttrSetEventData(changes=recorded_changes),
            ):
                attr_sus = self.suspensions.pop(attr_id)
                assert isinstance(attr_sus, Attributes)
                if recorded_changes != attr_sus.changes:
                    self._fail_nondeterminism(
                        attr_sus,
                        NondeterminismError(
                            f"workflow replay diverged at {attr_id}: recorded attributes "
                            f"{recorded_changes!r}, but the body now sets "
                            f"{attr_sus.changes!r}. The workflow body is non-deterministic."
                        ),
                    )
                    return
                if not attr_sus.future.cancelled():
                    attr_sus.future.set_result(None)

            case w.StepCompletedEvent(event_data=w.StepCompletedEventData(result=data)):
                sus = self.suspensions.pop(event.correlation_id)
                assert isinstance(sus, Suspension)
                result = ser.hydrate(
                    data,
                    what=f"the result of step {event.correlation_id}",
                    key=self.run_key,
                )
                if not sus.future.cancelled():
                    try:
                        validated = sus.step.codec.validate_return(result)
                    except signature_codec.TypeValidationError as error:
                        sus.future.set_exception(error)
                    else:
                        sus.future.set_result(validated)

            case w.WaitCompletedEvent():
                wait = self.suspensions.pop(event.correlation_id)
                assert isinstance(wait, Wait)
                if not wait.future.cancelled():
                    wait.future.set_result(None)

            case w.StepFailedEvent(event_data=w.StepFailedEventData(error=data)):
                sus = self.suspensions.pop(event.correlation_id)
                assert isinstance(sus, Suspension)
                what = f"the error of step {event.correlation_id}"
                try:
                    failure = ser.hydrate_error(data, what=what, key=self.run_key)
                except ser.SerializationError as error:
                    failure = errors.FatalError(f"Cannot read {what}: {error}")
                if not sus.future.cancelled():
                    if isinstance(failure, errors.StepCancelledError) and isinstance(
                        sus.future, CallbackCancelFuture
                    ):
                        # Died from the body's own cancellation: deliver it
                        # as one.
                        sus.future.deliver_cancellation(str(failure))
                    else:
                        sus.future.set_exception(failure)

            case w.HookConflictEvent(
                event_data=w.HookConflictEventData(
                    token=token,
                    conflicting_run_id=conflicting_run_id,
                )
            ):
                conflicting_hook = self.suspensions.get(event.correlation_id)
                if conflicting_hook is not None:
                    self.suspensions.pop(event.correlation_id)
                    assert isinstance(conflicting_hook, Hook)
                    conflict_error = errors.HookConflictError(token, conflicting_run_id)
                    conflicting_hook.conflict_error = conflict_error
                    conflicting_hook.conflicting_run = (
                        Run(conflicting_run_id) if conflicting_run_id else None
                    )
                    while conflicting_hook.futures:
                        future = conflicting_hook.futures.popleft()
                        if not future.cancelled():
                            future.set_exception(conflict_error)
                    while conflicting_hook.conflict_futures:
                        future = conflicting_hook.conflict_futures.popleft()
                        if future.cancelled():
                            continue
                        if conflicting_hook.conflicting_run is not None:
                            future.set_result(conflicting_hook.conflicting_run)
                        else:
                            # Older conflict events do not identify the owning run, so preserve the
                            # previous HookConflictError behavior when a Run cannot be constructed.
                            future.set_exception(conflict_error)

            case w.HookReceivedEvent(event_data=w.HookReceivedEventData(payload=data)):
                hook = self.suspensions[event.correlation_id]
                if isinstance(hook, Cancellation):
                    # A step cancellation already recorded: deregister it so
                    # the flush does not send it again.
                    self.suspensions.pop(event.correlation_id)
                    return

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


async def _ensure_hook_received(
    world: w.World, run: w.WorkflowRun, hook_input: w.HookResumeInput
) -> bool:
    """Write the resume's ``hook_received`` event. ``False`` means stop here.

    Safe to call even when the other writer of this resume has already written the
    event, or is writing it right now. The write says which resume it is, and the
    world uses that to keep the two of them to one event.

    One thing to notice: this does not catch ``EntityConflictError``, which every
    other write in this module does catch. A conflict here means the other writer
    has claimed the resume but its event cannot be read yet. That is temporary, so
    letting the error out leaves the message unacked and a later delivery finds the
    event. Catching it would ack a message that may hold the only copy of the
    payload.
    """
    # The producer's bytes, passed along rather than re-encoded, so the payload
    # digests on both writes match.
    #
    # The event is labelled with the run's spec version, not ours: this is
    # another writer's event and its payload is encoded to that version. Same
    # reason `run_started` carries the version of whoever created the run.
    event = w.HookReceivedEvent(
        correlation_id=hook_input.hook_id,
        event_data=w.HookReceivedEventData(payload=hook_input.payload, token=hook_input.token),
        spec_version=run.spec_version or w.SPEC_VERSION_CURRENT,
    )
    event._queue_input = hook_input
    try:
        await world.events_create(run.run_id, event)
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
    except w.HookResumeConflictError:
        # Client bug. Nothing to retry, and nothing wrong with the run --
        # so drop the message rather than fail anything.
        logger.error(
            "Dropping the resume of hook %r on run %r: resume id %r already stands "
            "for a different hook or payload.",
            hook_input.hook_id,
            run.run_id,
            hook_input.resume_id,
            exc_info=True,
        )
        return False
    return True


async def _write_attr_set(world: w.World, run_id: str, sus: Attributes) -> None:
    """Append the `attr_set` event one `set_attributes()` call is waiting on."""
    data = w.AttrSetEventData(
        changes=sus.changes,
        writer=w.WorkflowAttributeWriter(),
        allow_reserved_attributes=True if sus.allow_reserved else None,
    )
    try:
        await world.events_create(run_id, data.into_event(sus.correlation_id))
    except w.EntityConflictError:
        # A concurrent replay wrote this event first; just ignore silently.
        logger.debug(f"Workflow attributes {sus.correlation_id!r} have already been set")


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
        return w.HealthCheckPayload.from_wire(message)
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


class _ReplayImmediately:
    """Sentinel to have workflow_handler rerun _workflow_replay_pass"""


_REPLAY_IMMEDIATELY = _ReplayImmediately()


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

    req = w.WorkflowInvokePayload.from_wire(message)
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
        event_data=(
            w.RunStartedEventData.from_run_input(run_input) if run_input is not None else None
        ),
        # The creating client's version, so a run created here is not relabelled.
        spec_version=(run_input.spec_version if run_input is not None else w.SPEC_VERSION_CURRENT),
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

    while True:
        replay_result = await _workflow_replay_pass(
            req=req,
            workflow_run=workflow_run,
            workflow_started_at=workflow_started_at,
            registry=registry,
            namespace=namespace,
        )
        if not isinstance(replay_result, _ReplayImmediately):
            return replay_result


async def _send_cancellation(world: w.World, run_id: str, cancellation: Cancellation) -> None:
    payload = ser.dehydrate({"aborted": True, "reason": cancellation.reason})

    # Signal the running step first. A failure other than a direct
    # error from streams (because the stream is already closed) is an
    # actual failure, so that we fail and the run will get retried.
    stream_name = _abort_stream_name(cancellation.step_id)
    try:
        await world.streams_write(run_id, stream_name, streams.encode_frame(payload))
        await world.streams_close(run_id, stream_name)
    except w.WorkflowWorldError as error:
        if error.status not in (400, 409):
            raise
        logger.debug(f"Cancellation stream {stream_name!r} rejected the signal: {error}")

    # Create the hook if we haven't yet
    if not cancellation.has_created_event:
        hook_data = w.HookCreatedEventData(token=cancellation.token, is_system=True)
        try:
            await world.events_create(run_id, hook_data.into_event(cancellation.correlation_id))
        except w.EntityConflictError:
            logger.debug(f"Cancellation {cancellation.correlation_id!r} has already been created")

    # Mark receipt of it too
    received = w.HookReceivedEventData(payload=payload, token=cancellation.token)
    try:
        await world.events_create(run_id, received.into_event(cancellation.correlation_id))
    except w.EntityConflictError:
        logger.debug(f"Cancellation {cancellation.correlation_id!r} has already been received")


async def _send_cancellations(context: WorkflowOrchestratorContext) -> None:
    """Record and signal every pending step cancellation.

    A cancellation whose ``hook_received`` is already in the log was
    deregistered during replay; whatever is still registered needs to be made
    durable, whether the run is about to suspend or to finish.
    """
    world = w.get_world()
    pending = [sus for sus in context.suspensions.values() if isinstance(sus, Cancellation)]
    if not pending:
        return
    async with anyio.create_task_group() as tg:
        for cancellation in pending:
            tg.start_soon(_send_cancellation, world, context.run_id, cancellation)


async def _workflow_replay_pass(
    *,
    req: w.WorkflowInvokePayload,
    workflow_run: w.WorkflowRun,
    workflow_started_at: int,
    registry: core.Workflows,
    namespace: str | None,
) -> w.QueueContinuation | _ReplayImmediately | None:
    world = w.get_world()
    run_id = req.run_id

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
                run_id, w.WaitCompletedEvent(correlation_id=wait_event.correlation_id)
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
        workflow_info=WorkflowInfo(
            run_id=run_id,
            workflow_name=workflow_run.workflow_name,
            started_at=workflow_run.started_at,
            url=_workflow_url(),
            features=WorkflowFeatures(encryption=workflow_run.encryption_public_key is not None),
        ),
    )
    try:
        output = context.run_workflow(workflow_run)
    except Exception as e:
        error_message = "".join(traceback.format_exception_only(type(e), e)).strip()
        logger.exception("[Workflows] '%s' - workflow run failed: %s", run_id, error_message)
        await _send_cancellations(context)
        try:
            await world.events_create(
                run_id,
                w.RunFailedEventData(
                    error=ser.dehydrate_error(e),
                    error_code=classify_run_error(e),
                ).into_event(),
            )
        except w.EntityConflictError:
            logger.warning(f"Workflow run {run_id} was already completed")
        return None

    await _send_cancellations(context)

    if output is not None:
        try:
            await world.events_create(
                run_id,
                w.RunCompletedEventData(output=output).into_event(),
            )
        except w.EntityConflictError:
            logger.warning(f"Workflow run {run_id} was already completed")
        return None

    events_created = False
    immediate_replay_reasons: set[str] = set()

    # A hook token is not released until its disposal event is durable. Flush
    # disposals before creating new hooks so a workflow can reuse a token in the
    # same suspension without conflicting with its own previous hook.
    async with anyio.create_task_group() as tg:
        for hook in context.hooks.values():
            if hook.disposed and not hook.has_dispose_event:

                async def dispose_hook(h=hook):
                    try:
                        await world.events_create(
                            run_id,
                            w.HookDisposedEvent(correlation_id=h.correlation_id),
                        )
                    except (w.EntityConflictError, w.HookNotFoundError):
                        logger.debug(
                            f"Workflow hook {h.correlation_id!r} has already been disposed"
                        )

                tg.start_soon(dispose_hook)
                events_created = True

    # Now that the workflow is fully suspended and old hook tokens have been
    # released, create all pending events in parallel.
    async with anyio.create_task_group() as tg:
        for sus in context.suspensions.values():
            if sus.has_created_event:
                pass

            elif isinstance(sus, Suspension):

                async def create_step(s=sus):
                    step_data = w.StepCreatedEventData(step_name=s.step.name, input=s.input)
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
                            run_id=run_id,
                            step_id=s.correlation_id,
                            step_name=s.step.name,
                            requested_at=datetime.now(UTC),
                        ),
                        idempotency_key=s.correlation_id,
                    )

                tg.start_soon(create_step)
                events_created = True

            elif isinstance(sus, Wait):

                async def create_wait(s=sus):
                    wait_data = w.WaitCreatedEventData(resume_at=s.resume_at)
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
                        result = await world.events_create(
                            run_id, hook_data.into_event(s.correlation_id)
                        )
                    except w.EntityConflictError:
                        logger.debug(f"Workflow hook {s.correlation_id!r} has already been created")
                        if s.has_conflict_awaiter:
                            immediate_replay_reasons.add("hook_created")
                    else:
                        if isinstance(result.event, w.HookConflictEvent):
                            immediate_replay_reasons.add("hook_conflict")
                        elif s.has_conflict_awaiter:
                            immediate_replay_reasons.add("hook_created")

                tg.start_soon(create_hook)
                events_created = True

            elif isinstance(sus, Attributes):

                async def set_attr(s=sus):
                    data = w.AttrSetEventData(
                        changes=s.changes,
                        writer=w.WorkflowAttributeWriter(),
                        allow_reserved_attributes=True if s.allow_reserved else None,
                    )
                    try:
                        await world.events_create(run_id, data.into_event(s.correlation_id))
                    except w.EntityConflictError:
                        logger.debug(
                            f"Workflow attributes {s.correlation_id!r} have already been set"
                        )
                    immediate_replay_reasons.add("attr_set")

                tg.start_soon(set_attr)
                events_created = True

    if not context.suspensions and events_created:
        # A disposed hook can clear the last suspension while its lifecycle
        # event is still being flushed. Replay it before acknowledging.
        immediate_replay_reasons.add("suspensions_cleared")

    if immediate_replay_reasons:
        logger.debug(
            "[Workflows] '%s' - replaying after durable local progress: %s",
            run_id,
            ", ".join(sorted(immediate_replay_reasons)),
        )
        return _REPLAY_IMMEDIATELY

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


async def _retries_exhausted(
    message: str, step_run: w.WorkflowStep, *, world: w.World, run_id: str
) -> errors.FatalError:
    """Build the terminal error for a step invoked after its last attempt."""
    failure = errors.FatalError(message)
    if step_run.error is None:
        return failure
    try:
        key = None
        if ser.is_encrypted(step_run.error):
            key = await world.run_key(run_id)
        failure.__cause__ = ser.hydrate_error(
            step_run.error, what=f"the previous error of step {step_run.step_id}", key=key
        )
    except Exception as error:
        logger.debug(
            "[Workflows] '%s' - could not read the previous error of step '%s': %s",
            run_id,
            step_run.step_id,
            error,
        )
    return failure


async def _run_cancellable_step(
    step: core.Step[Any, T],
    args: Sequence[Any],
    kwargs: dict[str, Any],
    *,
    world: w.World,
    run_id: str,
    step_id: str,
) -> T:
    """Run a cancellable step and a listener for cancellations.

    The listener waits on the steps abort stream, and if it is
    signalled, cancels.
    """
    stream_name = _abort_stream_name(step_id)
    cancelled = False
    reason: str | None = None
    step_error: Exception
    # Run the step in a task so that it can be cancelled.
    step_task = asyncio.create_task(step.func(*args, **kwargs))

    async def listen() -> None:
        nonlocal cancelled, reason
        try:
            async for chunk in streams.reconnecting_frames(world, run_id, stream_name):
                cancelled = True
                try:
                    data = ser.hydrate(chunk, what=f"the cancellation of step {step_id}")
                except ser.SerializationError:
                    pass
                else:
                    if isinstance(data, dict) and data.get("reason") is not None:
                        reason = str(data["reason"])
                break
        except Exception as error:
            # A dead listener must not fail the step: without the signal
            # the step simply runs to completion.
            logger.debug(f"Cancellation listener for step {step_id!r} failed: {error}")
            return
        if cancelled:
            step_task.cancel(reason)

    async with anyio.create_task_group() as tg:
        tg.start_soon(listen)
        try:
            return await step_task
        except asyncio.CancelledError:
            # We only want to turn a CancelledError into
            # StepCancelledError if we actually got a
            # cancel. Otherwise we assume that the server is shutting
            # us down or something.
            if not cancelled:
                raise

            step_error = errors.StepCancelledError(
                reason if reason is not None else "step cancelled by its workflow"
            )
        except Exception as error:
            # Save the error to avoid ExceptionGroup wrapping
            step_error = error
        finally:
            tg.cancel_scope.cancel()

    raise step_error


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

    # step_started validates state (terminal -> EntityConflictError, retryAfter
    # not reached -> TooEarlyError), increments the attempt, and returns the
    # updated step entity, so no step pre-read is needed.
    try:
        start_result = await world.events_create(
            req.run_id,
            w.StepStartedEvent(correlation_id=req.step_id),
        )
    except w.TooEarlyError as e:
        # retryAfter not reached yet — keep the message visible and retry later.
        timeout_seconds = max(1, e.retry_after or 1)
        logger.debug(
            "[Workflows] '%s' - step '%s' retryAfter not reached, deferring %ds",
            req.run_id,
            req.step_name,
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
                run_id=req.run_id,
                requested_at=datetime.now(UTC),
            ),
        )
        return None

    # Use the step entity from the event response
    if not start_result.step:
        raise RuntimeError(f"step_started event for '{req.step_id}' did not return step entity")
    step_run = start_result.step
    current_attempt = step_run.attempt
    if not step_run.started_at:
        raise RuntimeError(f"Step '{req.step_id}' has no 'startedAt' timestamp")

    try:
        step = registry._get_step(req.step_name)
    except errors.StepNotRegisteredError as missing_error:
        logger.error(
            "[Workflows] '%s' - step '%s' is not registered, failing step",
            req.run_id,
            req.step_name,
        )
        await world.events_create(
            req.run_id,
            w.StepFailedEventData(error=ser.dehydrate_error(missing_error)).into_event(req.step_id),
        )
        await world.queue(
            queue_name,
            w.WorkflowInvokePayload(
                run_id=req.run_id,
                requested_at=datetime.now(UTC),
            ),
        )
        return None

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
                error=ser.dehydrate_error(
                    await _retries_exhausted(
                        error_message, step_run, world=world, run_id=req.run_id
                    )
                )
            ).into_event(req.step_id),
        )

        # Re-invoke the workflow to handle the failed step
        await world.queue(
            queue_name,
            w.WorkflowInvokePayload(
                run_id=req.run_id,
                requested_at=datetime.now(UTC),
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

    # Populate step_info and workflow_info
    step_state_token = _step_state_ctx.set(
        _StepState(
            step_info=StepInfo(
                run_id=req.run_id,
                step_id=req.step_id,
                step_name=step.name,
                attempt=current_attempt,
                step_started_at=step_run.started_at,
            ),
            workflow_info=WorkflowInfo(
                run_id=req.run_id,
                workflow_name=_workflow_name_from_queue(queue_name),
                # We have no way to get started_at without fetching
                # the run, which we don't want to have to do.
                started_at=None,
                url=_workflow_url(),
                features=WorkflowFeatures(
                    encryption=bool(step_run.input) and ser.is_encrypted(step_run.input)
                ),
            ),
        )
    )

    try:
        async with step_streams.dispatching():
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
            args, kwargs = step.codec.validate_arguments(args, kwargs)

            logger.debug(
                "[Workflows] '%s' - invoking step '%s' (step_id=%s, attempt=%d)",
                req.run_id,
                step.name,
                req.step_id,
                current_attempt,
            )
            # Execute the step function
            if step.cancellable:
                result = await _run_cancellable_step(
                    step, args, kwargs, world=world, run_id=req.run_id, step_id=req.step_id
                )
            else:
                result = await step.func(*args, **kwargs)

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
        output = ser.dehydrate(step.codec.dump_return(result))

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
            failure: Exception = e
            if fatal:
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
                failure = errors.FatalError(
                    f"Step '{step.name}' failed after {step.max_retries} "
                    f"{'retry' if step.max_retries == 1 else 'retries'}: {error_text}"
                )
                failure.__cause__ = e
                failure.stack = traceback.format_exc().rstrip("\n")  # type: ignore[attr-defined]
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
            await world.events_create(
                req.run_id,
                w.StepFailedEventData(error=ser.dehydrate_error(failure)).into_event(req.step_id),
            )
        else:
            # Not at max retries yet - retry the step
            logger.warning(
                "[Workflows] '%s' - Encountered %s "
                "while executing step '%s' (attempt %d): "
                "%s\n\n  This step has failed but will be retried",
                req.run_id,
                "RetryableError" if isinstance(e, errors.RetryableError) else "Error",
                step.name,
                current_attempt,
                e,
            )

            retry_at = e.retry_at if isinstance(e, errors.RetryableError) else None

            # Set step to pending for retry
            await world.events_create(
                req.run_id,
                w.StepRetryingEventData(
                    error=ser.dehydrate_error(e), retry_after=retry_at
                ).into_event(req.step_id),
            )

            # Return timeout to keep message visible for retry
            delay_seconds = 1.0
            if retry_at is not None:
                remaining = (retry_at - datetime.now(UTC)).total_seconds()
                delay_seconds = float(max(1, math.ceil(remaining)))
            return w.QueueContinuation(delay_seconds=delay_seconds)

    finally:
        _step_state_ctx.reset(step_state_token)
        _step_streams_ctx.reset(streams_token)

    # Re-invoke the workflow to continue execution
    await world.queue(
        queue_name,
        w.WorkflowInvokePayload(
            run_id=req.run_id,
            requested_at=datetime.now(UTC),
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
        # A seal marks a slot whose write never landed -- nothing happened there to replay.
        all_events.extend(event for event in response.data if event.event_type != "noop")
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


class Run(Generic[T]):
    def __init__(
        self,
        run_id: str,
        *,
        output_codec: signature_codec.SignatureCodec | None = None,
    ) -> None:
        self._run_id = run_id
        self._world = w.get_world()
        # Only `start` has the workflow in hand; a `Run` built from a run id
        # picked up elsewhere reads its output as whatever the wire carried.
        self._codec = output_codec

    @property
    def run_id(self) -> str:
        return self._run_id

    async def status(self) -> Literal["pending", "running", "completed", "failed", "cancelled"]:
        run = await self._world.runs_get(self._run_id)
        return run.status

    async def attributes(self) -> dict[str, str]:
        run = await self._world.runs_get(self._run_id)
        return dict(run.attributes)

    async def _failure(self, run: w.WorkflowRun) -> Exception:
        what = f"the error of run {run.run_id}"
        try:
            key = None
            if ser.is_encrypted(run.error):
                key = await self._world.run_key(run.run_id, deployment_id=run.deployment_id)
            return ser.hydrate_error(run.error, what=what, key=key)
        except Exception as error:
            logger.debug("[Workflows] '%s' - could not read %s: %s", run.run_id, what, error)
            return RuntimeError(f"cannot read {what}: {error}")

    async def return_value(self) -> T:
        while True:
            run = await self._world.runs_get(self._run_id)
            if run.status == "completed":
                if not run.output:
                    raise RuntimeError(f"Completed workflow {run.run_id} has no output")
                key = None
                if ser.is_encrypted(run.output):
                    key = await self._world.run_key(run.run_id, deployment_id=run.deployment_id)
                output = ser.hydrate(run.output, what=f"the output of run {run.run_id}", key=key)
                if self._codec is None:
                    return cast("T", output)
                return cast("T", self._codec.validate_return(output))

            elif run.status == "cancelled":
                raise RuntimeError("workflow cancelled")

            elif run.status == "failed":
                raise errors.WorkflowRunFailedError(
                    run.run_id,
                    await self._failure(run),
                    error_code=run.error_code,
                )

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


async def start(wf: core.Workflow[P, T], *args: P.args, **kwargs: P.kwargs) -> Run[T]:
    # Bound before anything is written, so an arity mistake raises here instead
    # of leaving a run behind that fails when its body is invoked.
    bound_args, bound_kwargs = wf.bind_arguments(args, kwargs)
    dumped_args, dumped_kwargs = wf.codec.dump_arguments(bound_args, bound_kwargs)
    world = w.get_world()
    deployment_id = await world.get_deployment_id()
    namespace = wf._resolve_queue_namespace()
    input_data = ser.dehydrate(ser.argument_array(dumped_args, dumped_kwargs))
    execution_context: dict[str, Any] = {"hookResumeInputVersion": w.HOOK_RESUME_INPUT_VERSION}
    if namespace is not None:
        execution_context["queueNamespace"] = namespace
    data = w.RunCreatedEventData(
        deployment_id=deployment_id,
        workflow_name=wf.workflow_id,
        input=input_data,
        execution_context=execution_context,
    )
    result = await world.events_create(None, data.into_event())

    # Assert that the run was created
    if not result.run:
        raise RuntimeError("Missing 'run' in server response for 'run_created' event")

    run_id = result.run.run_id
    await world.queue(
        w.get_queue_name(wf.workflow_id, namespace),
        w.WorkflowInvokePayload(run_id=run_id),
        deployment_id=deployment_id,
    )

    return Run(run_id, output_codec=wf.codec)


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
        w.WorkflowInvokePayload(run_id=hook.run_id),
    )
    return hook
