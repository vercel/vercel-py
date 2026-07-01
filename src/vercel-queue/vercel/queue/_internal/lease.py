"""Lease lifecycle helpers for queue message processing.

Automatic lease renewal is coordinated through one process-wide daemon thread
running an AnyIO worker. Public sync and async clients enqueue renewal commands
into an AnyIO memory stream owned by that worker, while the worker runs one
AnyIO task per active renewal.

Registration waits until the worker records the renewal and schedules its task,
but it does not wait for the first lease extension to complete. This surfaces
delivery failures at handler startup without coupling startup to renewal
network I/O. Fire-and-forget stop paths are still logged asynchronously.

Stopping a renewal removes future work and cancels any in-flight extension. The
RetryAfter path waits for that cancellation so a late automatic renewal cannot
overwrite the handler's explicit visibility update, while acknowledge paths can
avoid that tail latency because late renewals are harmless after ACK.

Lease renewal invariants:

- A renewal handle is single-use and idempotently stoppable.
- Start waits for worker scheduling, not lease extension I/O.
- Stop with wait=True waits for in-flight renewal cancellation or completion.
- At most one renewal for a token may be in flight at a time.
- Command submission uses a large bounded AnyIO memory stream.
"""

from __future__ import annotations

from typing import Any, NewType, Protocol, TypeAlias
from typing_extensions import Self

import logging
import math
import threading
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Coroutine, Iterator, MutableMapping
from dataclasses import dataclass
from datetime import datetime, timezone
from types import TracebackType

import anyio
import anyio.from_thread
import httpx
from anyio import to_thread
from anyio.abc import ObjectReceiveStream, ObjectSendStream, TaskGroup
from anyio.lowlevel import EventLoopToken, current_token
from anyio.streams.memory import MemoryObjectSendStream

from .errors import QueueError, ThrottledError
from .log import debug_log
from .streams import (
    AsyncStreamPayload,
    AsyncTextStreamPayload,
    SyncStreamPayload,
    SyncTextStreamPayload,
)
from .types import (
    Duration,
    Message,
    MessageMetadata,
    duration_to_float_seconds,
    duration_to_seconds,
)


class AsyncExtendMessageLease(Protocol):
    def __call__(
        self,
        message: Message[Any],
        duration: Duration,
    ) -> Coroutine[Any, Any, None]: ...


class ExtendMessageLease(Protocol):
    def __call__(self, message: Message[Any], duration: Duration) -> None: ...


class _LeaseStopEvent(Protocol):
    def set(self) -> None: ...


DEFAULT_PROCESSING_LEASE_SECONDS = 300
MIN_PROCESSING_LEASE_SECONDS = 30
MAX_VISIBILITY_TIMEOUT_SECONDS = 3600
_TRANSIENT_LEASE_RETRY_SECONDS = 3.0
_TRANSIENT_FOLLOW_UP_RETRY_SECONDS = 0.1
_DUE_RENEWAL_START_GRACE_SECONDS = 0.01
_IMMEDIATE_RENEWAL_GRACE_SECONDS = 10.0
DIRECTIVE_FOLLOW_UP_ATTEMPTS = 3
_STOPPED_LEASE_TOKEN_CACHE_SIZE = 1024
_LEASE_STOP_WAIT_TIMEOUT_SECONDS = 5.0
_LEASE_START_WAIT_TIMEOUT_SECONDS = 5.0
_LEASE_COMMAND_BUFFER_SIZE = 65_536
_LeaseRenewalToken = NewType("_LeaseRenewalToken", object)
logger = logging.getLogger(__name__)


class LeaseAsyncClient(Protocol):
    def _renew_lease(
        self,
        message: Message[Any],
        duration: Duration,
    ) -> Coroutine[Any, Any, None]: ...


@dataclass(kw_only=True)
class _LeaseExtensionRequest:
    token: _LeaseRenewalToken
    message: Message[Any]
    client: LeaseAsyncClient
    lease_seconds: int
    next_extension_at: float


@dataclass(kw_only=True)
class _LeaseExtensionStart:
    request: _LeaseExtensionRequest
    scheduled_event: threading.Event | anyio.Event
    event_loop_token: EventLoopToken | None = None


@dataclass(frozen=True, kw_only=True)
class _LeaseExtensionStop:
    token: _LeaseRenewalToken
    done_send: ObjectSendStream[None] | None = None
    done_event: _LeaseStopEvent | None = None
    event_loop_token: EventLoopToken | None = None


@dataclass(frozen=True)
class _LeaseWorkerShutdown:
    pass


_LeaseExtensionCommand: TypeAlias = (
    _LeaseExtensionStart | _LeaseExtensionStop | _LeaseWorkerShutdown
)


@dataclass(kw_only=True)
class _LeaseRenewalTask:
    request: _LeaseExtensionRequest
    cancel_scope: anyio.CancelScope
    stop_waiters: list[_LeaseExtensionStop]


@dataclass(kw_only=True)
class _LeaseWorkerRuntimeState:
    active: dict[_LeaseRenewalToken, _LeaseRenewalTask]
    stopped: OrderedDict[_LeaseRenewalToken, None]


@dataclass(kw_only=True)
class _LeaseWorkerState:
    command_send: MemoryObjectSendStream[_LeaseExtensionCommand] | None = None
    worker_token: EventLoopToken | None = None
    started: bool = False
    thread: threading.Thread | None = None
    startup_error: BaseException | None = None


_lease_worker_state = _LeaseWorkerState()
_lease_worker_ready = threading.Event()
_lease_worker_lock = threading.Lock()
_LEASE_WORKER_START_TIMEOUT_SECONDS = 5.0


def visibility_timeout_seconds(duration: Duration, *, name: str = "duration") -> int:
    exact_seconds = duration_to_float_seconds(duration)
    if not math.isfinite(exact_seconds):
        raise ValueError(f"{name} must be finite")
    if exact_seconds < 0:
        raise ValueError(f"{name} must be non-negative")
    if exact_seconds > MAX_VISIBILITY_TIMEOUT_SECONDS:
        raise ValueError(f"{name} cannot exceed {MAX_VISIBILITY_TIMEOUT_SECONDS} seconds")
    seconds = duration_to_seconds(duration)
    if exact_seconds > 0 and seconds == 0:
        raise ValueError(f"{name} must be at least 1 second or exactly 0")
    return seconds


def processing_lease_seconds(duration: Duration | None, *, name: str = "lease_duration") -> int:
    if duration is None:
        return DEFAULT_PROCESSING_LEASE_SECONDS
    seconds = visibility_timeout_seconds(duration, name=name)
    if seconds == 0:
        raise ValueError(f"{name} must be positive for automatic renewal")
    return max(MIN_PROCESSING_LEASE_SECONDS, seconds)


def _next_extension_delay(metadata: MessageMetadata, lease_seconds: int) -> float:
    if metadata.visibility_deadline is not None:
        remaining = (metadata.visibility_deadline - datetime.now(timezone.utc)).total_seconds()
        if remaining <= _IMMEDIATE_RENEWAL_GRACE_SECONDS:
            return 0.0
        return max(0.0, remaining - _IMMEDIATE_RENEWAL_GRACE_SECONDS)
    return _renewal_interval_seconds(lease_seconds)


def _renewal_interval_seconds(visibility_timeout_seconds: float) -> float:
    return min(60.0, max(10.0, visibility_timeout_seconds / 5))


def _next_extension_at(metadata: MessageMetadata, lease_seconds: int) -> float:
    delay = _next_extension_delay(metadata, lease_seconds)
    if delay <= 0.0:
        delay = _DUE_RENEWAL_START_GRACE_SECONDS
    return time.monotonic() + delay


class LeaseRenewal:
    """Single-use context manager for keeping a message lease alive."""

    def __init__(
        self,
        message: Message[Any],
        client: LeaseAsyncClient,
        lease_duration: Duration | None = None,
    ) -> None:
        self._message = message
        self._client = client
        self._lease_seconds = processing_lease_seconds(lease_duration)
        self._token: _LeaseRenewalToken | None = None
        self._entered = False
        self._closed = False
        self._closing = False

    def __enter__(self) -> Self:
        self.start()
        return self

    def start(self) -> None:
        """Start renewing the lease."""
        self._start_with_wait()

    async def start_async(self) -> None:
        """Start renewing the lease from async code."""
        await self._start_with_wait_async()

    def _start_with_wait(self) -> None:
        request = self._prepare_start()
        if request is None:
            return
        try:
            _send_lease_extension_start(request)
        except Exception:
            self._reset_start_after_failure()
            raise
        self._log_start_result()

    async def _start_with_wait_async(self) -> None:
        request = self._prepare_start()
        if request is None:
            return
        try:
            await _send_lease_extension_start_async(request)
        except BaseException:
            await _send_best_effort_async_start_failure_stop(request.token)
            self._reset_start_after_failure()
            raise
        self._log_start_result()

    def _prepare_start(self) -> _LeaseExtensionRequest | None:
        if self._entered:
            raise RuntimeError("lease renewal handles are single-use")
        if self._closed:
            raise RuntimeError("lease renewal handle is closed")
        if self._message.metadata.receipt_handle is None:
            return None
        self._entered = True
        _ensure_lease_renewal_thread()
        self._token = _LeaseRenewalToken(object())
        return _LeaseExtensionRequest(
            token=self._token,
            message=self._message,
            client=self._client,
            lease_seconds=self._lease_seconds,
            next_extension_at=_next_extension_at(self._message.metadata, self._lease_seconds),
        )

    def _reset_start_after_failure(self) -> None:
        self._token = None
        self._entered = False

    def _log_start_result(self) -> None:
        debug_log(
            "lease.renewal_registered",
            message_id=self._message.metadata.message_id,
            topic=self._message.metadata.topic,
            consumer_group=self._message.metadata.consumer_group,
            lease_seconds=self._lease_seconds,
        )

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop()

    def stop(self, *, wait: bool = True) -> None:
        """Stop renewing the lease. Safe to call more than once."""
        if self._closed or self._closing:
            return
        token = self._token
        if token is None:
            self._closed = True
            return
        self._closing = True
        try:
            stopped = self._send_stop(token, wait=wait)
        except Exception:
            self._closing = False
            raise
        self._token = None
        self._closed = True
        self._closing = False
        self._log_stop_result(stopped=stopped)

    async def stop_async(self, *, wait: bool = True) -> None:
        """Stop renewing the lease without blocking the current event loop."""
        if self._closed or self._closing:
            return
        token = self._token
        if token is None:
            self._closed = True
            return
        self._closing = True
        try:
            stopped = await self._send_stop_async(token, wait=wait)
        except BaseException:
            self._closing = False
            raise
        self._token = None
        self._closed = True
        self._closing = False
        self._log_stop_result(stopped=stopped)

    def _send_stop(self, token: _LeaseRenewalToken, *, wait: bool) -> bool:
        if wait:
            return _send_lease_extension_stop(token)
        self._send_best_effort_stop(token)
        return True

    async def _send_stop_async(self, token: _LeaseRenewalToken, *, wait: bool) -> bool:
        if wait:
            return await _send_lease_extension_stop_async(token)
        await self._send_best_effort_stop_async(token)
        return True

    def _log_stop_result(self, *, stopped: bool) -> None:
        if stopped:
            debug_log(
                "lease.renewal_stopped",
                message_id=self._message.metadata.message_id,
                topic=self._message.metadata.topic,
                consumer_group=self._message.metadata.consumer_group,
            )
            return
        logger.warning(
            "timed out waiting for queue lease renewal to stop for message %s on topic %s/%s",
            self._message.metadata.message_id,
            self._message.metadata.topic,
            self._message.metadata.consumer_group,
        )
        debug_log(
            "lease.renewal_stop_timeout",
            message_id=self._message.metadata.message_id,
            topic=self._message.metadata.topic,
            consumer_group=self._message.metadata.consumer_group,
            timeout_seconds=_LEASE_STOP_WAIT_TIMEOUT_SECONDS,
        )

    def _send_best_effort_stop(self, token: _LeaseRenewalToken) -> None:
        try:
            _send_lease_command(_LeaseExtensionStop(token=token))
        except Exception as exc:  # noqa: BLE001
            _log_best_effort_lease_stop_failure(exc)

    async def _send_best_effort_stop_async(self, token: _LeaseRenewalToken) -> None:
        try:
            await to_thread.run_sync(_send_lease_command, _LeaseExtensionStop(token=token))
        except Exception as exc:  # noqa: BLE001
            _log_best_effort_lease_stop_failure(exc)

    async def extend_async(
        self,
        duration: Duration,
        extend_lease: AsyncExtendMessageLease,
    ) -> None:
        """Extend the lease immediately if this message has lease metadata."""
        if self._message.metadata.receipt_handle is None:
            return
        await retry_async_follow_up(
            lambda: extend_lease(self._message, duration),
            event_prefix="visibility",
        )

    def extend(
        self,
        duration: Duration,
        extend_lease: ExtendMessageLease,
    ) -> None:
        """Extend the lease immediately if this message has lease metadata."""
        if self._message.metadata.receipt_handle is None:
            return
        retry_sync_follow_up(
            lambda: extend_lease(self._message, duration),
            event_prefix="visibility",
        )


def _send_lease_command(command: _LeaseExtensionCommand) -> None:
    command_send = _lease_worker_state.command_send
    worker_token = _lease_worker_state.worker_token
    if command_send is None or worker_token is None:
        raise RuntimeError("lease extension worker is not ready")
    try:
        anyio.from_thread.run_sync(command_send.send_nowait, command, token=worker_token)
    except anyio.WouldBlock as exc:
        raise RuntimeError("lease extension worker command buffer is full") from exc


def _send_lease_extension_start(request: _LeaseExtensionRequest) -> None:
    scheduled_event = threading.Event()
    command = _LeaseExtensionStart(request=request, scheduled_event=scheduled_event)
    _send_lease_command(command)
    if not scheduled_event.wait(_LEASE_START_WAIT_TIMEOUT_SECONDS):
        _send_best_effort_start_timeout_stop(request.token)
        raise TimeoutError("timed out waiting for queue lease renewal to start")


async def _send_lease_extension_start_async(request: _LeaseExtensionRequest) -> None:
    scheduled_event = anyio.Event()
    command = _LeaseExtensionStart(
        request=request,
        scheduled_event=scheduled_event,
        event_loop_token=current_token(),
    )
    send_errors = _send_lease_extension_start_command_async(command)
    try:
        with anyio.fail_after(_LEASE_START_WAIT_TIMEOUT_SECONDS):
            await scheduled_event.wait()
    except TimeoutError:
        await to_thread.run_sync(_send_best_effort_start_timeout_stop, request.token)
        raise
    if send_errors:
        raise send_errors[0]


def _send_lease_extension_start_command_async(
    command: _LeaseExtensionStart,
) -> list[Exception]:
    command_send = _lease_worker_state.command_send
    worker_token = _lease_worker_state.worker_token
    if command_send is None or worker_token is None:
        raise RuntimeError("lease extension worker is not ready")
    caller_token = current_token()
    send_errors: list[Exception] = []

    def send_nowait() -> None:
        try:
            command_send.send_nowait(command)
        except anyio.WouldBlock as exc:
            error = RuntimeError("lease extension worker command buffer is full")
            error.__cause__ = exc
            send_errors.append(error)
            _schedule_event_loop_callback(command.scheduled_event.set, caller_token)
        except Exception as exc:  # noqa: BLE001
            send_errors.append(exc)
            _schedule_event_loop_callback(command.scheduled_event.set, caller_token)

    _schedule_event_loop_callback(send_nowait, worker_token)
    return send_errors


def _send_best_effort_start_timeout_stop(token: _LeaseRenewalToken) -> None:
    try:
        _send_lease_command(_LeaseExtensionStop(token=token))
    except Exception as exc:
        logger.debug("failed to cancel timed out queue lease renewal start", exc_info=exc)
        debug_log(
            "lease.start_timeout_stop_failed",
            exception_class=exc.__class__.__name__,
            exception_message=str(exc),
        )


async def _send_best_effort_async_start_failure_stop(token: _LeaseRenewalToken) -> None:
    with anyio.CancelScope(shield=True):
        try:
            await to_thread.run_sync(_send_best_effort_start_timeout_stop, token)
        except Exception as exc:
            logger.debug("failed to cancel failed queue lease renewal start", exc_info=exc)
            debug_log(
                "lease.start_failure_stop_failed",
                exception_class=exc.__class__.__name__,
                exception_message=str(exc),
            )


def _log_best_effort_lease_stop_failure(exc: BaseException) -> None:
    logger.debug("failed to send best-effort queue lease renewal stop", exc_info=exc)
    debug_log(
        "lease.best_effort_stop_failed",
        exception_class=exc.__class__.__name__,
        exception_message=str(exc),
    )


def _send_lease_extension_stop(token: _LeaseRenewalToken) -> bool:
    deadline = time.monotonic() + _LEASE_STOP_WAIT_TIMEOUT_SECONDS
    done_event = threading.Event()
    _send_lease_command(_LeaseExtensionStop(token=token, done_event=done_event))
    return done_event.wait(max(0.0, deadline - time.monotonic()))


async def _send_lease_extension_stop_async(token: _LeaseRenewalToken) -> bool:
    done_event = anyio.Event()
    await to_thread.run_sync(
        _send_lease_command,
        _LeaseExtensionStop(
            token=token,
            done_event=done_event,
            event_loop_token=current_token(),
        ),
    )
    with anyio.move_on_after(_LEASE_STOP_WAIT_TIMEOUT_SECONDS) as scope:
        await done_event.wait()
    return not scope.cancel_called


def _ensure_lease_renewal_thread() -> None:
    if _lease_worker_state.started:
        _wait_for_lease_worker_ready()
        return
    wait_for_ready = False
    with _lease_worker_lock:
        if _lease_worker_state.started:
            wait_for_ready = True
        else:
            _lease_worker_ready.clear()
            _lease_worker_state.startup_error = None
            thread = threading.Thread(
                target=_lease_renewal_worker_thread,
                name="vercel-queue-lease-renewal",
                daemon=True,
            )
            debug_log("lease.worker_start", thread_name=thread.name)
            thread.start()
            _lease_worker_state.thread = thread
            _lease_worker_state.started = True
            wait_for_ready = True
    if wait_for_ready:
        _wait_for_lease_worker_ready()


def _wait_for_lease_worker_ready() -> None:
    thread = _lease_worker_state.thread
    if not _lease_worker_ready.wait(_LEASE_WORKER_START_TIMEOUT_SECONDS):
        _mark_lease_worker_stopped(thread)
        raise RuntimeError("lease extension worker did not start")
    startup_error = _lease_worker_state.startup_error
    if startup_error is not None:
        _mark_lease_worker_stopped(thread)
        raise RuntimeError("lease extension worker failed to start") from startup_error


def _lease_renewal_worker_thread() -> None:
    try:
        _run_lease_renewal_worker_thread()
    except BaseException as exc:
        _lease_worker_state.startup_error = exc
        _lease_worker_ready.set()
        raise
    finally:
        _mark_lease_worker_stopped(threading.current_thread())


def _mark_lease_worker_stopped(thread: threading.Thread | None = None) -> None:
    with _lease_worker_lock:
        if thread is not None and _lease_worker_state.thread is not thread:
            return
        _lease_worker_state.command_send = None
        _lease_worker_state.worker_token = None
        _lease_worker_state.thread = None
        _lease_worker_state.started = False


def _run_lease_renewal_worker_thread() -> None:
    anyio.run(_run_lease_renewal_worker_async)


async def _run_lease_renewal_worker_async() -> None:
    command_send, command_receive = anyio.create_memory_object_stream[_LeaseExtensionCommand](
        _LEASE_COMMAND_BUFFER_SIZE
    )
    _lease_worker_state.command_send = command_send
    _lease_worker_state.worker_token = current_token()
    _lease_worker_ready.set()
    debug_log("lease.worker_ready")
    try:
        await _lease_worker_async(command_receive)
    finally:
        command_send.close()
        await command_receive.aclose()


async def _lease_worker_async(
    commands: ObjectReceiveStream[_LeaseExtensionCommand],
) -> None:
    state = _LeaseWorkerRuntimeState(
        active={},
        stopped=OrderedDict(),
    )
    async with anyio.create_task_group() as task_group:
        while True:
            try:
                command = await _receive_lease_command(commands)
            except anyio.EndOfStream:
                task_group.cancel_scope.cancel()
                return
            if command is None:
                continue
            if await _handle_lease_worker_command(command, state, task_group):
                task_group.cancel_scope.cancel()
                return


async def _handle_lease_worker_command(
    command: _LeaseExtensionCommand,
    state: _LeaseWorkerRuntimeState,
    task_group: TaskGroup,
) -> bool:
    if isinstance(command, _LeaseExtensionStart):
        _handle_lease_extension_start(command, state, task_group)
        return False
    if isinstance(command, _LeaseExtensionStop):
        await _handle_lease_extension_stop(command, state)
        return False

    debug_log("lease.worker_shutdown")
    return True


def _handle_lease_extension_start(
    command: _LeaseExtensionStart,
    state: _LeaseWorkerRuntimeState,
    task_group: TaskGroup,
) -> None:
    try:
        _start_lease_renewal_task(command.request, state, task_group)
    finally:
        _signal_lease_start_scheduled(command)


def _signal_lease_start_scheduled(command: _LeaseExtensionStart) -> None:
    try:
        _schedule_lease_start_signal(command)
    except Exception as exc:
        logger.debug("failed to signal queue lease renewal start scheduling", exc_info=exc)
        debug_log(
            "lease.start_signal_failed",
            exception_class=exc.__class__.__name__,
            exception_message=str(exc),
        )


def _schedule_lease_start_signal(command: _LeaseExtensionStart) -> None:
    if command.event_loop_token is None:
        command.scheduled_event.set()
        return
    _schedule_event_loop_callback(command.scheduled_event.set, command.event_loop_token)


def _schedule_event_loop_callback(
    callback: Callable[[], None],
    event_loop_token: EventLoopToken,
) -> None:
    native_token = event_loop_token.native_token
    if call_soon_threadsafe := getattr(native_token, "call_soon_threadsafe", None):
        call_soon_threadsafe(callback)
        return
    if run_sync_soon := getattr(native_token, "run_sync_soon", None):
        run_sync_soon(callback)
        return
    anyio.from_thread.run_sync(callback, token=event_loop_token)


def _start_lease_renewal_task(
    request: _LeaseExtensionRequest,
    state: _LeaseWorkerRuntimeState,
    task_group: TaskGroup,
) -> None:
    if request.token in state.stopped:
        state.stopped.pop(request.token, None)
        return
    if request.token in state.active:
        return
    renewal_task = _LeaseRenewalTask(
        request=request,
        cancel_scope=anyio.CancelScope(),
        stop_waiters=[],
    )
    state.active[request.token] = renewal_task
    task_group.start_soon(_run_lease_renewal_task, renewal_task, state)


async def _handle_lease_extension_stop(
    command: _LeaseExtensionStop,
    state: _LeaseWorkerRuntimeState,
) -> None:
    renewal_task = state.active.pop(command.token, None)
    if renewal_task is not None:
        if command.done_send is not None or command.done_event is not None:
            renewal_task.stop_waiters.append(command)
        renewal_task.cancel_scope.cancel()
        return

    _remember_stopped_lease_token(command.token, state)
    await _signal_lease_stop_complete(command)


def _remember_stopped_lease_token(
    token: _LeaseRenewalToken,
    state: _LeaseWorkerRuntimeState,
) -> None:
    state.stopped[token] = None
    state.stopped.move_to_end(token)
    while len(state.stopped) > _STOPPED_LEASE_TOKEN_CACHE_SIZE:
        state.stopped.popitem(last=False)


async def _signal_lease_stop_complete(command: _LeaseExtensionStop) -> None:
    if command.done_send is not None:
        try:
            await command.done_send.send(None)
        except Exception as exc:  # noqa: BLE001
            _log_lease_stop_signal_failure(exc)
    if command.done_event is not None:
        try:
            if command.event_loop_token is None:
                command.done_event.set()
            else:
                _schedule_event_loop_callback(command.done_event.set, command.event_loop_token)
        except Exception as exc:  # noqa: BLE001
            _log_lease_stop_signal_failure(exc)


def _log_lease_stop_signal_failure(exc: BaseException) -> None:
    logger.debug("failed to signal queue lease renewal stop completion", exc_info=exc)
    debug_log(
        "lease.stop_signal_failed",
        exception_class=exc.__class__.__name__,
        exception_message=str(exc),
    )


def reset_lease_renewal_worker_for_tests() -> None:
    """Stop the background lease renewal worker and clear cached clients."""
    thread = _lease_worker_state.thread
    if _lease_worker_state.command_send is not None:
        _send_lease_command(_LeaseWorkerShutdown())
    if thread is not None:
        thread.join(timeout=5)
    with _lease_worker_lock:
        _lease_worker_state.command_send = None
        _lease_worker_state.worker_token = None
        _lease_worker_state.thread = None
        _lease_worker_state.started = False
        _lease_worker_ready.clear()


async def _receive_lease_command(
    commands: ObjectReceiveStream[_LeaseExtensionCommand],
) -> _LeaseExtensionCommand:
    return await commands.receive()


def _is_client_error(exc: QueueError) -> bool:
    if isinstance(exc, ThrottledError):
        return False
    status_code = exc.status_code
    if status_code == 408:
        return False
    return status_code is not None and 400 <= status_code < 500


async def _run_lease_renewal_task(
    renewal_task: _LeaseRenewalTask,
    state: _LeaseWorkerRuntimeState,
) -> None:
    request = renewal_task.request
    try:
        with renewal_task.cancel_scope:
            while state.active.get(request.token) is renewal_task:
                await anyio.sleep(max(0.0, request.next_extension_at - time.monotonic()))
                if state.active.get(request.token) is not renewal_task:
                    return
                await _run_lease_extension(request, _LeaseTaskActiveMap(state.active))
    finally:
        with anyio.CancelScope(shield=True):
            if state.active.get(request.token) is renewal_task:
                state.active.pop(request.token, None)
            for stop_command in renewal_task.stop_waiters:
                await _signal_lease_stop_complete(stop_command)


class _LeaseTaskActiveMap(MutableMapping[_LeaseRenewalToken, _LeaseExtensionRequest]):
    def __init__(self, active: dict[_LeaseRenewalToken, _LeaseRenewalTask]) -> None:
        self._active = active

    def __getitem__(self, token: _LeaseRenewalToken) -> _LeaseExtensionRequest:
        return self._active[token].request

    def __setitem__(
        self,
        token: _LeaseRenewalToken,
        request: _LeaseExtensionRequest,
    ) -> None:
        self._active[token].request = request

    def __delitem__(self, token: _LeaseRenewalToken) -> None:
        del self._active[token]

    def __iter__(self) -> Iterator[_LeaseRenewalToken]:
        return iter(self._active)

    def __len__(self) -> int:
        return len(self._active)


async def _run_lease_extension(
    request: _LeaseExtensionRequest,
    active: MutableMapping[_LeaseRenewalToken, _LeaseExtensionRequest],
) -> None:
    if request.token not in active:
        return
    debug_log(
        "lease.extension_attempt",
        message_id=request.message.metadata.message_id,
        topic=request.message.metadata.topic,
        consumer_group=request.message.metadata.consumer_group,
        lease_seconds=request.lease_seconds,
    )
    try:
        await request.client._renew_lease(request.message, request.lease_seconds)  # noqa: SLF001
    except QueueError as exc:
        if _is_client_error(exc):
            active.pop(request.token, None)
            debug_log(
                "lease.client_error_stop",
                message_id=request.message.metadata.message_id,
                topic=request.message.metadata.topic,
                consumer_group=request.message.metadata.consumer_group,
                exception_class=exc.__class__.__name__,
                exception_message=str(exc),
            )
            return
        if request.token not in active:
            return
        logger.warning(
            "transient queue lease renewal failure for message %s on topic %s/%s: %s",
            request.message.metadata.message_id,
            request.message.metadata.topic,
            request.message.metadata.consumer_group,
            exc,
        )
        retry_delay = _lease_extension_retry_delay(exc, request.lease_seconds)
        request.next_extension_at = time.monotonic() + retry_delay
        debug_log(
            "lease.transient_retry_scheduled",
            message_id=request.message.metadata.message_id,
            topic=request.message.metadata.topic,
            consumer_group=request.message.metadata.consumer_group,
            retry_delay_seconds=retry_delay,
            exception_class=exc.__class__.__name__,
            exception_message=str(exc),
        )
        return
    except Exception as exc:
        if request.token not in active:
            return
        logger.warning(
            "unexpected queue lease renewal failure for message %s on topic %s/%s",
            request.message.metadata.message_id,
            request.message.metadata.topic,
            request.message.metadata.consumer_group,
            exc_info=exc,
        )
        retry_delay = _lease_extension_retry_delay(exc, request.lease_seconds)
        request.next_extension_at = time.monotonic() + retry_delay
        debug_log(
            "lease.unexpected_retry_scheduled",
            message_id=request.message.metadata.message_id,
            topic=request.message.metadata.topic,
            consumer_group=request.message.metadata.consumer_group,
            retry_delay_seconds=retry_delay,
            exception_class=exc.__class__.__name__,
            exception_message=str(exc),
        )
        return
    if request.token not in active:
        return
    request.next_extension_at = time.monotonic() + _renewal_interval_seconds(request.lease_seconds)
    debug_log(
        "lease.extension_success",
        message_id=request.message.metadata.message_id,
        topic=request.message.metadata.topic,
        consumer_group=request.message.metadata.consumer_group,
    )


def _lease_extension_retry_delay(exc: BaseException, lease_seconds: int) -> float:
    if isinstance(exc, ThrottledError) and exc.retry_after is not None:
        retry_after = max(1.0, float(exc.retry_after))
        return min(retry_after, _renewal_interval_seconds(lease_seconds))
    return _TRANSIENT_LEASE_RETRY_SECONDS


async def retry_async_follow_up(
    operation: Callable[[], Awaitable[None]],
    *,
    event_prefix: str = "follow_up",
) -> None:
    last_error: BaseException | None = None
    for attempt in range(DIRECTIVE_FOLLOW_UP_ATTEMPTS):
        try:
            debug_log(f"{event_prefix}.retry_attempt", attempt=attempt + 1)
            await operation()
        except Exception as exc:
            if not _is_retryable_follow_up_error(exc):
                raise
            last_error = exc
            if attempt == DIRECTIVE_FOLLOW_UP_ATTEMPTS - 1:
                break
            await anyio.sleep(_follow_up_retry_delay(exc))
        else:
            return
    if last_error is not None:
        debug_log(
            f"{event_prefix}.retry_exhausted",
            attempts=DIRECTIVE_FOLLOW_UP_ATTEMPTS,
            exception_class=last_error.__class__.__name__,
            exception_message=str(last_error),
        )
        raise last_error


def retry_sync_follow_up(
    operation: Callable[[], None],
    *,
    event_prefix: str = "follow_up",
) -> None:
    last_error: BaseException | None = None
    for attempt in range(DIRECTIVE_FOLLOW_UP_ATTEMPTS):
        try:
            debug_log(f"{event_prefix}.retry_attempt", attempt=attempt + 1)
            operation()
        except Exception as exc:
            if not _is_retryable_follow_up_error(exc):
                raise
            last_error = exc
            if attempt == DIRECTIVE_FOLLOW_UP_ATTEMPTS - 1:
                break
            time.sleep(_follow_up_retry_delay(exc))
        else:
            return
    if last_error is not None:
        debug_log(
            f"{event_prefix}.retry_exhausted",
            attempts=DIRECTIVE_FOLLOW_UP_ATTEMPTS,
            exception_class=last_error.__class__.__name__,
            exception_message=str(last_error),
        )
        raise last_error


def _is_retryable_follow_up_error(exc: BaseException) -> bool:
    if isinstance(exc, ThrottledError):
        return exc.retry_after is not None
    if isinstance(exc, QueueError):
        status_code = exc.status_code
        return status_code == 408 or (status_code is not None and status_code >= 500)
    return isinstance(exc, httpx.TransportError)


def _follow_up_retry_delay(exc: BaseException) -> float:
    if isinstance(exc, ThrottledError) and exc.retry_after is not None:
        return max(1.0, float(exc.retry_after))
    return _TRANSIENT_FOLLOW_UP_RETRY_SECONDS


async def finalize_payload_async(payload: Any) -> None:
    if isinstance(payload, (AsyncStreamPayload, AsyncTextStreamPayload)):
        await payload.afinalize()
    elif isinstance(payload, (SyncStreamPayload, SyncTextStreamPayload)):
        payload.finalize()


def finalize_payload_sync(payload: Any) -> None:
    if isinstance(payload, (SyncStreamPayload, SyncTextStreamPayload)):
        payload.finalize()
