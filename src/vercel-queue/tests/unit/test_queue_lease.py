from __future__ import annotations

from typing import Any

import inspect
import json
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import anyio
import anyio.from_thread
import pytest
from anyio import to_thread
from anyio.lowlevel import current_token

from vercel.headers import get_headers, set_headers
from vercel.oidc import get_vercel_oidc_token_sync
from vercel.oidc.token import _clear_cached_oidc_token
from vercel.queue import (
    ALL_DEPLOYMENTS,
    CommunicationError,
    Delivery,
    Handoff,
    LeaseRenewal,
    Message,
    MessageMetadata,
    QueueClient,
    QueueError,
    SanitizedName,
    ThrottledError,
)
from vercel.queue._internal.client import _AsyncMessageLifecycle
from vercel.queue._internal.lease import (
    _ensure_lease_renewal_thread,
    _handle_lease_extension_stop,
    _lease_worker_async,
    _lease_worker_ready,
    _lease_worker_state,
    _LeaseExtensionRequest,
    _LeaseExtensionStart,
    _LeaseExtensionStop,
    _LeaseRenewalToken,
    _LeaseWorkerRuntimeState,
    _LeaseWorkerShutdown,
    _run_lease_extension,
    _send_lease_extension_start,
    _send_lease_extension_start_async,
    _send_lease_extension_stop_async,
    _signal_lease_start_scheduled,
    _signal_lease_stop_complete,
    _wait_for_lease_worker_ready,
    visibility_timeout_seconds,
)
from vercel.queue._internal.retry import retry_async_follow_up, retry_sync_follow_up
from vercel.queue._internal.types import Duration
from vercel.queue.devserver import EmbeddedQueueDevServer
from vercel.queue.sync import QueueClient as SyncQueueClient

from .helpers import (
    CREATED_AT_DT,
    make_leased_metadata,
    queue_httpx_module,
    queue_lease_anyio_module,
)


def _queue_debug_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        json.loads(record.message) for record in caplog.records if record.name == "vercel.queue"
    ]


def _sync_test_client() -> SyncQueueClient:
    return SyncQueueClient(
        token="token",
        region=None,
        base_url=None,
        deployment=ALL_DEPLOYMENTS,
        headers=None,
        timeout=10.0,
        http_client_factory=None,
    )


def _async_test_client() -> QueueClient:
    return QueueClient(
        token="token",
        region=None,
        base_url=None,
        deployment=ALL_DEPLOYMENTS,
        headers=None,
        timeout=10.0,
        http_client_factory=None,
    )


def _test_renewal(message: Message[Any]) -> LeaseRenewal:
    return LeaseRenewal(
        message,
        client=_FakeLeaseClient(lambda _message, _duration: None),
    )


class _FakeLeaseClient:
    def __init__(
        self,
        extend: Callable[[Message[Any], Any], object],
    ) -> None:
        self._extend = extend

    async def extend_lease(self, message: Message[Any], duration: Duration) -> None:
        result = self._extend(message, duration)
        if inspect.isawaitable(result):
            await result

    async def _extend_lease(
        self,
        message: Message[Any] | MessageMetadata,
        duration: Duration,
    ) -> None:
        assert isinstance(message, Message)
        await self.extend_lease(message, duration)

    async def _renew_lease(self, message: Message[Any], duration: Duration) -> None:
        await self.extend_lease(message, duration)


class _FakeLeasePool:
    def __init__(
        self,
        extend: Callable[[Message[Any], Any], object] | None = None,
    ) -> None:
        self.calls: list[tuple[dict[str, object], Message[Any], Duration]] = []
        self._extend = extend

    @asynccontextmanager
    async def acquire_async_client(self, **kwargs: object) -> AsyncIterator[_FakeLeaseClient]:
        def extend(message: Message[Any], duration: int) -> object:
            self.calls.append((kwargs, message, duration))
            if self._extend is None:
                return None
            return self._extend(message, duration)

        yield _FakeLeaseClient(extend)


def test_sync_run_lease_renewal_extends_on_enter_and_stops_on_exit(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    message = _sync_leased_message(
        client,
        visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    with client.run_lease_renewal(message, lease_duration=30) as renewal:
        assert isinstance(renewal, LeaseRenewal)
        _wait_for_sync_lease_deadline(eqs, "msg_1", "c", 30)

    first_deadline = eqs.state.by_id["msg_1"].lease_deadline_by_consumer["c"]
    time.sleep(0.01)
    assert eqs.state.by_id["msg_1"].lease_deadline_by_consumer["c"] == first_deadline


@pytest.mark.anyio
async def test_lease_extension_does_not_install_captured_headers_context() -> None:
    seen_headers: list[dict[str, str] | None] = []
    message = Message(
        payload=None,
        metadata=replace(
            make_leased_metadata("emails"),
            visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
        ),
    )
    set_headers({"x-vercel-oidc-token": "delivery-token"})
    set_headers({"x-vercel-oidc-token": "worker-token"})

    def record_extension_headers(_message: Message[Any], _duration: object) -> None:
        headers = get_headers()
        seen_headers.append(dict(headers) if headers is not None else None)

    request = _LeaseExtensionRequest(
        token=_LeaseRenewalToken(object()),
        message=message,
        client=_FakeLeaseClient(record_extension_headers),
        lease_seconds=30,
        next_extension_at=time.monotonic(),
    )

    await _run_lease_extension(request, {request.token: request})

    assert seen_headers == [{"x-vercel-oidc-token": "worker-token"}]
    assert get_headers() == {"x-vercel-oidc-token": "worker-token"}


def test_lease_start_primes_oidc_cache_from_ambient_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    message = Message(payload=None, metadata=make_leased_metadata("emails"))
    renewal = _sync_test_client().run_lease_renewal(message, lease_duration=30)

    monkeypatch.setattr(
        "vercel.queue._internal.lease._ensure_lease_renewal_thread",
        lambda: None,
    )
    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_start", calls.append)

    _clear_cached_oidc_token()
    token = "header.eyJleHAiOjk5OTk5OTk5OTl9.signature"
    set_headers({"x-vercel-oidc-token": token})
    renewal.start()
    set_headers(None)

    assert len(calls) == 1
    assert get_vercel_oidc_token_sync() == token


def test_lease_debug_logs_worker_start_extension_success_and_stop(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")
    client = eqs.get_sync_client()
    message = _sync_leased_message(
        client,
        visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    with client.run_lease_renewal(message, lease_duration=30):
        _wait_for_sync_lease_deadline(
            eqs,
            "msg_1",
            "c",
            30,
            condition=lambda: any(
                event["event"] == "lease.extension_success" for event in _queue_debug_events(caplog)
            ),
        )

    events = _queue_debug_events(caplog)
    event_names = {event["event"] for event in events}
    assert event_names >= {
        "lease.worker_start",
        "lease.worker_ready",
        "lease.renewal_registered",
        "lease.extension_attempt",
        "lease.extension_success",
        "lease.renewal_stopped",
    }


def test_sync_run_lease_renewal_extends_immediately_when_deadline_is_near(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    message = _sync_leased_message(
        client,
        visibility_deadline=datetime.now(timezone.utc) + timedelta(seconds=5),
    )

    with client.run_lease_renewal(message, lease_duration=30):
        _wait_for_sync_lease_deadline(eqs, "msg_1", "c", 30)

    assert eqs.state.by_id["msg_1"].lease_deadline_by_consumer["c"] == (
        eqs.state.now + timedelta(seconds=30)
    )


def test_sync_lease_renewal_stop_is_idempotent() -> None:
    renewal = _sync_test_client().run_lease_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails"))
    )

    renewal.start()
    renewal.stop()
    renewal.stop()


def test_sync_lease_renewal_noops_without_receipt_handle() -> None:
    metadata = MessageMetadata(
        message_id="m1",
        delivery_count=1,
        created_at=CREATED_AT_DT,
        topic="emails",
        consumer_group=SanitizedName("c"),
    )
    with _sync_test_client().run_lease_renewal(Message(payload=None, metadata=metadata)):
        time.sleep(0.01)


def test_sync_lease_renewal_uses_default_duration(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    message = Message(
        payload=None,
        metadata=replace(
            _sync_leased_message(
                client,
                visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
            ).metadata,
            visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
        ),
    )

    with client.run_lease_renewal(message):
        _wait_for_sync_lease_deadline(eqs, "msg_1", "c", 300)

    assert eqs.state.by_id["msg_1"].lease_deadline_by_consumer["c"] == (
        eqs.state.now + timedelta(seconds=300)
    )


def test_sync_lease_renewal_floors_short_duration(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_sync_client()
    message = _sync_leased_message(
        client,
        visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    with client.run_lease_renewal(message, lease_duration=1):
        _wait_for_sync_lease_deadline(eqs, "msg_1", "c", 30)

    assert eqs.state.by_id["msg_1"].lease_deadline_by_consumer["c"] == (
        eqs.state.now + timedelta(seconds=30)
    )


def test_sync_lease_renewal_rejects_zero_duration() -> None:
    client = _sync_test_client()

    with pytest.raises(ValueError, match="lease_duration must be positive"):
        client.run_lease_renewal(
            Message(payload=None, metadata=make_leased_metadata("emails")),
            lease_duration=0,
        )


def test_sync_lease_renewal_rejects_duration_above_server_max() -> None:
    client = _sync_test_client()

    with pytest.raises(ValueError, match="lease_duration cannot exceed 3600 seconds"):
        client.run_lease_renewal(
            Message(payload=None, metadata=make_leased_metadata("emails")),
            lease_duration=3601,
        )


@pytest.mark.parametrize(
    ("duration", "expected"),
    [
        (0, 0),
        (30, 30),
        (3600, 3600),
        (timedelta(seconds=3600), 3600),
    ],
)
def test_visibility_timeout_seconds_accepts_valid_boundaries(
    duration: Duration,
    expected: int,
) -> None:
    assert visibility_timeout_seconds(duration) == expected


@pytest.mark.parametrize(
    ("duration", "message"),
    [
        (-0.5, "duration must be non-negative"),
        (0.9, "duration must be at least 1 second or exactly 0"),
        (timedelta(milliseconds=900), "duration must be at least 1 second or exactly 0"),
        (3600.9, "duration cannot exceed 3600 seconds"),
    ],
)
def test_visibility_timeout_seconds_validates_before_truncating(
    duration: Duration,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        visibility_timeout_seconds(duration)


@pytest.mark.parametrize("duration", [float("nan"), float("inf"), float("-inf")])
def test_visibility_timeout_seconds_rejects_non_finite_values(duration: float) -> None:
    with pytest.raises(ValueError, match="duration must be finite"):
        visibility_timeout_seconds(duration)


@pytest.mark.anyio
async def test_async_run_lease_renewal_starts_on_enter_and_stops(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = eqs.get_async_client(base_url=eqs.base_url)
    message = await _async_leased_message(
        client,
        visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    renewal = client.run_lease_renewal(message, lease_duration=30)
    try:
        renewal.start()
        assert isinstance(renewal, LeaseRenewal)
        await _wait_for_async_lease_deadline(eqs, "msg_1", "c", 30)
    finally:
        renewal.stop()

    first_deadline = eqs.state.by_id["msg_1"].lease_deadline_by_consumer["c"]
    await anyio.sleep(0.01)
    assert eqs.state.by_id["msg_1"].lease_deadline_by_consumer["c"] == first_deadline


@pytest.mark.anyio
async def test_async_lease_renewal_stop_is_idempotent() -> None:
    renewal = _async_test_client().run_lease_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails"))
    )

    renewal.start()
    renewal.stop()
    renewal.stop()


@pytest.mark.anyio
async def test_async_lease_renewal_noops_without_receipt_handle() -> None:
    metadata = MessageMetadata(
        message_id="m1",
        delivery_count=1,
        created_at=CREATED_AT_DT,
        topic="emails",
        consumer_group=SanitizedName("c"),
    )
    renewal = _async_test_client().run_lease_renewal(Message(payload=None, metadata=metadata))
    renewal.start()
    renewal.stop()
    await anyio.sleep(0.01)


def test_sync_lease_start_reuses_worker_async_client_by_config(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = eqs.get_sync_client(
        token="token",
        headers={"x-test": "sync-cache"},
        deployment=ALL_DEPLOYMENTS,
    )
    first_message = _sync_leased_message(
        client,
        visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    second_message = _sync_leased_message(
        client,
        visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    created = 0
    queue_httpx = queue_httpx_module()
    original_client = queue_httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> Any:
        nonlocal created
        created += 1
        return original_client(*args, **kwargs)

    monkeypatch.setattr(queue_httpx, "AsyncClient", client_factory)
    first = client.run_lease_renewal(first_message, lease_duration=30)
    second = client.run_lease_renewal(second_message, lease_duration=45)
    first.start()
    second.start()
    try:
        _wait_for_sync_lease_deadline(eqs, "msg_1", "c", 30)
        _wait_for_sync_lease_deadline(eqs, "msg_2", "c", 45)
    finally:
        first.stop()
        second.stop()

    assert created == 1
    assert eqs.state.by_id["msg_1"].lease_deadline_by_consumer["c"] == (
        eqs.state.now + timedelta(seconds=30)
    )
    assert eqs.state.by_id["msg_2"].lease_deadline_by_consumer["c"] == (
        eqs.state.now + timedelta(seconds=45)
    )


@pytest.mark.anyio
async def test_async_client_reuses_worker_runtime_for_background_lease_extensions(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = eqs.get_async_client(
        token="token",
        base_url=eqs.base_url,
        headers={"x-test": "async-cache"},
        deployment=ALL_DEPLOYMENTS,
    )
    first_message = await _async_leased_message(
        client,
        visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    second_message = await _async_leased_message(
        client,
        visibility_deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    created = 0
    queue_httpx = queue_httpx_module()
    original_client = queue_httpx.AsyncClient

    def client_factory(*args: Any, **kwargs: Any) -> Any:
        nonlocal created
        created += 1
        return original_client(*args, **kwargs)

    monkeypatch.setattr(queue_httpx, "AsyncClient", client_factory)

    first = client.run_lease_renewal(first_message, lease_duration=30)
    second = client.run_lease_renewal(second_message, lease_duration=45)
    first.start()
    second.start()
    try:
        await _wait_for_async_lease_deadline(eqs, "msg_1", "c", 30)
        await _wait_for_async_lease_deadline(eqs, "msg_2", "c", 45)
    finally:
        first.stop()
        second.stop()

    assert created == 1
    assert eqs.state.by_id["msg_1"].lease_deadline_by_consumer["c"] == (
        eqs.state.now + timedelta(seconds=30)
    )
    assert eqs.state.by_id["msg_2"].lease_deadline_by_consumer["c"] == (
        eqs.state.now + timedelta(seconds=45)
    )


@pytest.mark.anyio
async def test_retry_async_follow_up_retries_throttled_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    async def operation() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ThrottledError(2)

    await retry_async_follow_up(operation, sleep=sleep)

    assert calls == 2
    assert sleeps == [2.0]


@pytest.mark.anyio
async def test_retry_async_follow_up_does_not_retry_throttle_without_retry_after() -> None:
    calls = 0

    async def operation() -> None:
        nonlocal calls
        calls += 1
        raise ThrottledError

    with pytest.raises(ThrottledError):
        await retry_async_follow_up(operation)

    assert calls == 1


@pytest.mark.anyio
async def test_retry_async_follow_up_does_not_retry_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ServerError(QueueError):
        status_code = 500

    calls = 0
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    async def operation() -> None:
        nonlocal calls
        calls += 1
        raise ServerError("server error")

    with pytest.raises(ServerError):
        await retry_async_follow_up(operation, sleep=sleep)

    assert calls == 1
    assert sleeps == []


def test_retry_sync_follow_up_retries_transport_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    sleeps: list[float] = []

    def sleep(delay: float) -> None:
        sleeps.append(delay)

    def operation() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise CommunicationError("network")

    monkeypatch.setattr(time, "sleep", sleep)
    retry_sync_follow_up(operation)

    assert calls == 2
    assert sleeps == [0.1]


def test_retry_sync_follow_up_does_not_retry_server_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ServerError(QueueError):
        status_code = 500

    calls = 0
    sleeps: list[float] = []

    def sleep(delay: float) -> None:
        sleeps.append(delay)

    def operation() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ServerError("server error")

    monkeypatch.setattr(time, "sleep", sleep)
    with pytest.raises(ServerError):
        retry_sync_follow_up(operation)

    assert calls == 1
    assert sleeps == []


def test_retry_sync_follow_up_does_not_retry_throttle_without_retry_after() -> None:
    calls = 0

    def operation() -> None:
        nonlocal calls
        calls += 1
        raise ThrottledError

    with pytest.raises(ThrottledError):
        retry_sync_follow_up(operation)

    assert calls == 1


def test_retry_sync_follow_up_does_not_retry_programmer_error() -> None:
    calls = 0

    def operation() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("bug")

    with pytest.raises(RuntimeError, match="bug"):
        retry_sync_follow_up(operation)

    assert calls == 1


def test_lease_worker_ready_wait_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "vercel.queue._internal.lease._LEASE_WORKER_START_TIMEOUT_SECONDS",
        0.001,
    )
    _lease_worker_ready.clear()

    with pytest.raises(RuntimeError, match="lease extension worker did not start"):
        _wait_for_lease_worker_ready()


def test_lease_worker_ready_wait_propagates_startup_error() -> None:
    error = RuntimeError("boom")
    _lease_worker_state.startup_error = error
    _lease_worker_ready.set()
    try:
        with pytest.raises(
            RuntimeError,
            match="lease extension worker failed to start",
        ) as exc_info:
            _wait_for_lease_worker_ready()
    finally:
        _lease_worker_state.startup_error = None
        _lease_worker_ready.clear()

    assert exc_info.value.__cause__ is error


def test_lease_worker_startup_error_clears_started_state() -> None:
    error = RuntimeError("boom")
    _lease_worker_state.startup_error = error
    _lease_worker_state.started = True
    _lease_worker_ready.set()

    try:
        with pytest.raises(RuntimeError, match="lease extension worker failed to start"):
            _ensure_lease_renewal_thread()
    finally:
        _lease_worker_state.startup_error = None
        _lease_worker_ready.clear()

    assert not _lease_worker_state.started
    assert _lease_worker_state.command_send is None
    assert _lease_worker_state.worker_token is None


def test_lease_renewal_start_waits_for_worker_scheduling_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[_LeaseExtensionRequest] = []

    def start(request: _LeaseExtensionRequest) -> None:
        requests.append(request)

    monkeypatch.setattr("vercel.queue._internal.lease._ensure_lease_renewal_thread", lambda: None)
    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_start", start)
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )

    renewal.start()

    assert len(requests) == 1
    assert renewal._token == requests[0].token


def test_lease_renewal_start_timeout_resets_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def start(request: _LeaseExtensionRequest) -> None:
        del request
        raise TimeoutError("timed out waiting for queue lease renewal to start")

    monkeypatch.setattr("vercel.queue._internal.lease._ensure_lease_renewal_thread", lambda: None)
    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_start", start)
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )

    with pytest.raises(TimeoutError, match="timed out waiting"):
        renewal.start()

    assert renewal._token is None
    assert not renewal._entered


@pytest.mark.anyio
async def test_lease_renewal_start_async_cancellation_resets_and_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stops: list[_LeaseRenewalToken] = []
    started = anyio.Event()

    async def start(request: _LeaseExtensionRequest) -> None:
        del request
        started.set()
        await anyio.sleep_forever()

    def stop(token: _LeaseRenewalToken) -> None:
        stops.append(token)

    monkeypatch.setattr("vercel.queue._internal.lease._ensure_lease_renewal_thread", lambda: None)
    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_start_async", start)
    monkeypatch.setattr("vercel.queue._internal.lease._send_best_effort_start_timeout_stop", stop)
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(renewal.start_async)
        await started.wait()
        task_group.cancel_scope.cancel()

    assert len(stops) == 1
    assert renewal._token is None
    assert not renewal._entered


def test_lease_extension_start_timeout_sends_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[object] = []
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
    )

    def send(command: object) -> None:
        commands.append(command)

    monkeypatch.setattr("vercel.queue._internal.lease._LEASE_START_WAIT_TIMEOUT_SECONDS", 0.001)
    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_command", send)

    with pytest.raises(TimeoutError, match="timed out waiting"):
        _send_lease_extension_start(request)

    assert len(commands) == 2
    start = commands[0]
    stop = commands[1]
    assert isinstance(start, _LeaseExtensionStart)
    assert isinstance(stop, _LeaseExtensionStop)
    assert stop.token == request.token


@pytest.mark.anyio
async def test_lease_worker_stop_after_start_timeout_cancels_renewal() -> None:
    started = anyio.Event()
    cancelled = anyio.Event()
    calls = 0

    async def extend(message: Message[Any], duration: int) -> None:
        nonlocal calls
        del message, duration
        calls += 1
        started.set()
        try:
            await anyio.sleep_forever()
        except anyio.get_cancelled_exc_class():
            cancelled.set()
            raise

    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )
    command_send, command_receive = queue_lease_anyio_module().create_memory_object_stream(10)

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(_lease_worker_async, command_receive)
        await command_send.send(
            _LeaseExtensionStart(request=request, scheduled_event=threading.Event())
        )
        await started.wait()
        await command_send.send(_LeaseExtensionStop(token=request.token))
        with anyio.fail_after(0.5):
            await cancelled.wait()
        await anyio.sleep(0.01)
        await command_send.aclose()
        task_group.cancel_scope.cancel()

    assert calls == 1


@pytest.mark.anyio
async def test_lease_renewal_start_async_does_not_block_caller_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()
    started = threading.Event()

    async def start(request: _LeaseExtensionRequest) -> None:
        del request
        started.set()
        await to_thread.run_sync(release.wait, 1)

    monkeypatch.setattr("vercel.queue._internal.lease._ensure_lease_renewal_thread", lambda: None)
    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_start_async", start)
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(renewal.start_async)
        await to_thread.run_sync(started.wait, 1)
        with anyio.fail_after(0.1):
            await anyio.sleep(0.001)
        release.set()

    assert renewal._entered


@pytest.mark.anyio
async def test_send_lease_extension_start_waits_for_real_worker_schedule() -> None:
    command_send, command_receive = queue_lease_anyio_module().create_memory_object_stream[Any](10)
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
    )

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(_lease_worker_async, command_receive)
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(_lease_worker_state, "command_send", command_send)
            monkeypatch.setattr(
                _lease_worker_state,
                "worker_token",
                current_token(),
            )
            await to_thread.run_sync(_send_lease_extension_start, request)
        await command_send.send(_LeaseWorkerShutdown())


@pytest.mark.anyio
async def test_send_lease_extension_start_async_waits_for_real_worker_schedule() -> None:
    _ensure_lease_renewal_thread()
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
    )

    with anyio.fail_after(0.5):
        await _send_lease_extension_start_async(request)


def test_signal_lease_start_scheduled_uses_asyncio_native_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    callbacks: list[Callable[[], None]] = []
    fallback_calls = 0

    class NativeToken:
        def call_soon_threadsafe(self, callback: Callable[[], None]) -> None:
            callbacks.append(callback)

    class EventLoopToken:
        native_token = NativeToken()

    def fallback(*args: object, **kwargs: object) -> None:
        nonlocal fallback_calls
        del args, kwargs
        fallback_calls += 1

    scheduled_event = threading.Event()
    event_loop_token: Any = EventLoopToken()
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
    )
    monkeypatch.setattr(queue_lease_anyio_module().from_thread, "run_sync", fallback)

    _signal_lease_start_scheduled(
        _LeaseExtensionStart(
            request=request,
            scheduled_event=scheduled_event,
            event_loop_token=event_loop_token,
        )
    )

    assert len(callbacks) == 1
    assert fallback_calls == 0
    assert not scheduled_event.is_set()
    callbacks[0]()
    assert scheduled_event.is_set()


def test_signal_lease_start_scheduled_uses_trio_native_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    callbacks: list[Callable[[], None]] = []
    fallback_calls = 0

    class NativeToken:
        def run_sync_soon(self, callback: Callable[[], None]) -> None:
            callbacks.append(callback)

    class EventLoopToken:
        native_token = NativeToken()

    def fallback(*args: object, **kwargs: object) -> None:
        nonlocal fallback_calls
        del args, kwargs
        fallback_calls += 1

    scheduled_event = threading.Event()
    event_loop_token: Any = EventLoopToken()
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
    )
    monkeypatch.setattr(queue_lease_anyio_module().from_thread, "run_sync", fallback)

    _signal_lease_start_scheduled(
        _LeaseExtensionStart(
            request=request,
            scheduled_event=scheduled_event,
            event_loop_token=event_loop_token,
        )
    )

    assert len(callbacks) == 1
    assert fallback_calls == 0
    assert not scheduled_event.is_set()
    callbacks[0]()
    assert scheduled_event.is_set()


def test_signal_lease_start_scheduled_falls_back_to_anyio_from_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Callable[[], None], object]] = []

    class NativeToken:
        pass

    class EventLoopToken:
        native_token = NativeToken()

    def fallback(callback: Callable[[], None], *, token: object) -> None:
        calls.append((callback, token))
        callback()

    scheduled_event = threading.Event()
    event_loop_token: Any = EventLoopToken()
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
    )
    monkeypatch.setattr(queue_lease_anyio_module().from_thread, "run_sync", fallback)

    _signal_lease_start_scheduled(
        _LeaseExtensionStart(
            request=request,
            scheduled_event=scheduled_event,
            event_loop_token=event_loop_token,
        )
    )

    assert calls == [(scheduled_event.set, event_loop_token)]
    assert scheduled_event.is_set()


@pytest.mark.anyio
async def test_auto_lease_renewal_retries_throttled_extension() -> None:
    async def extend(message: Message[Any], duration: int) -> None:
        del message, duration
        raise ThrottledError

    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )
    active = {request.token: request}

    await _run_lease_extension(request, active)

    assert request.token in active
    assert request.next_extension_at > time.monotonic()


@pytest.mark.anyio
async def test_auto_lease_renewal_uses_throttle_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 100.0

    async def extend(message: Message[Any], duration: int) -> None:
        del message, duration
        raise ThrottledError(2)

    monkeypatch.setattr(time, "monotonic", lambda: now)
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=60,
        client=_FakeLeaseClient(extend),
    )
    active = {request.token: request}

    await _run_lease_extension(request, active)

    assert request.next_extension_at == pytest.approx(102.0)


@pytest.mark.anyio
async def test_auto_lease_renewal_floors_zero_throttle_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 100.0

    async def extend(message: Message[Any], duration: int) -> None:
        del message, duration
        raise ThrottledError(0)

    monkeypatch.setattr(time, "monotonic", lambda: now)
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=60,
        client=_FakeLeaseClient(extend),
    )
    active = {request.token: request}

    await _run_lease_extension(request, active)

    assert request.next_extension_at == pytest.approx(101.0)


@pytest.mark.anyio
async def test_auto_lease_renewal_caps_long_throttle_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 100.0

    async def extend(message: Message[Any], duration: int) -> None:
        del message, duration
        raise ThrottledError(999)

    monkeypatch.setattr(time, "monotonic", lambda: now)
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )
    active = {request.token: request}

    await _run_lease_extension(request, active)

    assert request.next_extension_at == pytest.approx(110.0)


@pytest.mark.anyio
async def test_lease_debug_logs_transient_retry(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")

    async def extend(message: Message[Any], duration: int) -> None:
        del message, duration
        raise ThrottledError

    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )
    active = {request.token: request}

    await _run_lease_extension(request, active)

    events = _queue_debug_events(caplog)
    assert [event["event"] for event in events] == [
        "lease.extension_attempt",
        "lease.transient_retry_scheduled",
    ]


@pytest.mark.anyio
async def test_lease_debug_logs_selected_throttle_retry_delay(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")

    async def extend(message: Message[Any], duration: int) -> None:
        del message, duration
        raise ThrottledError(999)

    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )
    active = {request.token: request}

    await _run_lease_extension(request, active)

    events = _queue_debug_events(caplog)
    assert events[-1]["event"] == "lease.transient_retry_scheduled"
    assert events[-1]["retry_delay_seconds"] == pytest.approx(10.0)


@pytest.mark.anyio
async def test_auto_lease_renewal_stops_on_client_error() -> None:
    class ClientError(QueueError):
        status_code = 404

    async def extend(message: Message[Any], duration: int) -> None:
        del message, duration
        raise ClientError("not found")

    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )
    active = {request.token: request}
    await _run_lease_extension(request, active)

    assert request.token not in active


@pytest.mark.anyio
async def test_auto_lease_renewal_retries_request_timeout() -> None:
    class RequestTimeoutError(QueueError):
        status_code = 408

    async def extend(message: Message[Any], duration: int) -> None:
        del message, duration
        raise RequestTimeoutError("request timeout")

    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )
    active = {request.token: request}
    await _run_lease_extension(request, active)

    assert request.token in active
    assert request.next_extension_at > time.monotonic()


@pytest.mark.anyio
async def test_lease_debug_logs_client_error_stop(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "true")
    caplog.set_level(logging.INFO, logger="vercel.queue")

    class ClientError(QueueError):
        status_code = 404

    async def extend(message: Message[Any], duration: int) -> None:
        del message, duration
        raise ClientError("not found")

    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )
    active = {request.token: request}
    await _run_lease_extension(request, active)

    events = _queue_debug_events(caplog)
    assert [event["event"] for event in events] == [
        "lease.extension_attempt",
        "lease.client_error_stop",
    ]


@pytest.mark.anyio
async def test_lease_worker_processes_stop_while_extension_is_in_flight() -> None:
    started = anyio.Event()
    cancelled = anyio.Event()
    after_cancel_release = anyio.Event()

    async def extend(message: Message[Any], duration: int) -> None:
        del message, duration
        started.set()
        try:
            await anyio.sleep_forever()
        except anyio.get_cancelled_exc_class():
            cancelled.set()
            with anyio.CancelScope(shield=True):
                await after_cancel_release.wait()
            raise

    queue_lease_anyio = queue_lease_anyio_module()
    command_send, command_receive = queue_lease_anyio.create_memory_object_stream(10)
    done_send, done_receive = queue_lease_anyio.create_memory_object_stream[None](1)
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(_lease_worker_async, command_receive)
        await command_send.send(
            _LeaseExtensionStart(request=request, scheduled_event=threading.Event())
        )
        await started.wait()
        await command_send.send(_LeaseExtensionStop(token=request.token, done_send=done_send))
        await cancelled.wait()

        with anyio.move_on_after(0.05) as scope:
            await done_receive.receive()
        assert scope.cancel_called

        after_cancel_release.set()

        with anyio.fail_after(0.5):
            await done_receive.receive()

        await command_send.aclose()
        task_group.cancel_scope.cancel()

    await done_send.aclose()
    await done_receive.aclose()


def test_lease_renewal_stop_times_out_when_extension_cancellation_stalls(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    stops: list[object] = []

    def stop(token: object) -> bool:
        stops.append(token)
        return False

    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_stop", stop)
    caplog.set_level(logging.WARNING, logger="vercel.queue._internal.lease")
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )
    renewal._token = _LeaseRenewalToken(object())

    renewal.stop()

    assert len(stops) == 1
    assert "timed out waiting for queue lease renewal to stop" in caplog.text


def test_lease_renewal_stop_can_retry_after_send_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def stop(token: object) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("worker unavailable")
        return True

    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_stop", stop)
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )
    token = _LeaseRenewalToken(object())
    renewal._token = token

    with pytest.raises(RuntimeError, match="worker unavailable"):
        renewal.stop()

    assert renewal._token == token
    assert not renewal._closed
    assert not renewal._closing

    renewal.stop()

    assert calls == 2
    assert renewal._token is None
    assert renewal._closed
    assert not renewal._closing


def test_lease_renewal_best_effort_stop_ignores_send_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def send(command: object) -> None:
        del command
        raise RuntimeError("worker unavailable")

    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_command", send)
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )
    renewal._token = _LeaseRenewalToken(object())

    renewal.stop(wait=False)

    assert renewal._token is None
    assert renewal._closed
    assert not renewal._closing


def test_lease_renewal_stop_is_reentrant_while_closing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )
    renewal._token = _LeaseRenewalToken(object())

    def stop(token: object) -> bool:
        nonlocal calls
        del token
        calls += 1
        renewal.stop()
        return True

    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_stop", stop)

    renewal.stop()

    assert calls == 1
    assert renewal._closed
    assert not renewal._closing


@pytest.mark.anyio
async def test_async_message_lifecycle_uses_async_lease_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    starts = 0

    async def start_async(self: LeaseRenewal) -> None:
        nonlocal starts
        starts += 1

    def enter(self: LeaseRenewal) -> LeaseRenewal:
        del self
        raise AssertionError("async lifecycle must not use sync lease enter")

    monkeypatch.setattr(LeaseRenewal, "start_async", start_async)
    monkeypatch.setattr(LeaseRenewal, "__enter__", enter)
    client = _async_test_client()
    message = Message(payload=None, metadata=make_leased_metadata("emails"))

    async with _AsyncMessageLifecycle(message, client=client, lease_duration=30):
        raise Handoff

    assert starts == 1


@pytest.mark.anyio
async def test_lease_renewal_stop_async_can_retry_after_send_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def stop(token: object) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("worker unavailable")
        return True

    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_stop_async", stop)
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )
    token = _LeaseRenewalToken(object())
    renewal._token = token

    with pytest.raises(RuntimeError, match="worker unavailable"):
        await renewal.stop_async()

    assert renewal._token == token
    assert not renewal._closed
    assert not renewal._closing

    await renewal.stop_async()

    assert calls == 2
    assert renewal._token is None
    assert renewal._closed
    assert not renewal._closing


@pytest.mark.anyio
async def test_lease_renewal_stop_async_can_retry_after_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = anyio.Event()

    async def stop(token: object) -> bool:
        del token
        started.set()
        await anyio.sleep_forever()
        return True

    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_stop_async", stop)
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )
    token = _LeaseRenewalToken(object())
    renewal._token = token

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(renewal.stop_async)
        await started.wait()
        task_group.cancel_scope.cancel()

    assert renewal._token == token
    assert not renewal._closed
    assert not renewal._closing

    async def stop_success(token: object) -> bool:
        del token
        return True

    monkeypatch.setattr(
        "vercel.queue._internal.lease._send_lease_extension_stop_async",
        stop_success,
    )

    await renewal.stop_async()

    assert renewal._token is None
    assert renewal._closed
    assert not renewal._closing


@pytest.mark.anyio
async def test_lease_renewal_best_effort_stop_async_ignores_send_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def send(command: object) -> None:
        del command
        raise RuntimeError("worker unavailable")

    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_command", send)
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )
    renewal._token = _LeaseRenewalToken(object())

    await renewal.stop_async(wait=False)

    assert renewal._token is None
    assert renewal._closed
    assert not renewal._closing


@pytest.mark.anyio
async def test_lease_renewal_stop_async_is_reentrant_while_closing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    renewal = _test_renewal(
        Message(payload=None, metadata=make_leased_metadata("emails")),
    )
    renewal._token = _LeaseRenewalToken(object())

    async def stop(token: object) -> bool:
        nonlocal calls
        del token
        calls += 1
        await renewal.stop_async()
        return True

    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_extension_stop_async", stop)

    await renewal.stop_async()

    assert calls == 1
    assert renewal._closed
    assert not renewal._closing


@pytest.mark.anyio
async def test_signal_lease_stop_complete_ignores_failed_waiter_signal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class BrokenEvent:
        def set(self) -> None:
            raise RuntimeError("event loop closed")

    caplog.set_level(logging.DEBUG, logger="vercel.queue._internal.lease")

    await _signal_lease_stop_complete(
        _LeaseExtensionStop(
            token=_LeaseRenewalToken(object()),
            done_event=BrokenEvent(),
        )
    )

    assert "failed to signal queue lease renewal stop completion" in caplog.text


@pytest.mark.anyio
async def test_signal_lease_stop_complete_failed_send_does_not_block_event_signal() -> None:
    class Event:
        def __init__(self) -> None:
            self.signaled = False

        def set(self) -> None:
            self.signaled = True

    done_send, done_receive = queue_lease_anyio_module().create_memory_object_stream[None](1)
    await done_send.aclose()
    await done_receive.aclose()
    event = Event()

    await _signal_lease_stop_complete(
        _LeaseExtensionStop(
            token=_LeaseRenewalToken(object()),
            done_send=done_send,
            done_event=event,
        )
    )

    assert event.signaled


@pytest.mark.anyio
async def test_send_lease_extension_stop_async_waits_for_real_worker_signal() -> None:
    _ensure_lease_renewal_thread()
    token = _LeaseRenewalToken(object())

    with anyio.fail_after(0.5):
        stopped = await _send_lease_extension_stop_async(token)

    assert stopped is True


@pytest.mark.anyio
async def test_send_lease_extension_stop_async_does_not_wait_in_worker_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[_LeaseExtensionStop] = []
    command_sent = anyio.Event()
    stop_done = anyio.Event()
    waiter_started = anyio.Event()
    event_loop_token = current_token()
    release_waiter = threading.Event()

    def send(command: object) -> None:
        assert isinstance(command, _LeaseExtensionStop)
        commands.append(command)
        anyio.from_thread.run_sync(command_sent.set, token=event_loop_token)

    def blocking_worker_probe() -> None:
        anyio.from_thread.run_sync(waiter_started.set, token=event_loop_token)
        release_waiter.wait(timeout=1)

    async def stop() -> None:
        assert await _send_lease_extension_stop_async(_LeaseRenewalToken(object()))
        stop_done.set()

    monkeypatch.setattr("vercel.queue._internal.lease._send_lease_command", send)
    monkeypatch.setattr("vercel.queue._internal.lease._LEASE_STOP_WAIT_TIMEOUT_SECONDS", 1.0)
    monkeypatch.setattr(to_thread.current_default_thread_limiter(), "total_tokens", 1)

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(stop)
        with anyio.fail_after(0.5):
            await command_sent.wait()

        task_group.start_soon(to_thread.run_sync, blocking_worker_probe)
        with anyio.fail_after(0.5):
            await waiter_started.wait()

        assert commands[0].done_event is not None
        commands[0].done_event.set()
        release_waiter.set()
        with anyio.fail_after(0.5):
            await stop_done.wait()
        task_group.cancel_scope.cancel()


@pytest.mark.anyio
async def test_lease_worker_ignores_registration_after_stop() -> None:
    calls = 0

    async def extend(message: Message[Any], duration: int) -> None:
        nonlocal calls
        del message, duration
        calls += 1

    queue_lease_anyio = queue_lease_anyio_module()
    command_send, command_receive = queue_lease_anyio.create_memory_object_stream(10)
    done_send, done_receive = queue_lease_anyio.create_memory_object_stream[None](1)
    request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(_lease_worker_async, command_receive)
        await command_send.send(_LeaseExtensionStop(token=request.token, done_send=done_send))
        with anyio.fail_after(0.5):
            await done_receive.receive()
        await command_send.send(
            _LeaseExtensionStart(request=request, scheduled_event=threading.Event())
        )
        await anyio.sleep(0.05)
        await command_send.aclose()
        task_group.cancel_scope.cancel()

    await done_send.aclose()
    await done_receive.aclose()

    assert calls == 0


@pytest.mark.anyio
async def test_lease_worker_bounds_stopped_token_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("vercel.queue._internal.lease._STOPPED_LEASE_TOKEN_CACHE_SIZE", 2)
    first = _LeaseRenewalToken(object())
    second = _LeaseRenewalToken(object())
    third = _LeaseRenewalToken(object())
    state = _LeaseWorkerRuntimeState(
        active={},
        stopped=OrderedDict(),
    )

    await _handle_lease_extension_stop(_LeaseExtensionStop(token=first), state)
    await _handle_lease_extension_stop(_LeaseExtensionStop(token=second), state)
    await _handle_lease_extension_stop(_LeaseExtensionStop(token=third), state)

    assert list(state.stopped) == [second, third]

    calls = 0

    async def extend(message: Message[Any], duration: int) -> None:
        nonlocal calls
        del message, duration
        calls += 1

    second_request = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails", message_id="second")),
        lease_seconds=30,
        client=_FakeLeaseClient(extend),
    )
    second_request.token = second

    command_send, command_receive = queue_lease_anyio_module().create_memory_object_stream(10)
    async with anyio.create_task_group() as task_group:
        task_group.start_soon(_lease_worker_async, command_receive)
        await command_send.send(_LeaseExtensionStop(token=second))
        await command_send.send(
            _LeaseExtensionStart(request=second_request, scheduled_event=threading.Event())
        )
        await anyio.sleep(0.05)
        await command_send.aclose()
        task_group.cancel_scope.cancel()

    assert calls == 0


@pytest.mark.anyio
async def test_lease_worker_one_renewal_in_flight_does_not_block_another_token() -> None:
    calls: list[str] = []
    first_started = anyio.Event()
    second_started = anyio.Event()
    release_first = anyio.Event()

    async def extend(message: Message[Any], duration: int) -> None:
        del duration
        calls.append(message.metadata.message_id)
        if message.metadata.message_id == "first":
            first_started.set()
            await release_first.wait()
        if message.metadata.message_id == "second":
            second_started.set()

    command_send, command_receive = queue_lease_anyio_module().create_memory_object_stream(10)
    client = _FakeLeaseClient(extend)
    first = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails", message_id="first")),
        lease_seconds=30,
        client=client,
    )
    second = _lease_request(
        Message(payload=None, metadata=make_leased_metadata("emails", message_id="second")),
        lease_seconds=30,
        client=client,
    )

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(_lease_worker_async, command_receive)
        await command_send.send(
            _LeaseExtensionStart(request=first, scheduled_event=threading.Event())
        )
        await first_started.wait()
        await command_send.send(
            _LeaseExtensionStart(request=second, scheduled_event=threading.Event())
        )

        with anyio.fail_after(0.5):
            await second_started.wait()

        release_first.set()
        await command_send.aclose()
        task_group.cancel_scope.cancel()

    assert calls[:2] == ["first", "second"]


def _sync_leased_message(
    client: SyncQueueClient,
    *,
    visibility_deadline: datetime,
) -> Message[Any]:
    client.send("emails", {"ok": True})
    delivery: Delivery[Any] = next(client.poll("emails", "c", lease_duration=10))
    message = delivery.message
    return Message(
        payload=message.payload,
        metadata=replace(message.metadata, visibility_deadline=visibility_deadline),
    )


def _lease_request(
    message: Message[Any],
    *,
    lease_seconds: int,
    client: _FakeLeaseClient | None = None,
) -> _LeaseExtensionRequest:
    if client is None:
        client = _FakeLeaseClient(lambda _message, _duration: None)
    return _LeaseExtensionRequest(
        token=_LeaseRenewalToken(object()),
        message=message,
        client=client,
        lease_seconds=lease_seconds,
        next_extension_at=time.monotonic(),
    )


async def _async_leased_message(
    client: QueueClient,
    *,
    visibility_deadline: datetime,
) -> Message[Any]:
    await client.send("emails", {"ok": True})
    stream: AsyncIterator[Delivery[Any]] = client.poll("emails", "c", lease_duration=10)
    try:
        delivery = await anext(stream)
    except StopAsyncIteration:
        raise AssertionError("expected leased message") from None
    message = delivery.message
    return Message(
        payload=message.payload,
        metadata=replace(message.metadata, visibility_deadline=visibility_deadline),
    )


def _wait_for_sync_lease_deadline(
    server: EmbeddedQueueDevServer,
    message_id: str,
    consumer: str,
    seconds: int,
    *,
    condition: Callable[[], bool] | None = None,
) -> None:
    expected = server.state.now + timedelta(seconds=seconds)
    deadline = time.monotonic() + 1
    while server.state.by_id[message_id].lease_deadline_by_consumer[consumer] != expected or (
        condition is not None and not condition()
    ):
        if time.monotonic() >= deadline:
            raise AssertionError("lease deadline was not extended")
        time.sleep(0.001)


async def _wait_for_async_lease_deadline(
    server: EmbeddedQueueDevServer,
    message_id: str,
    consumer: str,
    seconds: int,
) -> None:
    await to_thread.run_sync(
        _wait_for_sync_lease_deadline,
        server,
        message_id,
        consumer,
        seconds,
    )
