"""VQS server emulation for embedded/local use."""

# ruff: noqa: S107  # hardcoded-password-default -- dummy token

from __future__ import annotations

from typing import Any, Literal, NamedTuple, Protocol

import base64
import heapq
import json
import math
import re
import sys
import threading
import time
import weakref
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Mapping, MutableMapping
from contextlib import (
    AbstractAsyncContextManager,
    AbstractContextManager,
    asynccontextmanager,
    contextmanager,
)
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from itertools import count
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import unquote

import anyio
import httpx
from anyio.abc import TaskGroup

from .client import QueueClient
from .client_sync import QueueClient as SyncQueueClient
from .config import ALL_DEPLOYMENTS, BaseUrl, DeploymentOption
from .constants import (
    CLOUD_EVENT_HEADER_TYPE,
    CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP,
    CLOUD_EVENT_HEADER_VQS_CREATED_AT,
    CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT,
    CLOUD_EVENT_HEADER_VQS_EXPIRES_AT,
    CLOUD_EVENT_HEADER_VQS_MESSAGE_ID,
    CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE,
    CLOUD_EVENT_HEADER_VQS_REGION,
    CLOUD_EVENT_HEADER_VQS_TOPIC,
    CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE,
    CLOUD_EVENT_TYPE_V2BETA,
    CONTENT_TYPE_MULTIPART_MIXED,
    CONTENT_TYPE_NDJSON,
    DEFAULT_RETRY_AFTER_SECONDS,
    HEADER_ACCEPT,
    HEADER_CONTENT_TYPE,
    VQS_HEADER_DELAY_SECONDS,
    VQS_HEADER_DELIVERY_COUNT,
    VQS_HEADER_DEPLOYMENT_ID,
    VQS_HEADER_EXPIRES_AT,
    VQS_HEADER_IDEMPOTENCY_KEY,
    VQS_HEADER_MAX_MESSAGES,
    VQS_HEADER_MESSAGE_ID,
    VQS_HEADER_RECEIPT_HANDLE,
    VQS_HEADER_RETENTION_SECONDS,
    VQS_HEADER_TIMESTAMP,
    VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS,
    VQS_NAME_PATTERN,
)
from .http import BaseQueueRuntime
from .log import debug_log
from .names import validate_name, validate_topic_name
from .subscribers import register_embedded_dispatcher, unregister_embedded_dispatcher
from .types import Duration

DEFAULT_DEPLOYMENT_PARTITION = "__all__"
DEFAULT_PULL_LEASE_SECONDS = 60
DEFAULT_PUSH_LEASE_SECONDS = 300
DEFAULT_RETENTION_SECONDS = 86_400
MAX_RETENTION_SECONDS = 604_800
MIN_RETENTION_SECONDS = 60
MAX_VISIBILITY_TIMEOUT_SECONDS = 3_600
PAYLOAD_SPILL_THRESHOLD_BYTES = 256 * 1000
BOUNDARY = "vqs-test-boundary"
QUEUE_PATH_PREFIX = ("api", "v3", "topic")
JSON_RESPONSE_HEADERS = [(b"content-type", b"application/json")]
QueueRouteAction = Literal["topic", "consumer", "message_id", "lease", "lease_visibility"]
ReceiveResponseFormat = Literal["multipart", "ndjson"]
SubscriptionMatchKind = Literal["exact", "prefix", "wildcard"]
DEPLOYMENT_ID_PATTERN = re.compile(VQS_NAME_PATTERN)


class EmbeddedQueueClock(Protocol):
    def now(self) -> datetime: ...

    def monotonic(self) -> float: ...


class EmbeddedQueuePushClient(Protocol):
    async def accept_and_handle(
        self,
        raw_body: bytes,
        headers: Mapping[str, str],
    ) -> None: ...


@dataclass
class RealEmbeddedQueueClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def monotonic(self) -> float:
        return time.monotonic()


@dataclass
class ManualEmbeddedQueueClock:
    current: datetime = field(default_factory=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc))

    def now(self) -> datetime:
        return self.current

    def monotonic(self) -> float:
        return self.current.timestamp()

    def shift(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


@dataclass(frozen=True)
class PushDelivery:
    body: bytes
    headers: dict[str, str]


@dataclass(frozen=True)
class _QueueRoute:
    action: QueueRouteAction
    topic: str
    consumer: str | None = None
    message_id: str | None = None
    receipt_handle: str | None = None


@dataclass(kw_only=True)
class _DispatcherRegistration:
    topic_pattern: str
    match_kind: SubscriptionMatchKind
    consumer_group: str
    retry_after_seconds: int | None
    initial_delay_seconds: int | None
    max_concurrency: int | None
    max_attempts: int | None
    registered_at: datetime
    concurrency_limit: int | None = None
    semaphore: anyio.Semaphore | None = None

    def key(self) -> tuple[str, SubscriptionMatchKind, str]:
        return self.topic_pattern, self.match_kind, self.consumer_group

    def matches_topic(self, topic: str) -> bool:
        if self.match_kind == "wildcard":
            return True
        if self.match_kind == "prefix":
            return topic.startswith(self.topic_pattern)
        return topic == self.topic_pattern


@dataclass(frozen=True)
class EmbeddedQueueRequest:
    method: str
    path: str
    headers: httpx.Headers
    body: bytes


@dataclass(frozen=True)
class EmbeddedQueueResponse:
    method: str
    action: QueueRouteAction
    status_code: int
    body: bytes = b""
    headers: tuple[tuple[bytes, bytes], ...] = ()


@dataclass
class StoredPayload:
    data: bytes | None = None
    path: Path | None = None

    @property
    def size(self) -> int:
        if self.data is not None:
            return len(self.data)
        if self.path is None:
            return 0
        return self.path.stat().st_size

    @property
    def spilled(self) -> bool:
        return self.path is not None

    def read(self) -> bytes:
        if self.data is not None:
            return self.data
        if self.path is None:
            return b""
        return self.path.read_bytes()

    def delete(self) -> None:
        if self.path is not None:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            self.path = None


@dataclass
class StoredMessage:
    topic: str
    message_id: str
    payload: StoredPayload
    content_type: str
    deployment: str
    idempotency_key: str | None
    retention_seconds: int
    delay_seconds: int
    created_at: datetime
    available_at: datetime
    expires_at: datetime
    duplicate_of: str | None = None
    acknowledged_by_consumer: set[str] = field(default_factory=set)
    delivery_count_by_consumer: dict[str, int] = field(default_factory=dict)
    leases_by_consumer: dict[str, str] = field(default_factory=dict)
    lease_deadline_by_consumer: dict[str, datetime] = field(default_factory=dict)

    @property
    def acknowledged(self) -> bool:
        return bool(self.acknowledged_by_consumer)

    @acknowledged.setter
    def acknowledged(self, value: bool) -> None:
        if value:
            self.acknowledged_by_consumer.add(DEFAULT_DEPLOYMENT_PARTITION)
        else:
            self.acknowledged_by_consumer.clear()

    def acknowledged_for(self, consumer: str) -> bool:
        return (
            consumer in self.acknowledged_by_consumer
            or DEFAULT_DEPLOYMENT_PARTITION in self.acknowledged_by_consumer
        )

    def acknowledge_for(self, consumer: str) -> None:
        self.acknowledged_by_consumer.add(consumer)


@dataclass(frozen=True)
class _LeasedMessage:
    message: StoredMessage
    receipt_handle: str
    consumer: str


class _PushIndexKey(NamedTuple):
    topic: str
    consumer: str
    deployment: str | None


@dataclass
class _PushDeliveryIndex:
    cursor: int = 0
    ready: deque[str] = field(default_factory=deque)
    blocked: list[tuple[datetime, int, str]] = field(default_factory=list)


@dataclass
class EmbeddedQueueState:
    clock: EmbeddedQueueClock = field(default_factory=RealEmbeddedQueueClock)
    messages: list[StoredMessage] = field(default_factory=list)
    by_topic: dict[str, list[StoredMessage]] = field(default_factory=dict)
    by_id: dict[str, StoredMessage] = field(default_factory=dict)
    idempotency: dict[tuple[str, str, str, bytes], str] = field(default_factory=dict)
    by_receipt: dict[str, StoredMessage] = field(default_factory=dict)
    push_delivery_indexes: dict[_PushIndexKey, _PushDeliveryIndex] = field(default_factory=dict)
    push_blocked_sequence: count[int] = field(default_factory=count)
    next_expires_at: datetime | None = None
    requests: list[EmbeddedQueueRequest] = field(default_factory=list)
    responses: list[EmbeddedQueueResponse] = field(default_factory=list)
    ids: count[int] = field(default_factory=lambda: count(1))
    receipts: count[int] = field(default_factory=lambda: count(1))
    payload_spill_threshold_bytes: int = PAYLOAD_SPILL_THRESHOLD_BYTES
    _payload_directory: TemporaryDirectory[str] = field(
        default_factory=lambda: TemporaryDirectory(prefix="vercel-queue-payloads-"),
        init=False,
        repr=False,
    )

    @property
    def payload_directory(self) -> Path:
        return Path(self._payload_directory.name)

    def store_payload(self, message_id: str, payload: bytes) -> StoredPayload:
        if len(payload) <= self.payload_spill_threshold_bytes:
            return StoredPayload(data=payload)
        path = self.payload_directory / f"{message_id}.body"
        path.write_bytes(payload)
        return StoredPayload(path=path)

    def clear_payloads(self) -> None:
        for message in self.messages:
            message.payload.delete()

    def reset(self) -> None:
        """Reset queue contents and request history for tests."""
        self.clear_payloads()
        self.messages.clear()
        self.by_topic.clear()
        self.by_id.clear()
        self.idempotency.clear()
        self.by_receipt.clear()
        self.push_delivery_indexes.clear()
        self.push_blocked_sequence = count()
        self.next_expires_at = None
        self.requests.clear()
        self.responses.clear()
        self.ids = count(1)
        self.receipts = count(1)
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def close(self) -> None:
        self.clear_payloads()
        self._payload_directory.cleanup()

    def shift(self, seconds: float) -> None:
        shift = getattr(self.clock, "shift", None)
        if shift is None:
            raise RuntimeError("broker clock does not support manual shifting")
        shift(seconds)

    @property
    def now(self) -> datetime:
        return self.clock.now()

    @now.setter
    def now(self, value: datetime) -> None:
        self.clock = ManualEmbeddedQueueClock(value)


@dataclass
class EmbeddedQueueServer:
    state: EmbeddedQueueState = field(default_factory=EmbeddedQueueState)
    _wake_callbacks: list[weakref.ReferenceType[Callable[[], None]]] = field(
        default_factory=list,
        init=False,
        repr=False,
    )

    def add_wake_callback(self, callback: Callable[[], None]) -> None:
        self._wake_callbacks[:] = [ref for ref in self._wake_callbacks if ref() is not None]
        if any(ref() is callback for ref in self._wake_callbacks):
            return
        callback_ref: weakref.ReferenceType[Callable[[], None]]
        try:
            callback_ref = weakref.WeakMethod(callback)  # type: ignore[arg-type]
        except TypeError:
            callback_ref = weakref.ref(callback)
        self._wake_callbacks.append(callback_ref)

    def shift(self, seconds: float) -> None:
        self.state.shift(seconds)

    @property
    def now(self) -> datetime:
        return self.state.now

    @now.setter
    def now(self, value: datetime) -> None:
        self.state.now = value

    def put(self, topic: str, payload: bytes, headers: Mapping[str, str]) -> StoredMessage:
        deployment = headers.get(VQS_HEADER_DEPLOYMENT_ID, DEFAULT_DEPLOYMENT_PARTITION)
        if deployment != DEFAULT_DEPLOYMENT_PARTITION:
            _validate_deployment_id(deployment)
        idempotency_key = headers.get(VQS_HEADER_IDEMPOTENCY_KEY)
        content_type = headers.get(HEADER_CONTENT_TYPE, "application/octet-stream")
        retention_seconds = _int_header(
            headers.get(VQS_HEADER_RETENTION_SECONDS),
            DEFAULT_RETENTION_SECONDS,
            minimum=MIN_RETENTION_SECONDS,
            maximum=MAX_RETENTION_SECONDS,
            name=VQS_HEADER_RETENTION_SECONDS,
        )
        delay_seconds = _int_header(
            headers.get(VQS_HEADER_DELAY_SECONDS),
            0,
            minimum=0,
            maximum=sys.maxsize,
            name=VQS_HEADER_DELAY_SECONDS,
        )
        if delay_seconds > retention_seconds:
            raise ValueError("Vqs-Delay-Seconds cannot exceed retention time")
        state = self.state
        created_at = state.now
        message_id = f"msg_{next(state.ids)}"
        duplicate_of: str | None = None
        if idempotency_key:
            key = (topic, deployment, idempotency_key, payload)
            duplicate_of = state.idempotency.get(key)
            state.idempotency[key] = duplicate_of or message_id
        message = StoredMessage(
            topic=topic,
            message_id=message_id,
            payload=state.store_payload(message_id, payload),
            content_type=content_type,
            deployment=deployment,
            idempotency_key=idempotency_key,
            retention_seconds=retention_seconds,
            delay_seconds=delay_seconds,
            created_at=created_at,
            available_at=created_at + timedelta(seconds=delay_seconds),
            expires_at=created_at + timedelta(seconds=retention_seconds),
            duplicate_of=duplicate_of,
        )
        state.messages.append(message)
        state.by_topic.setdefault(topic, []).append(message)
        state.by_id[message_id] = message
        if state.next_expires_at is None or message.expires_at < state.next_expires_at:
            state.next_expires_at = message.expires_at
        self.wake_dispatchers()
        return message

    def respond_once(
        self,
        *,
        method: str,
        action: QueueRouteAction,
        status_code: int,
        body: bytes = b"",
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.state.responses.append(
            EmbeddedQueueResponse(
                method=method.upper(),
                action=action,
                status_code=status_code,
                body=body,
                headers=tuple(
                    (key.encode("latin-1"), value.encode("latin-1"))
                    for key, value in (headers or {}).items()
                ),
            )
        )

    def response_for(self, request: _AsgiRequest, route: _QueueRoute) -> _AsgiResponse | None:
        for index, response in enumerate(self.state.responses):
            if response.method == request.method and response.action == route.action:
                self.state.responses.pop(index)
                return _AsgiResponse(response.status_code, response.body, list(response.headers))
        return None

    async def record_request(self, request: _AsgiRequest) -> None:
        self.state.requests.append(
            EmbeddedQueueRequest(
                method=request.method,
                path=request.path,
                headers=httpx.Headers(request.headers),
                body=await request.body(),
            )
        )

    def wake_dispatchers(self) -> None:
        live: list[weakref.ReferenceType[Callable[[], None]]] = []
        for ref in self._wake_callbacks:
            callback = ref()
            if callback is None:
                continue
            live.append(ref)
            callback()
        self._wake_callbacks[:] = live

    def visible_messages(
        self,
        topic: str,
        consumer: str,
        deployment: str | None,
    ) -> list[StoredMessage]:
        self.cleanup_expired()
        return [
            message
            for message in self.state.by_topic.get(topic, [])
            if self._can_deliver(message, topic, consumer, deployment)
        ]

    def next_push_message(
        self,
        topic: str,
        consumer: str,
        deployment: str | None,
    ) -> StoredMessage | None:
        self.cleanup_expired()
        now = self.state.now
        index = self._push_delivery_index(topic, consumer, deployment)
        self._push_index_messages(index, topic)
        self._push_ready_blocked(index, now)
        while index.ready:
            message_id = index.ready.popleft()
            message = self.state.by_id.get(message_id)
            if message is None:
                continue
            deadline = self._message_visible_deadline_for_consumer(
                message,
                topic,
                consumer,
                deployment,
                now,
            )
            if deadline is None:
                continue
            if deadline > now:
                self._push_block_after(index, deadline, message.message_id)
                continue
            return message
        return None

    def find_by_id(self, topic: str, message_id: str) -> StoredMessage | None:
        self.cleanup_expired()
        message = self.state.by_id.get(message_id)
        if message is None or message.topic != topic:
            return None
        return message

    def lease(self, message: StoredMessage, consumer: str, seconds: int) -> str:
        receipt_handle = (
            f"rh_{next(self.state.receipts)}:{message.message_id}:{consumer} /needs encoding"
        )
        message.delivery_count_by_consumer[consumer] = (
            message.delivery_count_by_consumer.get(consumer, 0) + 1
        )
        previous_receipt = message.leases_by_consumer.get(consumer)
        if previous_receipt is not None:
            self.state.by_receipt.pop(previous_receipt, None)
        message.leases_by_consumer[consumer] = receipt_handle
        self.state.by_receipt[receipt_handle] = message
        message.lease_deadline_by_consumer[consumer] = self.now + timedelta(seconds=seconds)
        return receipt_handle

    def receipt_message(
        self,
        topic: str,
        consumer: str,
        receipt_handle: str,
        deployment: str | None,
    ) -> StoredMessage | None:
        self.cleanup_expired()
        message = self.state.by_receipt.get(receipt_handle)
        if message is None:
            return None
        if message.topic != topic or message.leases_by_consumer.get(consumer) != receipt_handle:
            return None
        if deployment is not None and message.deployment != deployment:
            return None
        return message

    def receipt_exists(
        self,
        topic: str,
        receipt_handle: str,
        deployment: str | None,
    ) -> bool:
        if (message := self.state.by_receipt.get(receipt_handle)) is not None:
            return message.topic == topic and (
                deployment is None or message.deployment == deployment
            )
        receipt_message_id = _message_id_from_receipt_handle(receipt_handle)
        if receipt_message_id is None:
            return False
        message = self.state.by_id.get(receipt_message_id)
        return (
            message is not None
            and message.topic == topic
            and (deployment is None or message.deployment == deployment)
        )

    def push_delivery(
        self,
        topic: str,
        consumer: str,
        *,
        lease_seconds: int = DEFAULT_PUSH_LEASE_SECONDS,
        deployment: str | None = None,
        region: str = "iad1",
    ) -> PushDelivery | None:
        message = self.next_push_message(topic, consumer, deployment)
        if message is None:
            return None
        receipt_handle = self.lease(message, consumer, lease_seconds)
        visibility_deadline = message.lease_deadline_by_consumer[consumer]
        self.requeue_push_message(message, consumer, visibility_deadline)
        return PushDelivery(
            body=message.payload.read(),
            headers={
                CLOUD_EVENT_HEADER_TYPE: CLOUD_EVENT_TYPE_V2BETA,
                CLOUD_EVENT_HEADER_VQS_TOPIC: message.topic,
                CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP: consumer,
                CLOUD_EVENT_HEADER_VQS_MESSAGE_ID: message.message_id,
                CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE: receipt_handle,
                CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT: str(
                    message.delivery_count_by_consumer[consumer]
                ),
                CLOUD_EVENT_HEADER_VQS_CREATED_AT: _format_dt(message.created_at),
                CLOUD_EVENT_HEADER_VQS_EXPIRES_AT: _format_dt(message.expires_at),
                CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE: _format_dt(visibility_deadline),
                CLOUD_EVENT_HEADER_VQS_REGION: region,
                HEADER_CONTENT_TYPE: message.content_type,
            },
        )

    def next_visible_delay(self, topics: Iterable[str] | None = None) -> float | None:
        self.cleanup_expired()
        topic_set = set(topics) if topics is not None else None
        next_deadline: datetime | None = None
        now = self.state.now
        for message in self.state.messages:
            if message.duplicate_of is not None:
                continue
            if topic_set is not None and message.topic not in topic_set:
                continue
            if message.expires_at <= now:
                continue
            if next_deadline is None or message.available_at < next_deadline:
                next_deadline = message.available_at
            for deadline in message.lease_deadline_by_consumer.values():
                if deadline > now and (next_deadline is None or deadline < next_deadline):
                    next_deadline = deadline
        if next_deadline is None:
            return None
        return max(0.0, (next_deadline - now).total_seconds())

    def cleanup_expired(self) -> None:
        state = self.state
        now = state.now
        if state.next_expires_at is not None and state.next_expires_at > now:
            return
        expired = [message for message in state.messages if message.expires_at <= now]
        if not expired:
            state.next_expires_at = min(
                (message.expires_at for message in state.messages),
                default=None,
            )
            return
        expired_ids = {message.message_id for message in expired}
        state.messages = [
            message for message in state.messages if message.message_id not in expired_ids
        ]
        for topic, messages in list(state.by_topic.items()):
            live = [message for message in messages if message.message_id not in expired_ids]
            if live:
                state.by_topic[topic] = live
            else:
                state.by_topic.pop(topic, None)
        state.push_delivery_indexes.clear()
        for message_id in expired_ids:
            message = state.by_id.pop(message_id, None)
            if message is not None:
                for receipt_handle in message.leases_by_consumer.values():
                    state.by_receipt.pop(receipt_handle, None)
                message.payload.delete()
        state.next_expires_at = min(
            (message.expires_at for message in state.messages),
            default=None,
        )

    def _can_deliver(
        self,
        message: StoredMessage,
        topic: str,
        consumer: str,
        deployment: str | None,
    ) -> bool:
        now = self.state.now
        if (
            message.topic != topic
            or message.acknowledged_for(consumer)
            or message.duplicate_of is not None
        ):
            return False
        if message.expires_at <= now or message.available_at > now:
            return False
        if deployment is not None and message.deployment != deployment:
            return False
        return message.lease_deadline_by_consumer.get(consumer, now) <= now

    def requeue_push_message(
        self,
        message: StoredMessage,
        consumer: str,
        deadline: datetime,
    ) -> None:
        deployments: set[str | None] = {None}
        if message.deployment != DEFAULT_DEPLOYMENT_PARTITION:
            deployments.add(message.deployment)
        wake = deadline <= self.state.now
        for deployment in deployments:
            index = self.state.push_delivery_indexes.get(
                _PushIndexKey(message.topic, consumer, deployment)
            )
            if index is None:
                continue
            if wake:
                index.ready.append(message.message_id)
            else:
                self._push_block_after(index, deadline, message.message_id)
        if wake:
            self.wake_dispatchers()

    def _push_delivery_index(
        self,
        topic: str,
        consumer: str,
        deployment: str | None,
    ) -> _PushDeliveryIndex:
        return self.state.push_delivery_indexes.setdefault(
            _PushIndexKey(topic, consumer, deployment),
            _PushDeliveryIndex(),
        )

    def _push_index_messages(self, index: _PushDeliveryIndex, topic: str) -> None:
        messages = self.state.by_topic.get(topic, [])
        start = min(index.cursor, len(messages))
        for message in messages[start:]:
            index.ready.append(message.message_id)
        index.cursor = len(messages)

    def _push_ready_blocked(self, index: _PushDeliveryIndex, now: datetime) -> None:
        while index.blocked and index.blocked[0][0] <= now:
            _deadline, _sequence, message_id = heapq.heappop(index.blocked)
            index.ready.append(message_id)

    def _push_block_after(
        self,
        index: _PushDeliveryIndex,
        deadline: datetime,
        message_id: str,
    ) -> None:
        heapq.heappush(
            index.blocked,
            (deadline, next(self.state.push_blocked_sequence), message_id),
        )

    def _message_visible_deadline_for_consumer(
        self,
        message: StoredMessage,
        topic: str,
        consumer: str,
        deployment: str | None,
        now: datetime,
    ) -> datetime | None:
        if (
            message.topic != topic
            or message.acknowledged_for(consumer)
            or message.duplicate_of is not None
        ):
            return None
        if message.expires_at <= now:
            return None
        if deployment is not None and message.deployment != deployment:
            return None
        deadline = message.lease_deadline_by_consumer.get(consumer)
        if deadline is not None and deadline > now:
            return max(message.available_at, deadline)
        return message.available_at


class EmbeddedQueueAsgiApp:
    def __init__(self, server: EmbeddedQueueServer | None = None) -> None:
        self._server = server or EmbeddedQueueServer()
        self._async_client_factory = self._async_http_client_factory

    @property
    def state(self) -> EmbeddedQueueState:
        return self._server.state

    @property
    def server(self) -> EmbeddedQueueServer:
        return self._server

    def get_async_client(
        self,
        *,
        token: str | None = "local-token",
        region: str | None = "iad1",
        base_url: BaseUrl | None = "http://vqs.test",
        deployment: DeploymentOption = ALL_DEPLOYMENTS,
        headers: Mapping[str, str] | None = None,
        timeout: Duration | None = 10.0,
    ) -> QueueClient:
        return QueueClient(
            token=token,
            region=region,
            base_url=base_url,
            deployment=deployment,
            headers=headers,
            timeout=timeout,
            http_client_factory=self._async_client_factory,
        )

    def acquire_async_client(
        self,
        *,
        token: str | None = "local-token",
        region: str | None = "iad1",
        base_url: BaseUrl | None = "http://vqs.test",
        deployment: DeploymentOption = ALL_DEPLOYMENTS,
        headers: Mapping[str, str] | None = None,
        timeout: Duration | None = 10.0,
    ) -> AbstractAsyncContextManager[QueueClient]:
        @asynccontextmanager
        async def _acquire() -> AsyncIterator[QueueClient]:
            yield self.get_async_client(
                token=token,
                region=region,
                base_url=base_url,
                deployment=deployment,
                headers=headers,
                timeout=timeout,
            )

        return _acquire()

    def get_sync_client(
        self,
        *,
        token: str | None = "local-token",
        region: str | None = "iad1",
        base_url: BaseUrl | None = "http://vqs.test",
        deployment: DeploymentOption = ALL_DEPLOYMENTS,
        headers: Mapping[str, str] | None = None,
        timeout: Duration | None = 10.0,
    ) -> SyncQueueClient:
        return SyncQueueClient(
            token=token,
            region=region,
            base_url=base_url,
            deployment=deployment,
            headers=headers,
            timeout=timeout,
        )

    def acquire_sync_client(
        self,
        *,
        token: str | None = "local-token",
        region: str | None = "iad1",
        base_url: BaseUrl | None = "http://vqs.test",
        deployment: DeploymentOption = ALL_DEPLOYMENTS,
        headers: Mapping[str, str] | None = None,
        timeout: Duration | None = 10.0,
    ) -> AbstractContextManager[SyncQueueClient]:
        @contextmanager
        def _acquire() -> Iterator[SyncQueueClient]:
            yield self.get_sync_client(
                token=token,
                region=region,
                base_url=base_url,
                deployment=deployment,
                headers=headers,
                timeout=timeout,
            )

        return _acquire()

    def reset_clients(self) -> None:
        pass

    def _async_http_client_factory(self, **kwargs: object) -> httpx.AsyncClient:
        del kwargs
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self),
            base_url="http://vqs.test",
        )

    def create_async_client_factory(
        self,
    ) -> Callable[..., httpx.AsyncClient]:
        return self._async_client_factory

    def iter_push_deliveries(
        self,
        topic: str,
        consumer: str,
        *,
        lease_seconds: int = DEFAULT_PUSH_LEASE_SECONDS,
        deployment: str | None = None,
        region: str = "iad1",
    ) -> Iterator[PushDelivery]:
        while True:
            delivery = self._server.push_delivery(
                topic,
                consumer,
                lease_seconds=lease_seconds,
                deployment=deployment,
                region=region,
            )
            if delivery is None:
                return
            yield delivery

    async def __call__(
        self,
        scope: MutableMapping[str, Any],
        receive: Callable[[], Any],
        send: Callable[[MutableMapping[str, Any]], Any],
    ) -> None:
        if scope["type"] != "http":
            raise RuntimeError(f"unsupported ASGI scope type: {scope['type']!r}")
        request = _AsgiRequest(scope, receive)
        response = await self.handle(request)
        await response.send(send)

    async def handle(self, request: _AsgiRequest) -> _AsgiResponse:
        route = _parse_queue_route(request.path_parts)
        if route is None:
            return _AsgiResponse(404)

        await self._server.record_request(request)

        try:
            _validate_route(route)
        except ValueError as exc:
            return _bad_request(str(exc))

        if response := self._server.response_for(request, route):
            return response

        match request.method, route.action:
            case "POST", "topic":
                return await self._send_message(route.topic, request)
            case "POST", "consumer" if route.consumer is not None:
                return self._poll_messages(route.topic, route.consumer, request)
            case "POST", "message_id" if route.consumer is not None and route.message_id:
                return self._poll_message_by_id(
                    route.topic,
                    route.consumer,
                    route.message_id,
                    request,
                )
            case (("DELETE" | "PATCH"), "lease") if route.consumer and route.receipt_handle:
                return await self._handle_lease(
                    route.topic,
                    route.consumer,
                    route.receipt_handle,
                    request,
                )
            case "PATCH", "lease_visibility" if route.consumer and route.receipt_handle:
                return await self._handle_lease(
                    route.topic,
                    route.consumer,
                    route.receipt_handle,
                    request,
                )

        return _AsgiResponse(404)

    async def _send_message(self, topic: str, request: _AsgiRequest) -> _AsgiResponse:
        try:
            message = self._server.put(
                topic,
                await request.body(),
                request.headers,
            )
        except ValueError as exc:
            return _bad_request(str(exc))
        return _json_response(
            201,
            {"messageId": message.message_id},
            headers=[(VQS_HEADER_MESSAGE_ID.encode(), message.message_id.encode())],
        )

    def _poll_messages(self, topic: str, consumer: str, request: _AsgiRequest) -> _AsgiResponse:
        response_format = _receive_response_format(request)
        if isinstance(response_format, _AsgiResponse):
            return response_format
        try:
            limit = _int_header(
                request.headers.get(VQS_HEADER_MAX_MESSAGES),
                1,
                minimum=1,
                maximum=10,
                name=VQS_HEADER_MAX_MESSAGES,
            )
            lease_seconds = _int_header(
                request.headers.get(VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS),
                DEFAULT_PULL_LEASE_SECONDS,
                minimum=0,
                maximum=MAX_VISIBILITY_TIMEOUT_SECONDS,
                name=VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS,
            )
            deployment = _deployment_header(request.headers)
        except ValueError as exc:
            return _bad_request(str(exc))
        messages = self._server.visible_messages(topic, consumer, deployment)[:limit]
        if not messages:
            debug_log(
                "receive.empty",
                topic=topic,
                consumer_group=consumer,
                status_code=204,
            )
            return _AsgiResponse(204)
        leased = [
            _LeasedMessage(
                message=message,
                receipt_handle=self._server.lease(message, consumer, lease_seconds),
                consumer=consumer,
            )
            for message in messages
        ]
        return _receive_response(response_format, leased)

    def _poll_message_by_id(
        self,
        topic: str,
        consumer: str,
        message_id: str,
        request: _AsgiRequest,
    ) -> _AsgiResponse:
        response_format = _receive_response_format(request)
        if isinstance(response_format, _AsgiResponse):
            return response_format
        try:
            deployment = _deployment_header(request.headers)
            lease_seconds = _int_header(
                request.headers.get(VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS),
                DEFAULT_PULL_LEASE_SECONDS,
                minimum=0,
                maximum=MAX_VISIBILITY_TIMEOUT_SECONDS,
                name=VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS,
            )
        except ValueError as exc:
            return _bad_request(str(exc))
        found_message = self._server.find_by_id(topic, message_id)
        if (
            found_message is None
            or found_message.expires_at <= self._server.now
            or (deployment is not None and found_message.deployment != deployment)
        ):
            return _json_response(404, {"error": "Message not found"})
        if found_message.duplicate_of is not None:
            debug_log(
                "receive.redirect_duplicate",
                requested_message_id=message_id,
                original_message_id=found_message.duplicate_of,
            )
            return _json_response(
                409,
                {
                    "error": "This messageId was a duplicate - use originalMessageId instead",
                    "originalMessageId": found_message.duplicate_of,
                },
            )
        if found_message.acknowledged_for(consumer):
            return _AsgiResponse(410)
        deadline = found_message.lease_deadline_by_consumer.get(consumer)
        if deadline is not None and deadline > self._server.now:
            retry_after = max(1, int((deadline - self._server.now).total_seconds()))
            return _json_response(
                409,
                {"error": "Message is locked by another consumer"},
                headers=[(b"retry-after", str(retry_after).encode())],
            )
        receipt_handle = self._server.lease(found_message, consumer, lease_seconds)
        return _receive_response(
            response_format,
            [
                _LeasedMessage(
                    message=found_message,
                    receipt_handle=receipt_handle,
                    consumer=consumer,
                )
            ],
        )

    async def _handle_lease(
        self,
        topic: str,
        consumer: str,
        receipt_handle: str,
        request: _AsgiRequest,
    ) -> _AsgiResponse:
        try:
            deployment = _deployment_header(request.headers)
        except ValueError as exc:
            return _bad_request(str(exc))
        receipt_message = self._server.receipt_message(
            topic,
            consumer,
            receipt_handle,
            deployment,
        )
        if receipt_message is None:
            if self._server.receipt_exists(topic, receipt_handle, deployment):
                return _json_response(409, {"error": "Message is not currently in-flight"})
            return _json_response(404, {"error": "Message not found"})
        if receipt_message.acknowledged_for(consumer):
            return _json_response(409, {"error": "Message is not currently in-flight"})
        deadline = receipt_message.lease_deadline_by_consumer.get(consumer)
        if deadline is None or deadline <= self._server.now:
            return _json_response(409, {"error": "Message lease has expired"})
        if request.method == "DELETE":
            receipt_message.acknowledge_for(consumer)
            self._server.state.by_receipt.pop(receipt_handle, None)
            return _AsgiResponse(204)
        if request.method != "PATCH":
            return _AsgiResponse(404)

        try:
            body = json.loads((await request.body()).decode() or "{}")
            seconds = _visibility_timeout_seconds_from_body(body)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            return _bad_request(str(exc))
        new_deadline = self._server.now + timedelta(seconds=seconds)
        if new_deadline > receipt_message.expires_at:
            return _json_response(
                400,
                {
                    "error": "Visibility timeout cannot extend beyond message expiration",
                    "messageExpiresAt": _format_dt(receipt_message.expires_at),
                    "requestedExpiresAt": _format_dt(new_deadline),
                },
            )
        receipt_message.lease_deadline_by_consumer[consumer] = new_deadline
        self._server.requeue_push_message(receipt_message, consumer, new_deadline)
        return _json_response(200, {"success": True})


class EmbeddedQueueDispatcher:
    def __init__(
        self,
        server: EmbeddedQueueServer,
        client_factory: Callable[[], EmbeddedQueuePushClient],
    ) -> None:
        self._server = server
        self._client_factory = client_factory
        self._registrations: list[_DispatcherRegistration] = []
        self._inflight: set[tuple[tuple[str, SubscriptionMatchKind, str], str]] = set()
        self._inflight_counts: dict[tuple[str, SubscriptionMatchKind, str], int] = {}
        self._wake_send, self._wake_receive = anyio.create_memory_object_stream[None](1)
        self._server.add_wake_callback(self.wake)
        register_embedded_dispatcher(self)

    @property
    def server(self) -> EmbeddedQueueServer:
        return self._server

    def register_subscription(
        self,
        *,
        topic: str,
        consumer_group: str,
        retry_after_seconds: int | None,
        initial_delay_seconds: int | None,
        max_concurrency: int | None,
        max_attempts: int | None,
    ) -> None:
        if topic == "*":
            topic_pattern = topic
            match_kind: SubscriptionMatchKind = "wildcard"
        elif topic.endswith("*"):
            topic_pattern = topic[:-1]
            match_kind = "prefix"
        else:
            topic_pattern = topic
            match_kind = "exact"
        registration_key = (topic_pattern, match_kind, consumer_group)
        if any(registration.key() == registration_key for registration in self._registrations):
            return
        self._registrations.append(
            _DispatcherRegistration(
                topic_pattern=topic_pattern,
                match_kind=match_kind,
                consumer_group=consumer_group,
                retry_after_seconds=retry_after_seconds,
                initial_delay_seconds=initial_delay_seconds,
                max_concurrency=max_concurrency,
                max_attempts=max_attempts,
                registered_at=self._server.now,
                concurrency_limit=(
                    max(1, max_concurrency) if max_concurrency is not None else None
                ),
                semaphore=(
                    anyio.Semaphore(max(1, max_concurrency))
                    if max_concurrency is not None
                    else None
                ),
            )
        )
        debug_log(
            "embedded.subscription_registered",
            topic_pattern=topic_pattern,
            match_kind=match_kind,
            consumer_group=consumer_group,
            retry_after_seconds=retry_after_seconds,
            initial_delay_seconds=initial_delay_seconds,
            max_concurrency=max_concurrency,
            max_attempts=max_attempts,
        )
        self.wake()

    def wake(self) -> None:
        try:
            self._wake_send.send_nowait(None)
        except anyio.WouldBlock:
            pass
        except anyio.ClosedResourceError:
            pass

    async def run(self) -> None:
        async with anyio.create_task_group() as task_group:
            while True:
                delivered = False
                registrations = list(self._registrations)
                for registration in registrations:
                    delivered = self._schedule_registration(registration, task_group) or delivered

                delay = self._sleep_delay(delivered=delivered)
                if not delivered:
                    debug_log("embedded.no_message_after_wake", sleep_delay_seconds=delay)
                self._drain_wake()
                with anyio.move_on_after(delay):
                    await self._wake_receive.receive()

    def _schedule_registration(
        self,
        registration: _DispatcherRegistration,
        task_group: TaskGroup,
    ) -> bool:
        if not self._registration_ready(registration):
            return False

        delivered = False
        for topic in self._topics_for_registration(registration):
            delivered = self._schedule_topic(registration, topic, task_group) or delivered
        return delivered

    def _schedule_topic(
        self,
        registration: _DispatcherRegistration,
        topic: str,
        task_group: TaskGroup,
    ) -> bool:
        delivered = False
        while self._can_schedule(registration):
            delivery = self._server.push_delivery(
                topic,
                registration.consumer_group,
            )
            if delivery is None:
                break
            delivered = True
            message_id = delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID]
            registration_key = registration.key()
            self._inflight.add((registration_key, message_id))
            self._inflight_counts[registration_key] = (
                self._inflight_counts.get(registration_key, 0) + 1
            )
            debug_log(
                "embedded.delivery_scheduled",
                topic=topic,
                consumer_group=registration.consumer_group,
            )
            task_group.start_soon(
                self._deliver,
                registration,
                topic,
                message_id,
                delivery,
                name="vercel-embedded-queue-delivery",
            )
        return delivered

    async def aclose(self) -> None:
        await self._wake_send.aclose()
        await self._wake_receive.aclose()

    def unregister(self) -> None:
        unregister_embedded_dispatcher(self)

    def _drain_wake(self) -> None:
        while True:
            try:
                self._wake_receive.receive_nowait()
            except anyio.WouldBlock:
                return
            except anyio.EndOfStream:
                return

    def _sleep_delay(self, *, delivered: bool) -> float:
        if delivered:
            return 0.0
        next_delay = self._next_registration_visible_delay()
        if next_delay is None:
            return 0.1
        return min(max(next_delay, 0.0), 0.1)

    def _next_registration_visible_delay(self) -> float | None:
        self._server.cleanup_expired()
        now = self._server.now
        next_deadline: datetime | None = None
        for registration in self._registrations:
            if not self._registration_ready(registration):
                ready_at = registration.registered_at + timedelta(
                    seconds=registration.initial_delay_seconds or 0
                )
                next_deadline = _min_datetime(next_deadline, ready_at)
                continue
            if not self._can_schedule(registration):
                continue
            for topic in self._topics_for_registration(registration):
                topic_messages = self._server.state.by_topic.get(topic, [])
                for message in topic_messages:
                    deadline = self._message_visible_deadline(message, registration, now)
                    if deadline is None:
                        continue
                    if deadline <= now:
                        return 0.0
                    next_deadline = _min_datetime(next_deadline, deadline)
        if next_deadline is None:
            return None
        return max(0.0, (next_deadline - now).total_seconds())

    def _message_visible_deadline(
        self,
        message: StoredMessage,
        registration: _DispatcherRegistration,
        now: datetime,
    ) -> datetime | None:
        if not registration.matches_topic(message.topic):
            return None
        if message.duplicate_of is not None or message.expires_at <= now:
            return None
        if message.acknowledged_for(registration.consumer_group):
            return None
        deadline = message.lease_deadline_by_consumer.get(registration.consumer_group)
        if deadline is not None and deadline > now:
            return max(message.available_at, deadline)
        return message.available_at

    def _registration_ready(self, registration: _DispatcherRegistration) -> bool:
        if registration.initial_delay_seconds is None:
            return True
        ready_at = registration.registered_at + timedelta(
            seconds=registration.initial_delay_seconds
        )
        return self._server.now >= ready_at

    def _topics_for_registration(self, registration: _DispatcherRegistration) -> list[str]:
        topics = [
            topic
            for topic in self._server.state.by_topic
            if registration.matches_topic(topic) and self._can_schedule(registration)
        ]
        return list(dict.fromkeys(topics))

    def _can_schedule(self, registration: _DispatcherRegistration) -> bool:
        if registration.concurrency_limit is None:
            return True
        return self._inflight_counts.get(registration.key(), 0) < registration.concurrency_limit

    async def _deliver(
        self,
        registration: _DispatcherRegistration,
        topic: str,
        message_id: str,
        delivery: PushDelivery,
    ) -> None:
        try:
            semaphore = registration.semaphore
            if semaphore is None:
                await self._deliver_once(registration, delivery)
            else:
                async with semaphore:
                    await self._deliver_once(registration, delivery)
        finally:
            del topic
            registration_key = registration.key()
            self._inflight.discard((registration_key, message_id))
            count = self._inflight_counts.get(registration_key, 0)
            if count <= 1:
                self._inflight_counts.pop(registration_key, None)
            else:
                self._inflight_counts[registration_key] = count - 1
            self.wake()

    async def _deliver_once(
        self,
        registration: _DispatcherRegistration,
        delivery: PushDelivery,
    ) -> None:
        try:
            client = self._client_factory()
            await client.accept_and_handle(delivery.body, delivery.headers)
            debug_log(
                "embedded.delivery_success",
                message_id=delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID],
                topic=delivery.headers[CLOUD_EVENT_HEADER_VQS_TOPIC],
                consumer_group=registration.consumer_group,
            )
        except Exception:  # noqa: BLE001
            message = self._server.state.by_id.get(
                delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID]
            )
            debug_log(
                "embedded.delivery_failure",
                message_id=delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID],
                topic=delivery.headers[CLOUD_EVENT_HEADER_VQS_TOPIC],
                consumer_group=registration.consumer_group,
            )
            if message is None:
                return
            attempts = message.delivery_count_by_consumer.get(registration.consumer_group, 0)
            if registration.max_attempts is not None and attempts >= registration.max_attempts:
                message.acknowledge_for(registration.consumer_group)
                debug_log(
                    "embedded.max_attempts_acknowledgement",
                    message_id=message.message_id,
                    topic=message.topic,
                    consumer_group=registration.consumer_group,
                    attempts=attempts,
                )
                return
            retry_after_seconds = registration.retry_after_seconds or DEFAULT_RETRY_AFTER_SECONDS
            message.lease_deadline_by_consumer[registration.consumer_group] = (
                self._server.now + timedelta(seconds=retry_after_seconds)
            )
            self._server.requeue_push_message(
                message,
                registration.consumer_group,
                message.lease_deadline_by_consumer[registration.consumer_group],
            )
            debug_log(
                "embedded.retry_after_applied",
                message_id=message.message_id,
                topic=message.topic,
                consumer_group=registration.consumer_group,
                retry_after_seconds=retry_after_seconds,
            )


@dataclass(frozen=True)
class EmbeddedQueueDevServer:
    """A running embedded queue dev server."""

    state: EmbeddedQueueState
    base_url: str
    app: EmbeddedQueueAsgiApp
    _thread: threading.Thread | None = field(default=None, repr=False, compare=False)

    def is_running(self) -> bool:
        """Return whether the embedded HTTP server thread is still running."""
        return self._thread is None or self._thread.is_alive()

    def reset(self) -> None:
        """Reset server state and cached clients between tests."""
        self.app.reset_clients()
        self.state.reset()

    def get_async_client(
        self,
        *,
        token: str | None = "local-token",
        region: str | None = "iad1",
        base_url: BaseUrl | None = "http://vqs.test",
        deployment: DeploymentOption = ALL_DEPLOYMENTS,
        headers: Mapping[str, str] | None = None,
        timeout: Duration | None = 10.0,
    ) -> QueueClient:
        """Create an async client backed by the embedded app."""
        return self.app.get_async_client(
            token=token,
            region=region,
            base_url=base_url,
            deployment=deployment,
            headers=headers,
            timeout=timeout,
        )

    @property
    def client(self) -> QueueClient:
        """Default async client backed by the embedded app."""
        return self.get_async_client()

    @property
    def http(self) -> BaseQueueRuntime:
        """Low-level async HTTP runtime backed by the embedded server."""
        return self.get_async_client(base_url=self.base_url).http

    def get_sync_client(
        self,
        *,
        token: str | None = "local-token",
        region: str | None = "iad1",
        base_url: BaseUrl | None = None,
        deployment: DeploymentOption = ALL_DEPLOYMENTS,
        headers: Mapping[str, str] | None = None,
        timeout: Duration | None = 10.0,
    ) -> SyncQueueClient:
        """Create a sync client backed by the running embedded server."""
        return self.app.get_sync_client(
            token=token,
            region=region,
            base_url=self.base_url if base_url is None else base_url,
            deployment=deployment,
            headers=headers,
            timeout=timeout,
        )

    def iter_push_deliveries(
        self,
        topic: str,
        consumer: str,
        *,
        lease_seconds: int = DEFAULT_PUSH_LEASE_SECONDS,
        deployment: str | None = None,
        region: str = "iad1",
    ) -> Iterator[PushDelivery]:
        """Lease visible messages as synthetic push deliveries."""
        return self.app.iter_push_deliveries(
            topic,
            consumer,
            lease_seconds=lease_seconds,
            deployment=deployment,
            region=region,
        )


@dataclass(frozen=True)
class EmbeddedQueueService:
    server: EmbeddedQueueServer
    dispatcher: EmbeddedQueueDispatcher
    token: str
    region: str
    base_url: str
    deployment: DeploymentOption
    async_http_client_factory: Callable[..., httpx.AsyncClient]

    def get_async_client(self) -> QueueClient:
        return QueueClient(
            token=self.token,
            region=self.region,
            base_url=self.base_url,
            deployment=self.deployment,
            http_client_factory=self.async_http_client_factory,
        )

    async def aclose(self) -> None:
        pass


@asynccontextmanager
async def embedded_queue_service(
    *,
    token: str = "local-token",
    region: str = "iad1",
    base_url: str = "http://vqs.test",
    deployment: DeploymentOption = ALL_DEPLOYMENTS,
    manual_clock: bool = False,
) -> AsyncIterator[EmbeddedQueueService]:
    app = create_embedded_queue_app(manual_clock=manual_clock)
    server = app.server
    service: EmbeddedQueueService

    def client_factory() -> QueueClient:
        return service.get_async_client()

    dispatcher = EmbeddedQueueDispatcher(server, client_factory)
    service = EmbeddedQueueService(
        server=server,
        dispatcher=dispatcher,
        token=token,
        region=region,
        base_url=base_url,
        deployment=deployment,
        async_http_client_factory=app.create_async_client_factory(),
    )
    try:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(dispatcher.run, name="vercel-embedded-queue-dispatcher")
            dispatcher.wake()
            try:
                yield service
            finally:
                task_group.cancel_scope.cancel()
    finally:
        dispatcher.unregister()
        await dispatcher.aclose()
        await service.aclose()


def create_embedded_queue_app(
    *,
    manual_clock: bool = False,
) -> EmbeddedQueueAsgiApp:
    """Create an embedded queue ASGI app and its backing server."""
    if manual_clock:
        state = EmbeddedQueueState(clock=ManualEmbeddedQueueClock())
    else:
        state = EmbeddedQueueState()
    server = EmbeddedQueueServer(state=state)
    return EmbeddedQueueAsgiApp(server)


class _AsgiRequest:
    def __init__(self, scope: Mapping[str, Any], receive: Callable[[], Any]) -> None:
        self.method = str(scope["method"])
        self.path = str(scope["path"])
        self.path_parts = [unquote(part) for part in str(scope["path"]).split("/") if part]
        self.headers = httpx.Headers([
            (key.decode("latin-1"), value.decode("latin-1"))
            for key, value in scope.get("headers", [])
        ])
        self._receive = receive
        self._body: bytes | None = None

    async def body(self) -> bytes:
        if self._body is not None:
            return self._body
        chunks: list[bytes] = []
        more_body = True
        while more_body:
            event = await self._receive()
            chunks.append(event.get("body", b""))
            more_body = bool(event.get("more_body", False))
        self._body = b"".join(chunks)
        return self._body


@dataclass
class _AsgiResponse:
    status_code: int
    body: bytes = b""
    headers: list[tuple[bytes, bytes]] = field(default_factory=list)

    async def send(self, send: Callable[[MutableMapping[str, Any]], Any]) -> None:
        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": self.headers,
        })
        await send({"type": "http.response.body", "body": self.body})


def _json_response(
    status_code: int,
    payload: Mapping[str, object],
    *,
    headers: list[tuple[bytes, bytes]] | None = None,
) -> _AsgiResponse:
    return _AsgiResponse(
        status_code,
        json.dumps(payload).encode(),
        [*JSON_RESPONSE_HEADERS, *(headers or [])],
    )


def _bad_request(message: str) -> _AsgiResponse:
    return _json_response(400, {"error": message})


def _visibility_timeout_seconds_from_body(body: object) -> int:
    if not isinstance(body, dict):
        raise TypeError("JSON body must be an object")
    seconds_value = body.get("visibilityTimeoutSeconds")
    if not isinstance(seconds_value, int) or isinstance(seconds_value, bool):
        raise TypeError("Invalid visibilityTimeoutSeconds - must be a non-negative integer")
    return _int_header_value(
        seconds_value,
        DEFAULT_PULL_LEASE_SECONDS,
        minimum=0,
        maximum=MAX_VISIBILITY_TIMEOUT_SECONDS,
        name="visibilityTimeoutSeconds",
    )


def _message_id_from_receipt_handle(receipt_handle: str) -> str | None:
    parts = receipt_handle.split(":", 2)
    if len(parts) < 3 or not parts[0].startswith("rh_"):
        return None
    return parts[1] or None


def _parse_queue_route(parts: list[str]) -> _QueueRoute | None:
    prefix_length = len(QUEUE_PATH_PREFIX)
    prefix_index = next(
        (
            index
            for index in range(len(parts) - prefix_length + 1)
            if tuple(parts[index : index + prefix_length]) == QUEUE_PATH_PREFIX
        ),
        None,
    )
    if prefix_index is None:
        return None
    parts = parts[prefix_index:]
    if len(parts) <= prefix_length:
        return None

    topic = parts[prefix_length]
    tail = parts[prefix_length + 1 :]
    if tail == []:
        return _QueueRoute("topic", topic)
    if len(tail) < 2 or tail[0] != "consumer":
        return None

    consumer = tail[1]
    consumer_tail = tail[2:]
    if consumer_tail == []:
        return _QueueRoute("consumer", topic, consumer=consumer)
    if len(consumer_tail) == 2 and consumer_tail[0] == "id":
        return _QueueRoute(
            "message_id",
            topic,
            consumer=consumer,
            message_id=consumer_tail[1],
        )
    if len(consumer_tail) >= 2 and consumer_tail[0] == "lease":
        lease_tail = consumer_tail[1:]
        action: QueueRouteAction = "lease"
        if lease_tail[-1:] == ["visibility"]:
            lease_tail = lease_tail[:-1]
            action = "lease_visibility"
        if not lease_tail:
            return None
        return _QueueRoute(
            action,
            topic,
            consumer=consumer,
            receipt_handle="/".join(lease_tail),
        )
    return None


def _validate_route(route: _QueueRoute) -> None:
    validate_topic_name(route.topic)
    if route.consumer is not None:
        validate_name(route.consumer, field="consumer_group")


def _deployment_header(headers: Mapping[str, str]) -> str | None:
    deployment = headers.get(VQS_HEADER_DEPLOYMENT_ID)
    if deployment is not None:
        _validate_deployment_id(deployment)
    return deployment


def _validate_deployment_id(deployment: str) -> None:
    if not deployment or DEPLOYMENT_ID_PATTERN.fullmatch(deployment) is None:
        raise ValueError(f"Invalid deployment id: {deployment!r}; must match {VQS_NAME_PATTERN}")


def _receive_response_format(request: _AsgiRequest) -> ReceiveResponseFormat | _AsgiResponse:
    accept = request.headers.get(HEADER_ACCEPT)
    if accept is None:
        return _bad_request(
            "Accept header required. Supported: multipart/mixed, application/x-ndjson"
        )

    accepted_types = {_media_type(value) for value in accept.split(",")}
    if "*/*" in accepted_types:
        return _bad_request(
            "Accept header required. Supported: multipart/mixed, application/x-ndjson"
        )
    if CONTENT_TYPE_MULTIPART_MIXED in accepted_types:
        return "multipart"
    if CONTENT_TYPE_NDJSON in accepted_types:
        return "ndjson"
    return _bad_request("Unsupported Accept header. Use multipart/mixed or application/x-ndjson")


def _media_type(value: str) -> str:
    return value.split(";", 1)[0].strip().lower()


def async_client_factory(app: EmbeddedQueueAsgiApp) -> Any:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://vqs.test")


def _receive_response(
    response_format: ReceiveResponseFormat,
    messages: list[_LeasedMessage],
) -> _AsgiResponse:
    if response_format == "ndjson":
        return _ndjson_response(messages)
    return _multipart_response([
        _multipart_part(message.message, message.receipt_handle, message.consumer)
        for message in messages
    ])


def _ndjson_response(messages: list[_LeasedMessage]) -> _AsgiResponse:
    lines = [
        json.dumps(
            {
                "messageId": leased.message.message_id,
                "receiptHandle": leased.receipt_handle,
                "deliveryCount": leased.message.delivery_count_by_consumer[leased.consumer],
                "timestamp": _format_dt(leased.message.created_at),
                "expiresAt": _format_dt(leased.message.expires_at),
                "contentType": leased.message.content_type,
                "body": base64.b64encode(leased.message.payload.read()).decode("ascii"),
            },
            separators=(",", ":"),
        ).encode()
        for leased in messages
    ]
    return _AsgiResponse(
        200,
        b"\n".join(lines) + b"\n",
        [(b"content-type", CONTENT_TYPE_NDJSON.encode())],
    )


def _multipart_response(parts: list[bytes]) -> _AsgiResponse:
    body = b"".join(parts) + f"--{BOUNDARY}--\r\n".encode()
    return _AsgiResponse(
        200,
        body,
        [(b"content-type", f"{CONTENT_TYPE_MULTIPART_MIXED}; boundary={BOUNDARY}".encode())],
    )


def _multipart_part(message: StoredMessage, receipt_handle: str, consumer: str) -> bytes:
    headers = [
        f"--{BOUNDARY}".encode(),
        f"{HEADER_CONTENT_TYPE}: {message.content_type}".encode(),
        f"{VQS_HEADER_MESSAGE_ID}: {message.message_id}".encode(),
        f"{VQS_HEADER_RECEIPT_HANDLE}: {receipt_handle}".encode(),
        f"{VQS_HEADER_DELIVERY_COUNT}: {message.delivery_count_by_consumer[consumer]}".encode(),
        f"{VQS_HEADER_TIMESTAMP}: {_format_dt(message.created_at)}".encode(),
        f"{VQS_HEADER_EXPIRES_AT}: {_format_dt(message.expires_at)}".encode(),
    ]
    return b"\r\n".join([*headers, b"", message.payload.read(), b""])


def _format_dt(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _min_datetime(current: datetime | None, candidate: datetime) -> datetime:
    if current is None or candidate < current:
        return candidate
    return current


def _int_header(
    value: str | None,
    default: int,
    *,
    minimum: int,
    maximum: int,
    name: str,
) -> int:
    return _int_header_value(
        value,
        default,
        minimum=minimum,
        maximum=maximum,
        name=name,
    )


def _int_header_value(
    value: object,
    default: int,
    *,
    minimum: int,
    maximum: int,
    name: str,
) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        try:
            numeric = float(value)
        except ValueError as exc:
            raise ValueError(f"{name} must be an integer") from exc
        if not math.isfinite(numeric) or not numeric.is_integer():
            raise ValueError(f"{name} must be an integer")
        parsed = int(numeric)
    else:
        raise TypeError(f"{name} must be an integer")
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return parsed
