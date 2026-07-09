from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload

import warnings
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
)
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
from types import TracebackType
from urllib.parse import quote

from vercel.headers import HeadersContext

from .config import (
    CURRENT_DEPLOYMENT,
    BaseUrl,
    DeploymentOption,
    apply_custom_headers,
    resolve_base_url,
    resolve_deployment,
    resolve_region,
    validate_region,
)
from .constants import (
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_MULTIPART_MIXED,
    HEADER_ACCEPT,
    HEADER_AUTHORIZATION,
    HEADER_CONTENT_TYPE,
    HEADER_USER_AGENT,
    USER_AGENT,
    VQS_HEADER_CLIENT_TS,
    VQS_HEADER_DELAY_SECONDS,
    VQS_HEADER_DEPLOYMENT_ID,
    VQS_HEADER_IDEMPOTENCY_KEY,
    VQS_HEADER_MAX_MESSAGES,
    VQS_HEADER_RETENTION_SECONDS,
    VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS,
)
from .errors import InvalidLimitError, MessageUnavailableError, ProtocolError
from .http import (
    AsyncHttpClientFactory,
    AsyncHttpMessage,
    AsyncHttpResponse,
    AsyncPushDeliveryBody,
    AsyncPushDeliveryInput,
    AsyncQueueRuntime,
    BaseQueueRuntime,
    PushDeliveryBody,
)
from .lease import (
    DEFAULT_PROCESSING_LEASE_SECONDS,
    LeaseRenewal,
    finalize_payload_async,
    finalize_payload_sync,
    processing_lease_seconds,
    retry_async_follow_up,
    retry_sync_follow_up,
    visibility_timeout_seconds,
)
from .log import debug_log, debug_log_for_msg
from .messages import (
    message_from_part_async,
    message_with_region,
)
from .multipart import (
    parse_multipart_messages,
)
from .names import SanitizedName, normalize_name, validate_topic_name
from .polling import poll_and_handle_async
from .push import (
    accept_input,
    parse_push_delivery,
    parse_push_delivery_metadata,
)
from .response import queue_response_error, send_response_error
from .streams import AsyncStreamPayload, AsyncTextStreamPayload
from .subscribers import (
    QueueSubscriber,
    call_subscribers,
    infer_subscriber_transport,
)
from .transports import infer_send_transport, receive_transport_for_topic, send_transport_for_topic
from .types import (
    Duration,
    Handoff,
    Headers,
    Message,
    MessageID,
    MessageMetadata,
    RawHeaders,
    RetryAfter,
    StrContainer,
    Topic,
    Transport,
    duration_to_float_seconds,
    duration_to_seconds,
)

T = TypeVar("T")
QUEUE_API_PATH = "/api/v3/topic"

if TYPE_CHECKING:
    from .asgi import QueueClientAsgiApp


class _DuplicateMessageRedirect(Exception):  # noqa: N818
    def __init__(self, message_id: MessageID) -> None:
        self.message_id = message_id
        super().__init__(message_id)


class _DeferredClose:
    def __init__(self) -> None:
        self._close: Callable[[], Awaitable[None]] | None = None

    def set(self, close: Callable[[], Awaitable[None]]) -> None:
        self._close = close

    async def aclose(self) -> None:
        close = self._close
        if close is not None:
            await close()


class BaseQueueClient:
    def __init__(
        self,
        *,
        runtime: BaseQueueRuntime,
        token: str | None = None,
        region: str | None = None,
        base_url: BaseUrl | None = None,
        deployment: DeploymentOption = CURRENT_DEPLOYMENT,
        headers: Mapping[str, str] | None = None,
        timeout: Duration | None = 10.0,
    ) -> None:
        self._runtime = runtime
        self.token = token
        self.region = validate_region(region)
        self._base_url_source = base_url
        self.base_url = resolve_base_url(base_url, region=self.region)
        self._runtime.configure_base_url(self.base_url)
        self._resolved_region = resolve_region(self.region)
        self.deployment = deployment
        self.headers = dict(headers or {})
        self.timeout = None if timeout is None else duration_to_float_seconds(timeout)
        if self.timeout is not None and self.timeout < 0:
            raise ValueError("timeout must be non-negative")

    @property
    def http(self) -> BaseQueueRuntime:
        return self._runtime

    def _url(self, *parts: str) -> str:
        return self._url_from_base(self.base_url, *parts)

    def _url_from_base(self, base_url: str, *parts: str) -> str:
        path = "/".join(quote(part, safe="") for part in parts)
        return f"{base_url}{QUEUE_API_PATH}/{path}"

    def _metadata_url(self, metadata: MessageMetadata, *parts: str) -> str:
        if metadata.region is None:
            return self._url(*parts)
        # Push callbacks are regional; follow-up ACK/lease calls must return to
        # the region that owns the delivered message.
        return self._url_from_base(
            resolve_base_url(self._base_url_source, region=metadata.region),
            *parts,
        )

    def _headers(
        self,
        *,
        token: str,
        content_type: str | None = None,
        accept: str | None = None,
        deployment: DeploymentOption = CURRENT_DEPLOYMENT,
        custom_headers: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        headers = {
            HEADER_AUTHORIZATION: f"Bearer {token}",
            HEADER_USER_AGENT: USER_AGENT,
            VQS_HEADER_CLIENT_TS: datetime.now(timezone.utc).isoformat(),
        }
        if content_type is not None:
            headers[HEADER_CONTENT_TYPE] = content_type
        if accept is not None:
            headers[HEADER_ACCEPT] = accept
        apply_custom_headers(headers, self.headers)
        apply_custom_headers(headers, custom_headers)
        selected_deployment = self.deployment if deployment is CURRENT_DEPLOYMENT else deployment
        resolved_deployment = resolve_deployment(selected_deployment)
        if resolved_deployment:
            headers[VQS_HEADER_DEPLOYMENT_ID] = resolved_deployment
        return headers

    async def _poll_headers(self, lease_duration: Duration | None = None) -> dict[str, str]:
        if lease_duration is None:
            lease_duration_seconds = DEFAULT_PROCESSING_LEASE_SECONDS
        else:
            lease_duration_seconds = visibility_timeout_seconds(
                lease_duration,
                name="lease_duration",
            )

        headers = self._headers(
            token=await self._runtime.token(self.token),
            accept=CONTENT_TYPE_MULTIPART_MIXED,
        )
        headers[VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS] = str(lease_duration_seconds)
        return headers

    async def _send(
        self,
        topic: str | SanitizedName | Topic[T],
        payload: T,
        *,
        transport: Transport[T] | None = None,
        idempotency_key: str | None = None,
        retention: Duration | None = None,
        delay: Duration | None = None,
        deployment: DeploymentOption = CURRENT_DEPLOYMENT,
        headers: Mapping[str, str] | None = None,
    ) -> MessageID | None:
        transport = transport or cast(
            "Transport[T]",
            send_transport_for_topic(topic) or infer_send_transport(payload),
        )
        request_headers = self._headers(
            token=await self._runtime.token(self.token),
            content_type=transport.content_type,
            deployment=deployment,
            custom_headers=headers,
        )
        resolved_retention = None if retention is None else duration_to_seconds(retention)
        resolved_delay = None if delay is None else duration_to_seconds(delay)
        if resolved_retention is not None and resolved_retention < 0:
            raise ValueError("retention must be non-negative")
        if resolved_delay is not None and resolved_delay < 0:
            raise ValueError("delay must be non-negative")
        if idempotency_key:
            request_headers[VQS_HEADER_IDEMPOTENCY_KEY] = idempotency_key
        if resolved_retention is not None:
            request_headers[VQS_HEADER_RETENTION_SECONDS] = str(resolved_retention)
        if resolved_delay is not None:
            request_headers[VQS_HEADER_DELAY_SECONDS] = str(resolved_delay)
        resolved_topic = validate_topic_name(topic)
        response = await self._runtime.post(
            self._url(resolved_topic),
            content=transport.serialize(payload),
            headers=request_headers,
        )
        if response.status_code == 202:
            debug_log("send.deferred", topic=resolved_topic, status_code=202)
            # Deferred ingestion intentionally has no usable message ID yet.
            warnings.warn(
                "message was accepted but delivery is deferred (202 Accepted)",
                stacklevel=2,
            )
            return None
        if response.status_code != 201 and (error := await send_response_error(response)):
            raise error
        data = response.json()
        if not isinstance(data, dict) or "messageId" not in data:
            raise RuntimeError("Queue API returned an unexpected response: missing 'messageId'")
        return MessageID(str(data["messageId"]))

    async def _receive(
        self,
        topic: str | SanitizedName | Topic[T],
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
        transport: Transport[T] | None = None,
    ) -> AsyncIterator[Message[T]]:
        if limit < 1 or limit > 10:
            raise InvalidLimitError(limit)

        headers = await self._poll_headers(lease_duration=lease_duration)
        headers[VQS_HEADER_MAX_MESSAGES] = str(limit)

        resolved_topic = validate_topic_name(topic)
        consumer = normalize_name(
            consumer_group,
            field="consumer_group",
        )
        transport = transport or cast("Transport[T]", receive_transport_for_topic(topic))
        async with self._runtime.stream_post(
            self._url(resolved_topic, "consumer", str(consumer)),
            headers=headers,
        ) as response:
            if response.status_code == 204:
                debug_log(
                    "receive.empty",
                    topic=resolved_topic,
                    consumer_group=str(consumer),
                    status_code=204,
                )
                return
            if error := await queue_response_error(response):
                raise error

            async for part_headers, body in parse_multipart_messages(response):
                message = await message_from_part_async(
                    resolved_topic,
                    str(consumer),
                    transport,
                    part_headers,
                    body,
                )
                # Safety: yield-in-context-manager-in-async-generator
                #
                # We want to retain full streaming semantics of pulls and so
                # have to keep the HTTP stream open while the callers deal
                # with yielded messages.  ASYNC119 correctly warns against
                # yielding from an async generator, but the risk here is
                # just leaking an async HTTP connection past loop lifetime,
                # which is something we can live with.
                yield message_with_region(message, self._resolved_region)  # noqa: ASYNC119

    async def _poll_by_id(
        self,
        topic: str | SanitizedName | Topic[T],
        consumer_group: str | SanitizedName,
        message_id: MessageID,
        *,
        lease_duration: Duration | None = None,
        transport: Transport[T] | None = None,
        region: str | None = None,
        _seen_message_ids: frozenset[MessageID] | None = None,
    ) -> Message[T]:
        topic = validate_topic_name(topic)
        consumer = normalize_name(
            consumer_group,
            field="consumer_group",
        )
        transport = transport or cast("Transport[T]", receive_transport_for_topic(topic))
        seen_message_ids = _seen_message_ids or frozenset()
        current_message_id = message_id

        while True:
            if current_message_id in seen_message_ids:
                raise ProtocolError(
                    f"receive-by-id duplicate redirect loop for message {current_message_id}"
                )

            seen_message_ids |= {current_message_id}

            try:
                message = await self._poll_by_id_once(
                    topic,
                    str(consumer),
                    current_message_id,
                    lease_duration=lease_duration,
                    transport=transport,
                    region=region,
                )
            except _DuplicateMessageRedirect as redirect:
                debug_log(
                    "receive.redirect_duplicate",
                    requested_message_id=current_message_id,
                    original_message_id=redirect.message_id,
                )
                current_message_id = redirect.message_id
                continue

            return message_with_region(
                message,
                validate_region(region) or self._resolved_region,
            )

    async def _poll_by_id_once(
        self,
        topic: str,
        consumer: str,
        message_id: MessageID,
        *,
        lease_duration: Duration | None,
        transport: Transport[T],
        region: str | None,
    ) -> Message[T]:
        headers = await self._poll_headers(lease_duration=lease_duration)

        async with AsyncExitStack() as stack:
            response = await stack.enter_async_context(
                self._runtime.stream_post(
                    self._regional_url(
                        topic,
                        "consumer",
                        consumer,
                        "id",
                        message_id,
                        region=region,
                    ),
                    headers=headers,
                )
            )
            await _raise_or_redirect_response(response, message_id)

            try:
                part_headers, part_body = await anext(parse_multipart_messages(response))
            except StopAsyncIteration:
                raise ProtocolError("multipart response contained no parts") from None

            response_owner = _DeferredClose()
            message = await message_from_part_async(
                topic,
                consumer,
                transport,
                part_headers,
                _close_response_with_stream(part_body, response_owner.aclose),
            )
            if _message_payload_closes_response(message):
                response_owner.set(stack.pop_all().aclose)
            return message

    async def _accept(
        self,
        raw_body: PushDeliveryBody,
        headers: Headers,
        *,
        transport: Transport[T] | None = None,
        lease_duration: Duration | None = None,
    ) -> Message[T]:
        parsed = await parse_push_delivery(
            raw_body,
            headers,
            transport=transport,
        )
        metadata = parsed.metadata
        if parsed.message is not None:
            return Message(payload=parsed.message.payload, metadata=metadata)
        # Header-only callbacks are notifications; fetch by ID to acquire the
        # actual receipt handle before user code runs.
        debug_log(
            "push.header_only_fetch",
            topic=metadata.topic,
            consumer_group=str(metadata.consumer_group),
        )
        message = await self._poll_by_id(
            Topic[T](metadata.topic),
            metadata.consumer_group,
            metadata.message_id,
            lease_duration=lease_duration,
            transport=transport,
            region=metadata.region,
        )
        return message_with_region(message, metadata.region)

    async def _acknowledge(self, message: Message[T] | MessageMetadata) -> None:
        target = self._lease_target(message)
        headers = self._headers(token=await self._runtime.token(self.token))
        response = await self._runtime.delete(target.url, headers=headers)
        if error := await queue_response_error(response, message_id=target.metadata.message_id):
            raise error

    async def _extend_lease(
        self,
        message: Message[T] | MessageMetadata,
        duration: Duration,
        *,
        runtime: BaseQueueRuntime | None = None,
    ) -> None:
        lease_duration = visibility_timeout_seconds(duration)
        target = self._lease_target(message)
        runtime = runtime or self._runtime
        headers = self._headers(
            token=await runtime.token(self.token),
            content_type=CONTENT_TYPE_JSON,
        )
        response = await runtime.patch(
            target.url,
            json={"visibilityTimeoutSeconds": int(lease_duration)},
            headers=headers,
        )
        if error := await queue_response_error(response, message_id=target.metadata.message_id):
            raise error

    async def _renew_lease(self, message: Message[Any], duration: Duration) -> None:
        await self._extend_lease(message, duration)

    def _lease_target(self, message: Message[Any] | MessageMetadata) -> _LeaseTarget:
        metadata = message.metadata if isinstance(message, Message) else message
        if metadata.receipt_handle is None:
            raise ValueError("message metadata must include receipt_handle")
        return _LeaseTarget(
            metadata=metadata,
            url=self._metadata_url(
                metadata,
                metadata.topic,
                "consumer",
                str(metadata.consumer_group),
                "lease",
                metadata.receipt_handle,
            ),
        )

    def _regional_url(self, *parts: str, region: str | None) -> str:
        if region is None:
            return self._url(*parts)
        return self._url_from_base(resolve_base_url(self._base_url_source, region=region), *parts)

    def run_lease_renewal(
        self,
        message: Message[Any],
        lease_duration: Duration | None = None,
        headers_context: HeadersContext | None = None,
    ) -> LeaseRenewal:
        """Create a context manager that keeps a message lease alive."""
        return LeaseRenewal(
            message,
            client=self,
            lease_duration=lease_duration,
            headers_context=headers_context,
        )


class Delivery(Generic[T]):
    """Lifecycle wrapper returned by poll mode.

    Entering a delivery starts automatic lease renewal and returns the message.
    Clean exits acknowledge the message. Exceptional exits finalize payloads and
    leave the message unacknowledged, except for queue directives handled by the
    lifecycle.
    """

    def __init__(
        self,
        message: Message[T],
        *,
        client: Any,
        lease_duration: Duration | None = None,
    ) -> None:
        self._message = message
        self._client = client
        self._renewal = LeaseRenewal(
            message,
            client=client,
            lease_duration=processing_lease_seconds(lease_duration),
        )
        self._async_lifecycle = _AsyncMessageLifecycle(
            message,
            client=client,
            lease_duration=lease_duration,
        )
        self._accepted = False
        self._entered = False

    def accept(self) -> Message[T]:
        """Accept this delivery for manual lifecycle management.

        The returned message remains leased to this consumer group, but the
        delivery context manager no longer owns acknowledgement, lease renewal,
        payload finalization, or retry directives. Callers that accept a
        delivery must explicitly acknowledge it, extend or release its lease,
        and close streaming payloads when needed.

        """
        if self._entered:
            raise RuntimeError("entered delivery cannot be accepted")
        if self._accepted:
            raise RuntimeError("delivery has already been accepted")
        self._accepted = True
        return self._message

    def _raise_if_accepted(self) -> None:
        if self._accepted:
            raise RuntimeError("accepted delivery cannot be used as a context manager")

    def __enter__(self) -> Message[T]:
        self._raise_if_accepted()
        self._entered = True
        self._renewal.__enter__()
        return self._message

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        wait_for_renewal_stop = isinstance(exc, (Handoff, RetryAfter))
        try:
            self._renewal.stop(wait=wait_for_renewal_stop)
        finally:
            finalize_payload_sync(self._message.payload)

        if isinstance(exc, Handoff):
            debug_log_for_msg("message.handoff", self._message)
            return True
        if isinstance(exc, RetryAfter):
            self._renewal.extend(exc.timeout_seconds, self._client.extend_lease)
            debug_log_for_msg(
                "message.retry_after",
                self._message,
                retry_after_seconds=exc.timeout_seconds,
            )
            return True
        if exc is None:
            retry_sync_follow_up(
                lambda: self._client.acknowledge(self._message),
                event_prefix="acknowledge",
            )
            debug_log_for_msg("message.ack", self._message)
        return None

    async def __aenter__(self) -> Message[T]:
        self._raise_if_accepted()
        self._entered = True
        await self._async_lifecycle.__aenter__()
        return self._message

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return await self._async_lifecycle.__aexit__(exc_type, exc, traceback)

    @property
    def message(self) -> Message[T]:
        """Message represented by this delivery, for manual lifecycle use."""
        return self._message


class QueueClient(BaseQueueClient):
    """Asynchronous Vercel Queues client.

    Args:
        token: Vercel OIDC token. Defaults to environment/OIDC resolution.
        region: Queue region, such as ``"iad1"``.
        base_url: Custom service base URL or region resolver for tests or proxies.
        deployment: Deployment partition selection. Use ``ALL_DEPLOYMENTS``
            to omit ``Vqs-Deployment-Id``.
        headers: Default custom headers for send requests.
        timeout: HTTP timeout in seconds.
        http_client_factory: Optional factory for the underlying HTTP client.

    """

    def __init__(
        self,
        *,
        token: str | None = None,
        region: str | None = None,
        base_url: BaseUrl | None = None,
        deployment: DeploymentOption = CURRENT_DEPLOYMENT,
        headers: Mapping[str, str] | None = None,
        timeout: Duration | None = 10.0,
        http_client_factory: AsyncHttpClientFactory | None = None,
    ) -> None:
        super().__init__(
            runtime=AsyncQueueRuntime(
                timeout=timeout,
                client_factory=http_client_factory,
            ),
            token=token,
            region=region,
            base_url=base_url,
            deployment=deployment,
            headers=headers,
            timeout=timeout,
        )

    async def send(
        self,
        topic: str | SanitizedName | Topic[T],
        payload: T,
        *,
        idempotency_key: str | None = None,
        retention: Duration | None = None,
        delay: Duration | None = None,
        deployment: DeploymentOption = CURRENT_DEPLOYMENT,
        headers: Mapping[str, str] | None = None,
    ) -> MessageID | None:
        """Send a message to a topic.

        Args:
            topic: Topic object or topic name.
            payload: Payload accepted by the topic or inferred transport.
            idempotency_key: Optional service-side deduplication key.
            retention: Optional message retention duration.
            delay: Optional delay before the message becomes visible.
            deployment: Per-send deployment partition selection.
            headers: Custom non-protected headers to include.

        Returns:
            Created message ID, or ``None`` if ingestion was deferred.

        Raises:
            QueueError: If the service rejects the request.
            RuntimeError: If the service returns an unexpected success body.

        """
        return await self._send(
            topic,
            payload,
            idempotency_key=idempotency_key,
            retention=retention,
            delay=delay,
            deployment=deployment,
            headers=headers,
        )

    async def _accept_impl(
        self,
        raw_body: AsyncPushDeliveryInput,
        headers: RawHeaders | None = None,
        *,
        transport: Transport[T] | None = None,
        lease_duration: Duration | None = None,
    ) -> Message[T]:
        raw_body, headers = accept_input(raw_body, headers)
        return await self._accept(
            raw_body,
            headers,
            transport=transport,
            lease_duration=lease_duration,
        )

    @overload
    async def accept_and_handle(
        self,
        raw_body: AsyncPushDeliveryBody,
        headers: RawHeaders,
        *,
        lease_duration: Duration | None = None,
    ) -> None: ...

    @overload
    async def accept_and_handle(
        self,
        raw_body: AsyncHttpMessage,
        headers: None = None,
        *,
        lease_duration: Duration | None = None,
    ) -> None: ...

    async def accept_and_handle(
        self,
        raw_body: AsyncPushDeliveryInput,
        headers: RawHeaders | None = None,
        *,
        lease_duration: Duration | None = None,
    ) -> None:
        """Accept a push callback and dispatch matching subscribers.

        Args:
            raw_body: Callback body bytes, byte iterable, or response object.
            headers: Callback request headers, unless ``raw_body`` is a response.
            lease_duration: Processing timeout used while handlers run.

        Raises:
            UnhandledMessageError: If no subscription matches the topic.
            QueueError: If fetching, acknowledging, or retry scheduling fails.

        """
        await self._accept_and_handle(
            raw_body,
            headers,
            lease_duration=lease_duration,
        )

    async def _accept_and_handle(
        self,
        raw_body: AsyncPushDeliveryInput,
        headers: RawHeaders | None = None,
        *,
        transport: Transport[T] | None = None,
        lease_duration: Duration | None = None,
    ) -> None:
        raw_body, headers = accept_input(raw_body, headers)
        processing_lease_duration = processing_lease_seconds(lease_duration)
        if transport is None:
            metadata = parse_push_delivery_metadata(headers)
            transport = infer_subscriber_transport(metadata)
        # Delivery headers carry the auth context for follow-up queue calls,
        # including lease renewals running outside this call stack.
        with HeadersContext(headers).use():
            message = await self._accept(
                raw_body,
                headers,
                transport=transport,
                lease_duration=processing_lease_duration,
            )
            lifecycle = _AsyncMessageLifecycle(
                message,
                client=self,
                lease_duration=processing_lease_duration,
            )
            async with lifecycle:
                await call_subscribers(message)

    @overload
    def poll(
        self,
        topic: Topic[AsyncStreamPayload],
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> AsyncIterator[Delivery[AsyncStreamPayload]]: ...

    @overload
    def poll(
        self,
        topic: Topic[AsyncTextStreamPayload],
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> AsyncIterator[Delivery[AsyncTextStreamPayload]]: ...

    @overload
    def poll(
        self,
        topic: Topic[T],
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> AsyncIterator[Delivery[T]]: ...

    @overload
    def poll(
        self,
        topic: str,
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> AsyncIterator[Delivery[Any]]: ...

    @overload
    def poll(
        self,
        topic: SanitizedName,
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> AsyncIterator[Delivery[Any]]: ...

    def poll(
        self,
        topic: str | SanitizedName | Topic[T],
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> AsyncIterator[Delivery[T]]:
        """Poll available deliveries for a consumer group.

        Args:
            topic: Topic object or topic name to receive from.
            consumer_group: Consumer group to receive as.
            limit: Maximum messages to claim, from 1 through 10.
            lease_duration: Optional processing timeout for received messages.

        Returns:
            Async iterator of deliveries. Enter each delivery to process its message.

        Raises:
            InvalidLimitError: If ``limit`` is outside the service range.
            QueueError: If the service rejects the request.
            MessageCorruptedError: If a payload cannot be deserialized.

        """

        async def _deliveries() -> AsyncIterator[Delivery[T]]:
            async for message in self._receive(
                topic,
                consumer_group,
                limit=limit,
                lease_duration=lease_duration,
            ):
                yield Delivery(message, client=self, lease_duration=lease_duration)

        return _deliveries()

    async def poll_and_handle(
        self,
        subscriber: QueueSubscriber[..., Any],
        *,
        topics: StrContainer | None = None,
        interval: Duration = 1.0,
        limit: int | None = None,
        lease_duration: Duration | None = None,
    ) -> None:
        """Continuously poll messages and dispatch one registered subscriber.

        Args:
            subscriber: Callback previously registered with ``@subscribe``.
            topics: Concrete topic names to poll. Required for wildcard or prefix
                subscription patterns; exact subscriptions infer their topic.
            interval: Idle backoff when all configured topics are empty.
            limit: Per-request maximum from 1 through 10. ``None`` drains until
                empty before idling.
            lease_duration: Optional processing timeout for received messages.

        """
        await poll_and_handle_async(
            poll=self.poll,
            subscriber=subscriber,
            topics=topics,
            interval=interval,
            limit=limit,
            lease_duration=lease_duration,
        )

    async def acknowledge(self, message: Message[T] | MessageMetadata) -> None:
        """Acknowledge a received message.

        Args:
            message: Message or metadata containing a delivery token.

        Raises:
            ValueError: If delivery metadata is incomplete.
            QueueError: If the service rejects the acknowledgement.

        """
        await self._acknowledge(message)

    async def extend_lease(
        self,
        message: Message[T] | MessageMetadata,
        duration: Duration,
    ) -> None:
        """Extend message processing.

        Args:
            message: Message or metadata containing a delivery token.
            duration: New processing timeout. Use zero to make it available again.

        Raises:
            ValueError: If duration or delivery metadata is invalid.
            QueueError: If the service rejects the update.

        """
        await self._extend_lease(message, duration)

    def asgi_app(self) -> QueueClientAsgiApp:
        """Return an ASGI push-callback app backed by this client."""
        from .asgi import QueueClientAsgiApp  # noqa: PLC0415

        return QueueClientAsgiApp(self)


@dataclass(frozen=True, kw_only=True)
class _LeaseTarget:
    metadata: MessageMetadata
    url: str


class _AsyncMessageLifecycle:
    def __init__(
        self,
        message: Message[Any],
        *,
        client: QueueClient,
        lease_duration: Duration | None = None,
    ) -> None:
        self._message = message
        self._client = client
        self._renewal = LeaseRenewal(
            message,
            client=client,
            lease_duration=processing_lease_seconds(lease_duration),
        )

    async def __aenter__(self) -> LeaseRenewal:
        await self._renewal.start_async()
        return self._renewal

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        # ACK, Handoff, and RetryAfter deliberately use different renewal shutdown
        # semantics. Server-side, background renewal and final directives are
        # all conditional writes against the same active lease record. If a
        # renewal races with ACK, the final state is safe either way: renewal
        # first still leaves the same receipt handle valid for ACK, and ACK
        # first makes the later renewal fail with a 4xx because the message is
        # no longer INFLIGHT. Waiting for cancellation in the success path only
        # adds tail latency without improving correctness.
        #
        # Handoff leaves the delivery open for another owner, so a best-effort
        # stop failure can keep the hidden renewal alive past handoff.
        #
        # RetryAfter is different because it is itself a visibility update. A
        # late automatic renewal can overwrite the shorter retry delay the
        # handler requested, delaying redelivery until the normal processing
        # lease. For that path we wait for the renewal worker to finish or
        # cancel any in-flight extension before applying the RetryAfter
        # visibility change. If stopping times out, we still apply the
        # directive: the worst remaining case is the pre-existing race, while
        # refusing to apply RetryAfter would always leave the long lease behind.
        wait_for_renewal_stop = isinstance(exc, (Handoff, RetryAfter))
        try:
            await self._renewal.stop_async(wait=wait_for_renewal_stop)
        finally:
            await finalize_payload_async(self._message.payload)

        if isinstance(exc, Handoff):
            debug_log_for_msg("message.handoff", self._message)
            return True
        if isinstance(exc, RetryAfter):
            await self._renewal.extend_async(
                exc.timeout_seconds,
                self._client.extend_lease,
            )
            debug_log_for_msg(
                "message.retry_after",
                self._message,
                retry_after_seconds=exc.timeout_seconds,
            )
            return True
        if exc is None:
            await retry_async_follow_up(
                lambda: self._client.acknowledge(self._message),
                event_prefix="acknowledge",
            )
            debug_log_for_msg("message.ack", self._message)
        return None


async def _close_response_with_stream(
    body: AsyncIterator[bytes],
    close_response: Callable[[], Awaitable[None]],
) -> AsyncIterator[bytes]:
    try:
        async for chunk in body:
            yield chunk
    finally:
        await close_response()


async def _raise_or_redirect_response(
    response: AsyncHttpResponse,
    message_id: MessageID,
) -> None:
    if error := await queue_response_error(response, message_id=message_id):
        if isinstance(error, MessageUnavailableError) and error.original_message_id:
            original_message_id = error.original_message_id
            if original_message_id != message_id:
                raise _DuplicateMessageRedirect(original_message_id) from error
        raise error


def _message_payload_closes_response(message: Message[Any]) -> bool:
    return isinstance(message.payload, (AsyncStreamPayload, AsyncTextStreamPayload))
