from __future__ import annotations

from typing import Any, TypeVar, overload

import concurrent.futures
from collections.abc import Iterator, Mapping
from types import TracebackType

from vercel.headers import HeadersContext

from .asynctools import iter_async_iterator, iter_coroutine
from .client import BaseQueueClient, Delivery
from .config import CURRENT_DEPLOYMENT, BaseUrl, DeploymentOption
from .http import (
    AsyncQueueRuntime,
    HttpResponse,
    PushDeliveryBody,
    SyncHttpClientFactory,
    SyncQueueRuntime,
)
from .lease import (
    LeaseRenewal,
    finalize_payload_sync,
    processing_lease_seconds,
    retry_message_after_sync,
)
from .log import debug_log_for_msg
from .messages import sync_message_payload
from .names import SanitizedName
from .polling import run_poll_and_handle_sync, start_sync_polling_thread
from .push import accept_input_sync, parse_push_delivery_metadata
from .retry import retry_sync_follow_up
from .streams import SyncStreamPayload, SyncTextStreamPayload
from .subscribers import (
    QueueSubscriber,
    call_subscribers_sync,
    infer_subscriber_transport,
    reject_async_subscriber_for_sync,
)
from .types import (
    Duration,
    Handoff,
    Message,
    MessageID,
    MessageMetadata,
    RawHeaders,
    RetryAfter,
    StrContainer,
    Topic,
    Transport,
)

T = TypeVar("T")


class QueueClient(BaseQueueClient):
    """Synchronous Vercel Queues client.

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
        http_client_factory: SyncHttpClientFactory | None = None,
    ) -> None:
        super().__init__(
            runtime=SyncQueueRuntime(
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

    def send(
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
        """Send a message to a topic."""
        return iter_coroutine(
            self._send(
                topic,
                payload,
                idempotency_key=idempotency_key,
                retention=retention,
                delay=delay,
                deployment=deployment,
                headers=headers,
            )
        )

    def _accept_impl(
        self,
        raw_body: PushDeliveryBody | HttpResponse,
        headers: RawHeaders | None = None,
        *,
        transport: Transport[T] | None = None,
        lease_duration: Duration | None = None,
    ) -> Message[T]:
        raw_body, headers = accept_input_sync(raw_body, headers)
        message = iter_coroutine(
            self._accept(
                raw_body,
                headers,
                transport=transport,
                lease_duration=lease_duration,
            )
        )
        return sync_message_payload(message)

    @overload
    def accept_and_handle(
        self,
        raw_body: PushDeliveryBody,
        headers: RawHeaders,
        *,
        lease_duration: Duration | None = None,
    ) -> None: ...

    @overload
    def accept_and_handle(
        self,
        raw_body: HttpResponse,
        headers: None = None,
        *,
        lease_duration: Duration | None = None,
    ) -> None: ...

    def accept_and_handle(
        self,
        raw_body: PushDeliveryBody | HttpResponse,
        headers: RawHeaders | None = None,
        *,
        lease_duration: Duration | None = None,
    ) -> None:
        """Accept a push callback and dispatch matching subscribers."""
        self._accept_and_handle(
            raw_body,
            headers,
            lease_duration=lease_duration,
        )

    def _accept_and_handle(
        self,
        raw_body: PushDeliveryBody | HttpResponse,
        headers: RawHeaders | None = None,
        *,
        transport: Transport[T] | None = None,
        lease_duration: Duration | None = None,
    ) -> None:
        raw_body, headers = accept_input_sync(raw_body, headers)
        processing_lease_duration = processing_lease_seconds(lease_duration)
        if transport is None:
            metadata = parse_push_delivery_metadata(headers)
            transport = infer_subscriber_transport(metadata)
        # Delivery headers carry the auth context for follow-up queue calls,
        # including lease renewals running outside this call stack.
        with HeadersContext(headers).use():
            message = sync_message_payload(
                iter_coroutine(
                    self._accept(
                        raw_body,
                        headers,
                        transport=transport,
                        lease_duration=processing_lease_duration,
                    )
                )
            )
            lifecycle = _MessageLifecycle(
                message,
                client=self,
                lease_duration=processing_lease_duration,
            )
            with lifecycle:
                call_subscribers_sync(message)

    @overload
    def poll(
        self,
        topic: Topic[SyncStreamPayload],
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> Iterator[Delivery[SyncStreamPayload]]: ...

    @overload
    def poll(
        self,
        topic: Topic[SyncTextStreamPayload],
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> Iterator[Delivery[SyncTextStreamPayload]]: ...

    @overload
    def poll(
        self,
        topic: Topic[T],
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> Iterator[Delivery[T]]: ...

    @overload
    def poll(
        self,
        topic: str,
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> Iterator[Delivery[Any]]: ...

    @overload
    def poll(
        self,
        topic: SanitizedName,
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> Iterator[Delivery[Any]]: ...

    def poll(
        self,
        topic: str | SanitizedName | Topic[Any],
        consumer_group: str | SanitizedName,
        *,
        limit: int = 1,
        lease_duration: Duration | None = None,
    ) -> Iterator[Delivery[Any]]:
        """Poll available deliveries for a consumer group."""
        messages = self._receive(
            topic,
            consumer_group,
            limit=limit,
            lease_duration=lease_duration,
        )
        for message in iter_async_iterator(messages):
            yield Delivery(
                sync_message_payload(message),
                client=self,
                lease_duration=lease_duration,
            )

    def poll_and_handle(
        self,
        subscriber: QueueSubscriber[..., Any],
        *,
        topics: StrContainer | None = None,
        interval: Duration = 1.0,
        limit: int | None = None,
        lease_duration: Duration | None = None,
    ) -> concurrent.futures.Future[None]:
        """Start a background thread that polls and dispatches one subscriber."""
        reject_async_subscriber_for_sync(subscriber)
        return start_sync_polling_thread(
            lambda stop: run_poll_and_handle_sync(
                poll=self.poll,
                subscriber=subscriber,
                topics=topics,
                interval=interval,
                limit=limit,
                lease_duration=lease_duration,
                stop=stop,
            ),
            name="vercel-queue-poll-and-handle",
        )

    def acknowledge(self, message: Message[T] | MessageMetadata) -> None:
        """Acknowledge a received message.

        This is a lower-level API for integrations that manage message
        lifecycles manually. Application code should usually process messages
        through ``poll`` delivery context managers or ``accept_and_handle`` so
        acknowledgement happens automatically.
        """
        retry_sync_follow_up(
            lambda: iter_coroutine(self._acknowledge(message)),
            event_prefix="ack",
        )

    def extend_lease(
        self,
        message: Message[T] | MessageMetadata,
        duration: Duration,
    ) -> None:
        """Extend message processing.

        This is a lower-level API for integrations that manage message
        lifecycles manually. Application code should usually rely on delivery
        context managers or ``LeaseRenewal`` to keep leases alive while a
        handler runs.
        """
        retry_sync_follow_up(
            lambda: iter_coroutine(self._extend_lease(message, duration)),
            event_prefix="visibility",
        )

    def retry_after(
        self,
        message: Message[T] | MessageMetadata,
        delay: Duration,
    ) -> None:
        """Request redelivery of a received message after ``delay``.

        This is a lower-level API for integrations that manage message
        lifecycles manually. Application code should usually raise
        ``RetryAfter`` from a handler instead of calling this directly.

        Unlike ``extend_lease``, this is a settlement-style follow-up:
        transient failures are retried, and a lease that no longer exists is
        tolerated with a warning because the message will be redelivered
        anyway.
        """
        retry_message_after_sync(message, delay, self._extend_lease_sync)

    def _extend_lease_sync(
        self,
        message: Message[Any] | MessageMetadata,
        duration: Duration,
    ) -> None:
        iter_coroutine(self._extend_lease(message, duration))

    async def _renew_lease(self, message: Message[Any], duration: Duration) -> None:
        runtime = AsyncQueueRuntime(timeout=self.timeout)
        runtime.configure_base_url(self.base_url)
        await self._extend_lease(message, duration, runtime=runtime)


class _MessageLifecycle:
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

    def __enter__(self) -> LeaseRenewal:
        self._renewal.__enter__()
        return self._renewal

    def __exit__(
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


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = ("LeaseRenewal", "QueueClient")
