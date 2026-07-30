from __future__ import annotations

from typing import Any, TypeVar, overload

from collections.abc import AsyncIterator, Mapping

from .client import Delivery, LeaseRenewal, QueueClient
from .config import (
    CURRENT_DEPLOYMENT,
    DeploymentOption,
)
from .http import (
    AsyncHttpMessage,
    AsyncPushDeliveryBody,
)
from .names import SanitizedName
from .subscribers import QueueSubscriber
from .types import (
    Duration,
    MessageID,
    RawHeaders,
    StrContainer,
    Topic,
)

T = TypeVar("T")


async def send(
    topic: str | SanitizedName | Topic[T],
    payload: T,
    *,
    idempotency_key: str | None = None,
    retention: Duration | None = None,
    delay: Duration | None = None,
    deployment: DeploymentOption = CURRENT_DEPLOYMENT,
    headers: Mapping[str, str] | None = None,
) -> MessageID | None:
    """Send a message with the default asynchronous client.

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

    """
    client = QueueClient()
    return await client.send(
        topic,
        payload,
        idempotency_key=idempotency_key,
        retention=retention,
        delay=delay,
        deployment=deployment,
        headers=headers,
    )


@overload
async def accept_and_handle(
    raw_body: AsyncPushDeliveryBody,
    headers: RawHeaders,
    *,
    lease_duration: Duration | None = None,
) -> None: ...


@overload
async def accept_and_handle(
    raw_body: AsyncHttpMessage,
    headers: None = None,
    *,
    lease_duration: Duration | None = None,
) -> None: ...


async def accept_and_handle(
    raw_body: AsyncPushDeliveryBody | AsyncHttpMessage,
    headers: RawHeaders | None = None,
    *,
    lease_duration: Duration | None = None,
) -> None:
    """Accept a push callback and dispatch async subscribers.

    Args:
        raw_body: Callback body bytes, byte iterable, or response object.
        headers: Callback request headers, unless ``raw_body`` is a response.
        lease_duration: Processing timeout used while handlers run.

    Raises:
        UnhandledMessageError: If no subscription matches the topic.
        QueueError: If fetching, acknowledging, or retry scheduling fails.

    """
    client = QueueClient()
    await client._accept_and_handle(  # noqa: SLF001
        raw_body,
        headers,
        lease_duration=lease_duration,
    )


@overload
def poll(
    topic: Topic[T],
    consumer_group: str | SanitizedName,
    *,
    limit: int = 1,
    lease_duration: Duration | None = None,
) -> AsyncIterator[Delivery[T]]: ...


@overload
def poll(
    topic: str,
    consumer_group: str | SanitizedName,
    *,
    limit: int = 1,
    lease_duration: Duration | None = None,
) -> AsyncIterator[Delivery[Any]]: ...


@overload
def poll(
    topic: SanitizedName,
    consumer_group: str | SanitizedName,
    *,
    limit: int = 1,
    lease_duration: Duration | None = None,
) -> AsyncIterator[Delivery[Any]]: ...


def poll(
    topic: str | SanitizedName | Topic[T],
    consumer_group: str | SanitizedName,
    *,
    limit: int = 1,
    lease_duration: Duration | None = None,
) -> AsyncIterator[Delivery[T]]:
    """Poll available deliveries with the default asynchronous client.

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

    """

    async def _iterate() -> AsyncIterator[Delivery[T]]:
        client = QueueClient()
        async for delivery in client.poll(
            topic,
            consumer_group,
            limit=limit,
            lease_duration=lease_duration,
        ):
            yield delivery

    return _iterate()


async def poll_and_handle(
    subscriber: QueueSubscriber[..., Any],
    *,
    topics: StrContainer | None = None,
    interval: Duration = 1.0,
    limit: int | None = None,
    lease_duration: Duration | None = None,
) -> None:
    """Continuously poll messages with the default async client.

    Args:
        subscriber: Callback previously registered with ``@subscribe``.
        topics: Concrete topic names to poll. Required for wildcard or prefix
            subscription patterns; exact subscriptions infer their topic.
        interval: Idle backoff when all configured topics are empty.
        limit: Per-request maximum from 1 through 10. ``None`` drains until
            empty before idling.
        lease_duration: Optional processing timeout for received messages.

    """
    client = QueueClient()
    await client.poll_and_handle(
        subscriber,
        topics=topics,
        interval=interval,
        limit=limit,
        lease_duration=lease_duration,
    )


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__: tuple[str, ...] = (
    "Delivery",
    "LeaseRenewal",
    "QueueClient",
    "accept_and_handle",
    "poll",
    "poll_and_handle",
    "send",
)
