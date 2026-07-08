from __future__ import annotations

from typing import Any, TypeVar, overload

from collections.abc import Iterator, Mapping

from .client import Delivery
from .client_sync import LeaseRenewal, QueueClient
from .config import CURRENT_DEPLOYMENT, DeploymentOption
from .http import HttpResponse, PushDeliveryBody
from .names import SanitizedName
from .types import (
    Duration,
    Message,
    MessageID,
    MessageMetadata,
    RawHeaders,
    Topic,
)

T = TypeVar("T")


def send(
    topic: str | SanitizedName | Topic[T],
    payload: T,
    *,
    idempotency_key: str | None = None,
    retention: Duration | None = None,
    delay: Duration | None = None,
    deployment: DeploymentOption = CURRENT_DEPLOYMENT,
    headers: Mapping[str, str] | None = None,
) -> MessageID | None:
    """Send a message with the default synchronous client."""
    client = QueueClient()
    return client.send(
        topic,
        payload,
        idempotency_key=idempotency_key,
        retention=retention,
        delay=delay,
        deployment=deployment,
        headers=headers,
    )


@overload
def accept_and_handle(
    raw_body: PushDeliveryBody,
    headers: RawHeaders,
    *,
    lease_duration: Duration | None = None,
) -> None: ...


@overload
def accept_and_handle(
    raw_body: HttpResponse,
    headers: None = None,
    *,
    lease_duration: Duration | None = None,
) -> None: ...


def accept_and_handle(
    raw_body: PushDeliveryBody | HttpResponse,
    headers: RawHeaders | None = None,
    *,
    lease_duration: Duration | None = None,
) -> None:
    """Accept a push callback and dispatch sync subscribers."""
    client = QueueClient()
    client._accept_and_handle(  # noqa: SLF001
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
) -> Iterator[Delivery[T]]: ...


@overload
def poll(
    topic: str,
    consumer_group: str | SanitizedName,
    *,
    limit: int = 1,
    lease_duration: Duration | None = None,
) -> Iterator[Delivery[Any]]: ...


@overload
def poll(
    topic: SanitizedName,
    consumer_group: str | SanitizedName,
    *,
    limit: int = 1,
    lease_duration: Duration | None = None,
) -> Iterator[Delivery[Any]]: ...


def poll(
    topic: str | SanitizedName | Topic[Any],
    consumer_group: str | SanitizedName,
    *,
    limit: int = 1,
    lease_duration: Duration | None = None,
) -> Iterator[Delivery[Any]]:
    """Poll available deliveries with the default synchronous client."""

    def _iterate() -> Iterator[Delivery[Any]]:
        client = QueueClient()
        yield from client.poll(
            topic,
            consumer_group,
            limit=limit,
            lease_duration=lease_duration,
        )

    return _iterate()


def acknowledge(message: Message[T] | MessageMetadata) -> None:
    """Acknowledge a received message with the default sync client."""
    client = QueueClient()
    client.acknowledge(message)


def extend_lease(message: Message[T] | MessageMetadata, duration: Duration) -> None:
    """Extend message processing with the default sync client."""
    client = QueueClient()
    client.extend_lease(message, duration)


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = (
    "Delivery",
    "LeaseRenewal",
    "QueueClient",
    "accept_and_handle",
    "acknowledge",
    "extend_lease",
    "poll",
    "send",
)
