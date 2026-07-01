"""Queue test state helpers."""

from __future__ import annotations

from .._internal import subscribers as queue_subscribers
from .._internal.http import (
    reset_async_http_client_pool_for_tests,
    reset_http_client_pools_for_tests,
)


def reset_default_queue_clients() -> None:
    """Reset cached default Queue clients for tests."""
    reset_http_client_pools_for_tests()


async def reset_default_async_queue_clients() -> None:
    """Reset cached default asynchronous Queue clients for tests."""
    await reset_async_http_client_pool_for_tests()


def clear_subscriptions() -> None:
    """Clear registered subscribers and embedded dispatchers for tests."""
    queue_subscribers.clear_subscriptions_for_tests()


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = (
    "clear_subscriptions",
    "reset_default_async_queue_clients",
    "reset_default_queue_clients",
)
