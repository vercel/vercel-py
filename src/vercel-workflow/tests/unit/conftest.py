"""Fixtures shared by the unit tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

import vercel.queue as vqs
from vercel.queue.testing import clear_subscriptions


@pytest.fixture
def isolated_subscriptions() -> Iterator[None]:
    """Keep queue handler registrations out of the global registry.

    ``create_queue_handler`` registers a ``@subscribe`` handler as a side
    effect, so tests that build a world would otherwise leak subscriptions
    into every test that runs after them.
    """
    saved = vqs.get_subscriptions()
    clear_subscriptions()
    try:
        yield
    finally:
        clear_subscriptions()
        for subscription in saved:
            vqs.subscribe(
                topic=subscription.topic,
                consumer_group=subscription.consumer_group,
                retry_after=subscription.retry_after_seconds,
                initial_delay=subscription.initial_delay_seconds,
                max_concurrency=subscription.max_concurrency,
                max_attempts=subscription.max_attempts,
            )(subscription.func)
