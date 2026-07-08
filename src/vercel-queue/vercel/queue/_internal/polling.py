from __future__ import annotations

from typing import Any

import concurrent.futures
import threading
from collections.abc import Callable, Iterator

import anyio

from .errors import InvalidLimitError
from .subscribers import call_subscriber, call_subscriber_sync, poll_targets_for_subscriber
from .types import Duration, StrContainer, duration_to_float_seconds

MAX_POLL_LIMIT = 10


def _request_limit(limit: int | None) -> int:
    if limit is None:
        return MAX_POLL_LIMIT
    if limit < 1 or limit > MAX_POLL_LIMIT:
        raise InvalidLimitError(limit)
    return limit


def _idle_interval_seconds(interval: Duration) -> float:
    seconds = duration_to_float_seconds(interval)
    if seconds < 0:
        raise ValueError("interval must be non-negative")
    return seconds


async def _poll_topic_batch_async(
    *,
    poll: Callable[..., Any],
    subscriber: Callable[..., Any],
    topic: str,
    consumer_group: str,
    request_limit: int,
    lease_duration: Duration | None,
) -> int:
    handled = 0
    async for delivery in poll(
        topic,
        consumer_group,
        limit=request_limit,
        lease_duration=lease_duration,
    ):
        async with delivery as message:
            await call_subscriber(subscriber, message)
        handled += 1
    return handled


def _poll_topic_batch_sync(
    *,
    poll: Callable[..., Iterator[Any]],
    subscriber: Callable[..., Any],
    topic: str,
    consumer_group: str,
    request_limit: int,
    lease_duration: Duration | None,
    stop: threading.Event,
) -> int:
    handled = 0
    for delivery in poll(
        topic,
        consumer_group,
        limit=request_limit,
        lease_duration=lease_duration,
    ):
        if stop.is_set():
            break
        with delivery as message:
            call_subscriber_sync(subscriber, message)
        handled += 1
    return handled


async def poll_and_handle_async(
    *,
    poll: Callable[..., Any],
    subscriber: Callable[..., Any],
    topics: StrContainer | None,
    interval: Duration,
    limit: int | None,
    lease_duration: Duration | None,
) -> None:
    targets = poll_targets_for_subscriber(subscriber, topics)
    request_limit = _request_limit(limit)
    idle_seconds = _idle_interval_seconds(interval)

    while True:
        handled_any = False
        for topic, consumer_group in targets:
            while True:
                handled_batch = await _poll_topic_batch_async(
                    poll=poll,
                    subscriber=subscriber,
                    topic=topic,
                    consumer_group=consumer_group,
                    request_limit=request_limit,
                    lease_duration=lease_duration,
                )
                handled_any = handled_any or handled_batch > 0
                if limit is not None or handled_batch == 0:
                    break
        if not handled_any:
            await anyio.sleep(idle_seconds)


def run_poll_and_handle_sync(
    *,
    poll: Callable[..., Iterator[Any]],
    subscriber: Callable[..., Any],
    topics: StrContainer | None,
    interval: Duration,
    limit: int | None,
    lease_duration: Duration | None,
    stop: threading.Event,
) -> None:
    targets = poll_targets_for_subscriber(subscriber, topics)
    request_limit = _request_limit(limit)
    idle_seconds = _idle_interval_seconds(interval)

    while not stop.is_set():
        handled_any = False
        for topic, consumer_group in targets:
            if stop.is_set():
                break
            while not stop.is_set():
                handled_batch = _poll_topic_batch_sync(
                    poll=poll,
                    subscriber=subscriber,
                    topic=topic,
                    consumer_group=consumer_group,
                    request_limit=request_limit,
                    lease_duration=lease_duration,
                    stop=stop,
                )
                handled_any = handled_any or handled_batch > 0
                if limit is not None or handled_batch == 0:
                    break
        if not handled_any:
            stop.wait(idle_seconds)


class PollingFuture(concurrent.futures.Future[None]):
    def __init__(self) -> None:
        super().__init__()
        self._stop = threading.Event()
        self._cancel_requested = False

    @property
    def stop_event(self) -> threading.Event:
        return self._stop

    def cancel(self) -> bool:
        if self.done():
            return False
        self._cancel_requested = True
        self._stop.set()
        return True

    def cancelled(self) -> bool:
        return self._cancel_requested and self.done()

    def result(self, timeout: float | None = None) -> None:
        result = super().result(timeout)
        if self._cancel_requested:
            raise concurrent.futures.CancelledError
        return result

    def exception(self, timeout: float | None = None) -> BaseException | None:
        if self._cancel_requested and self.done():
            raise concurrent.futures.CancelledError
        return super().exception(timeout)


def start_sync_polling_thread(
    target: Callable[[threading.Event], None],
    *,
    name: str,
) -> PollingFuture:
    future = PollingFuture()

    def run() -> None:
        try:
            target(future.stop_event)
        except BaseException as exc:  # noqa: BLE001
            if not future.done():
                future.set_exception(exc)
        else:
            if not future.done():
                future.set_result(None)

    thread = threading.Thread(target=run, name=name, daemon=True)
    thread.start()
    return future
