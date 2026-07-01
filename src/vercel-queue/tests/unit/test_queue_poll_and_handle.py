from __future__ import annotations

from typing import Any

import concurrent.futures
import time
from collections.abc import Callable
from datetime import timedelta
from functools import partial

import anyio
import pytest

from vercel.queue import Message, QueueClient, SubscriptionError, poll_and_handle, subscribe
from vercel.queue.devserver import EmbeddedQueueDevServer
from vercel.queue.sync import QueueClient as SyncQueueClient

from .helpers import wait_until


def _wait_until_sync(predicate: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 2
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition was not met")
        time.sleep(0.01)


@pytest.mark.anyio
async def test_top_level_poll_and_handle_uses_default_client(
    isolated_subscriptions: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    @subscribe(topic="jobs", consumer_group="workers")
    async def handle(payload: dict[str, str]) -> None:
        del payload

    class Client(QueueClient):
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        async def poll_and_handle(self, subscriber: object, **kwargs: object) -> None:
            calls.append(("poll", subscriber))
            calls.append(("kwargs", kwargs))

    monkeypatch.setattr("vercel.queue._internal.api_async.QueueClient", Client)

    await poll_and_handle(handle, interval=2, limit=3, lease_duration=30)

    assert calls == [
        ("poll", handle),
        ("kwargs", {"topics": None, "interval": 2, "limit": 3, "lease_duration": 30}),
    ]


@pytest.mark.anyio
async def test_async_poll_and_handle_exact_topic_acknowledges(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    calls: list[dict[str, bool]] = []

    @subscribe(topic="emails", consumer_group="test-group")
    async def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    client = eqs.get_async_client(base_url=eqs.base_url)
    await client.send("emails", {"ok": True})
    with anyio.move_on_after(2):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(partial(client.poll_and_handle, handle, interval=0.01))
            await wait_until(lambda: eqs.state.by_id["msg_1"].acknowledged)
            task_group.cancel_scope.cancel()

    assert calls == [{"ok": True}]
    assert eqs.state.by_id["msg_1"].acknowledged


@pytest.mark.anyio
async def test_async_poll_and_handle_handler_error_logs_and_continues(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[int] = []

    @subscribe(topic="jobs", consumer_group="workers")
    def handle(payload: dict[str, int]) -> None:
        calls.append(payload["n"])
        if payload["n"] == 1:
            raise RuntimeError("boom")

    client = eqs.get_async_client(base_url=eqs.base_url)
    await client.send("jobs", {"n": 1})
    await client.send("jobs", {"n": 2})
    with anyio.move_on_after(2):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(partial(client.poll_and_handle, handle, interval=0.01))
            await wait_until(lambda: eqs.state.by_id["msg_2"].acknowledged)
            task_group.cancel_scope.cancel()

    assert calls == [1, 2]
    assert "queue subscriber failed while polling" in caplog.text
    assert not eqs.state.by_id["msg_1"].acknowledged
    assert eqs.state.by_id["msg_1"].lease_deadline_by_consumer["workers"] == (
        eqs.state.now + timedelta(seconds=60)
    )
    assert eqs.state.by_id["msg_2"].acknowledged


@pytest.mark.anyio
async def test_async_poll_and_handle_wildcard_requires_topics(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    calls: list[str] = []

    @subscribe(topic="events-*", consumer_group="analytics")
    def handle(message: Message[dict[str, str]]) -> None:
        calls.append(message.metadata.topic)

    client = eqs.get_async_client(base_url=eqs.base_url)
    with pytest.raises(SubscriptionError, match="pass concrete topics"):
        await client.poll_and_handle(handle, interval=0)

    await client.send("events-one", {"ok": "yes"})
    with anyio.move_on_after(2):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(
                partial(
                    client.poll_and_handle,
                    handle,
                    topics=["events-one"],
                    interval=0.01,
                )
            )
            await wait_until(lambda: calls == ["events-one"])
            task_group.cancel_scope.cancel()

    assert calls == ["events-one"]


def test_sync_poll_and_handle_returns_cancellable_future(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    calls: list[dict[str, bool]] = []

    @subscribe(topic="emails", consumer_group="test-group")
    def handle(payload: dict[str, bool]) -> None:
        calls.append(payload)

    client = eqs.get_sync_client()
    client.send("emails", {"ok": True})
    future = client.poll_and_handle(handle, interval=0.01)
    _wait_until_sync(lambda: calls == [{"ok": True}])
    assert future.cancel()
    with pytest.raises(concurrent.futures.CancelledError):
        future.result(timeout=2)

    assert future.done()
    assert future.cancelled()
    assert eqs.state.by_id["msg_1"].acknowledged


def test_sync_poll_and_handle_rejects_string_topics(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    @subscribe(topic="events-*", consumer_group="analytics")
    def handle(payload: object) -> None:
        del payload

    client = eqs.get_sync_client()
    future = client.poll_and_handle(
        handle,
        topics="events-one",  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        interval=0.01,
    )
    with pytest.raises(TypeError, match="not a string"):
        future.result(timeout=2)


def test_sync_poll_and_handle_rejects_async_subscriber(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    @subscribe(topic="emails", consumer_group="test-group")
    async def handle(payload: object) -> None:
        del payload

    client = eqs.get_sync_client()
    with pytest.raises(RuntimeError, match="async subscribers must be polled"):
        client.poll_and_handle(handle)


def test_sync_poll_and_handle_handler_error_logs_and_continues(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[int] = []

    @subscribe(topic="jobs", consumer_group="workers")
    def handle(payload: dict[str, int]) -> None:
        calls.append(payload["n"])
        if payload["n"] == 1:
            raise RuntimeError("boom")

    client = eqs.get_sync_client()
    client.send("jobs", {"n": 1})
    client.send("jobs", {"n": 2})
    future = client.poll_and_handle(handle, interval=0.01)
    _wait_until_sync(lambda: calls == [1, 2])
    future.cancel()
    with pytest.raises(concurrent.futures.CancelledError):
        future.result(timeout=2)

    assert calls == [1, 2]
    assert "queue subscriber failed while polling" in caplog.text
    assert not eqs.state.by_id["msg_1"].acknowledged
    assert eqs.state.by_id["msg_1"].lease_deadline_by_consumer["workers"] == (
        eqs.state.now + timedelta(seconds=60)
    )
    assert eqs.state.by_id["msg_2"].acknowledged


def test_sync_poll_and_handle_future_surfaces_poll_errors(
    isolated_subscriptions: None,
) -> None:
    @subscribe(topic="emails", consumer_group="test-group")
    def handle(payload: object) -> None:
        del payload

    class BrokenClient(SyncQueueClient):
        def poll(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("poll failed")

    client = BrokenClient(token="token")
    future = client.poll_and_handle(handle, interval=0.01)
    with pytest.raises(RuntimeError, match="poll failed"):
        future.result(timeout=2)


def test_sync_poll_and_handle_thread_exits_on_cancel(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    @subscribe(topic="emails", consumer_group="test-group")
    def handle(payload: object) -> None:
        del payload

    client = eqs.get_sync_client()
    future = client.poll_and_handle(handle, interval=0.01)
    future.cancel()
    deadline = time.monotonic() + 2
    while not future.done() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert future.done()
