from __future__ import annotations

from typing import TYPE_CHECKING

import base64
import json
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace

import dramatiq
import dramatiq.broker as dramatiq_broker
import pytest
from dramatiq.worker import Worker

from vercel.headers import set_headers
from vercel.integrations.dramatiq import VercelQueueBroker
from vercel.oidc import token as oidc
from vercel.queue import ALL_DEPLOYMENTS
from vercel.queue.testing import clear_subscriptions, reset_default_queue_clients

if TYPE_CHECKING:
    from vercel.queue.devserver import EmbeddedQueueDevServer

pytest_plugins = ["vercel.queue.testing.pytest"]


@pytest.fixture(autouse=True)
def isolated_vqs_state() -> Iterator[None]:
    reset_default_queue_clients()
    clear_subscriptions()
    old_broker = dramatiq_broker.global_broker
    try:
        yield
    finally:
        dramatiq_broker.global_broker = old_broker
        reset_default_queue_clients()
        clear_subscriptions()


def test_poll_worker_processes_and_acknowledges_message(
    embedded_queue_server: EmbeddedQueueDevServer,
) -> None:
    broker = VercelQueueBroker(
        token="token",
        region="iad1",
        base_url=embedded_queue_server.base_url,
        deployment=ALL_DEPLOYMENTS,
        consumer_group="dramatiq",
        lease_duration=30,
        timeout=5,
        poll=True,
    )
    dramatiq.set_broker(broker)
    handled: list[str] = []

    @dramatiq.actor(queue_name="emails", actor_name="dramatiq-worker.send_email")
    def send_email(user_id: str) -> None:
        handled.append(user_id)

    worker = Worker(broker, worker_threads=1, worker_timeout=50)
    worker.start()
    try:
        send_email.send("user_1")
        _wait_for(lambda: handled == ["user_1"])
    finally:
        worker.stop(timeout=5000)
        broker.close()

    assert len(embedded_queue_server.state.messages) == 1
    assert embedded_queue_server.state.messages[0].topic == "emails"
    assert embedded_queue_server.state.messages[0].acknowledged is True


def test_broker_refreshes_mounted_token_without_recreation(
    embedded_queue_server: EmbeddedQueueDevServer,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VERCEL_QUEUE_TOKEN", raising=False)
    monkeypatch.delenv("VERCEL_OIDC_TOKEN", raising=False)
    set_headers(None)
    oidc._clear_cached_oidc_token()
    path = tmp_path / "token"
    now = time.time()
    monkeypatch.setattr(oidc, "_OIDC_TOKEN_PATH", path)
    monkeypatch.setattr(oidc, "time", SimpleNamespace(time=lambda: now, monotonic=lambda: now))

    def rotate() -> str:
        payload = base64.urlsafe_b64encode(json.dumps({"exp": now + 120}).encode()).decode()
        token = f"header.{payload}.signature"
        replacement = tmp_path / "replacement"
        replacement.write_text(token, encoding="utf-8")
        replacement.replace(path)
        return token

    broker = VercelQueueBroker(
        region="iad1",
        base_url=embedded_queue_server.base_url,
        deployment=ALL_DEPLOYMENTS,
        middleware=[],
        poll=True,
    )
    broker.declare_queue("emails")
    message: dramatiq.Message[None] = dramatiq.Message(
        queue_name="emails", actor_name="send_email", args=("user_1",), kwargs={}, options={}
    )
    requests = embedded_queue_server.state.requests
    try:
        first = rotate()
        broker.enqueue(message)
        assert requests[-1].headers["Authorization"] == f"Bearer {first}"

        now += 60
        second = rotate()
        deliveries = list(broker.poll_messages("emails", prefetch=1))
        assert requests[-1].headers["Authorization"] == f"Bearer {second}"

        now += 60
        third = rotate()
        broker.acknowledge_message(deliveries[0])
        assert requests[-1].headers["Authorization"] == f"Bearer {third}"
        broker.enqueue(message)
        assert requests[-1].headers["Authorization"] == f"Bearer {third}"
    finally:
        broker.close()
        oidc._clear_cached_oidc_token()


def _wait_for(predicate: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before timeout")
