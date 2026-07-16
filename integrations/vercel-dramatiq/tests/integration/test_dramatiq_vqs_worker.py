from __future__ import annotations

from typing import TYPE_CHECKING

import time
from collections.abc import Callable, Iterator

import dramatiq
import dramatiq.broker as dramatiq_broker
import pytest
from dramatiq.worker import Worker

from vercel.integrations.dramatiq import VercelQueueBroker
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


def _wait_for(predicate: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before timeout")
