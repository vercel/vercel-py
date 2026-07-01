from __future__ import annotations

from typing import Any, cast

import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker
from kombu import Queue

import vercel.integrations.celery as public_vqs_celery
import vercel.integrations.celery._broker as vqs_celery
from vercel.queue import ALL_DEPLOYMENTS, sanitize_name
from vercel.queue._internal.embedded import ManualEmbeddedQueueClock
from vercel.queue.devserver import EmbeddedQueueDevServer, embedded_queue_dev_server
from vercel.queue.sync import QueueClient as SyncQueueClient
from vercel.queue.testing import (
    clear_subscriptions,
    reset_default_queue_clients,
)


@dataclass(frozen=True)
class WorkerApp:
    app: Celery
    add: Any
    worker: Any
    push_channels: tuple[Any, ...] = ()


@pytest.fixture(scope="session", autouse=True)
def clean_vqs_state() -> Iterator[None]:
    reset_default_queue_clients()
    clear_subscriptions()
    _clear_celery_broker_state()
    try:
        yield
    finally:
        reset_default_queue_clients()
        clear_subscriptions()
        _clear_celery_broker_state()


@pytest.fixture(scope="session")
def eqs() -> Iterator[EmbeddedQueueDevServer]:
    with embedded_queue_dev_server() as server:
        yield server


@pytest.fixture
def isolated_eqs(
    eqs: EmbeddedQueueDevServer,
    push_worker: WorkerApp,
) -> Iterator[EmbeddedQueueDevServer]:
    _reset_celery_broker_state(push_worker)
    eqs.reset()
    try:
        yield eqs
    finally:
        eqs.reset()
        _reset_celery_broker_state(push_worker)


@pytest.fixture(scope="session")
def poll_worker(
    eqs: EmbeddedQueueDevServer,
) -> Iterator[WorkerApp]:
    app = _celery_app(
        "poll-worker",
        "vercel-poll://localhost//",
        eqs.base_url,
    )

    @app.task(name="poll-worker.add")
    def add(left: int, right: int) -> int:
        return left + right

    with start_worker(app, pool="solo", perform_ping_check=False, loglevel="WARNING") as worker:
        yield WorkerApp(app=app, add=add, worker=worker)


@pytest.fixture(scope="session")
def push_worker(
    eqs: EmbeddedQueueDevServer,
) -> Iterator[WorkerApp]:
    app = _celery_app(
        "push-worker",
        "vercel-push://localhost//",
        eqs.base_url,
    )
    vqs_celery.register_celery_app_queues(app, start_worker=False)

    @app.task(name="push-worker.add")
    def add(left: int, right: int) -> int:
        return left + right

    with start_worker(app, pool="solo", perform_ping_check=False, loglevel="WARNING") as worker:
        yield WorkerApp(
            app=app,
            add=add,
            worker=worker,
            push_channels=tuple(vqs_celery._push_channels),
        )


def test_poll_worker_processes_and_acknowledges_task(
    isolated_eqs: EmbeddedQueueDevServer,
    poll_worker: WorkerApp,
) -> None:
    result = poll_worker.add.apply_async((2, 5), queue="emails")
    assert result.get(timeout=10, disable_sync_subtasks=False) == 7
    _wait_for_acknowledged(isolated_eqs, poll_worker)

    assert len(isolated_eqs.state.messages) == 1
    assert isolated_eqs.state.messages[0].topic == _vqs_topic(poll_worker, "emails")
    assert isolated_eqs.state.messages[0].acknowledged is True


def test_push_worker_processes_sdk_callback_and_acknowledges_task(
    isolated_eqs: EmbeddedQueueDevServer,
    push_worker: WorkerApp,
) -> None:
    result = push_worker.add.apply_async((3, 4), queue="emails")
    _deliver_push(isolated_eqs, push_worker)
    assert result.get(timeout=10, disable_sync_subtasks=False) == 7
    _wait_for_acknowledged(isolated_eqs, push_worker)

    assert len(isolated_eqs.state.messages) == 1
    assert isolated_eqs.state.messages[0].topic == _vqs_topic(push_worker, "emails")
    assert isolated_eqs.state.messages[0].acknowledged is True


def test_push_callback_retries_until_worker_channel_is_available(
    isolated_eqs: EmbeddedQueueDevServer,
    push_worker: WorkerApp,
) -> None:
    result = push_worker.add.apply_async((8, 13), queue="emails")
    push_channels = list(vqs_celery._push_channels)
    vqs_celery._push_channels.clear()
    try:
        _deliver_push(isolated_eqs, push_worker)
    finally:
        vqs_celery._push_channels[:] = push_channels

    message = isolated_eqs.state.messages[0]
    assert message.acknowledged is False
    assert message.lease_deadline_by_consumer["celery"] == (
        isolated_eqs.state.now + timedelta(seconds=1)
    )

    cast("ManualEmbeddedQueueClock", isolated_eqs.state.clock).shift(1)
    _deliver_push(isolated_eqs, push_worker)
    assert result.get(timeout=10, disable_sync_subtasks=False) == 21
    _wait_for_acknowledged(isolated_eqs, push_worker)

    assert message.acknowledged is True
    assert message.delivery_count_by_consumer["celery"] == 2


def test_auto_worker_polls_locally(
    eqs: EmbeddedQueueDevServer,
) -> None:
    queue_name = "auto-poll-emails"
    _clear_celery_broker_state()
    eqs.reset()
    previous_vercel = os.environ.pop("VERCEL", None)
    try:
        app = _celery_app(
            "auto-poll-worker",
            "vercel://localhost//",
            eqs.base_url,
            queue_name=queue_name,
        )

        @app.task(name="auto-poll-worker.add")
        def add(left: int, right: int) -> int:
            return left + right

        app.finalize(auto=True)

        with start_worker(
            app,
            pool="solo",
            perform_ping_check=False,
            loglevel="WARNING",
        ) as worker:
            worker_app = WorkerApp(app=app, add=add, worker=worker)
            result = add.apply_async((5, 8), queue=queue_name)
            assert result.get(timeout=10, disable_sync_subtasks=False) == 13
            _wait_for_acknowledged(eqs, worker_app)

        assert len(eqs.state.messages) == 1
        assert eqs.state.messages[0].topic == _vqs_topic(worker_app, queue_name)
        assert eqs.state.messages[0].acknowledged is True
    finally:
        if previous_vercel is None:
            os.environ.pop("VERCEL", None)
        else:
            os.environ["VERCEL"] = previous_vercel
        eqs.reset()
        _clear_celery_broker_state()


def test_auto_worker_uses_push_on_vercel(
    eqs: EmbeddedQueueDevServer,
) -> None:
    queue_name = "auto-push-emails"
    _clear_celery_broker_state()
    eqs.reset()
    previous_vercel = os.environ.get("VERCEL")
    os.environ["VERCEL"] = "1"
    try:
        app = _celery_app(
            "auto-push-worker",
            "vercel://localhost//",
            eqs.base_url,
            queue_name=queue_name,
        )
        vqs_celery.register_celery_app_queues(app, start_worker=False)

        @app.task(name="auto-push-worker.add")
        def add(left: int, right: int) -> int:
            return left + right

        app.finalize(auto=True)

        with start_worker(
            app,
            pool="solo",
            perform_ping_check=False,
            loglevel="WARNING",
        ) as worker:
            worker_app = WorkerApp(
                app=app,
                add=add,
                worker=worker,
                push_channels=tuple(vqs_celery._push_channels),
            )
            result = add.apply_async((21, 34), queue=queue_name)
            _deliver_push(eqs, worker_app, queue=queue_name)
            assert result.get(timeout=10, disable_sync_subtasks=False) == 55
            _wait_for_acknowledged(eqs, worker_app)

        assert len(eqs.state.messages) == 1
        assert eqs.state.messages[0].topic == _vqs_topic(worker_app, queue_name)
        assert eqs.state.messages[0].acknowledged is True
    finally:
        if previous_vercel is None:
            os.environ.pop("VERCEL", None)
        else:
            os.environ["VERCEL"] = previous_vercel
        eqs.reset()
        _clear_celery_broker_state()


def _celery_app(
    name: str,
    broker_url: str,
    base_url: str,
    *,
    queue_name: str = "emails",
) -> Celery:
    public_vqs_celery.install_vercel_celery_integration(register_queues=False)
    app = Celery(name, broker=broker_url, backend="cache+memory://")
    app.conf.update(
        task_default_queue=queue_name,
        task_queues=(Queue(queue_name),),
        task_serializer="json",
        result_serializer="json",
        accept_content=("json",),
        broker_transport_options={
            "token": "token",
            "region": "iad1",
            "base_url": base_url,
            "deployment": ALL_DEPLOYMENTS,
            "consumer_group": "celery",
            "queue_name_prefix": f"celery-{name}-",
            "lease_duration": 30,
            "requeue_delay_seconds": 0,
            "timeout": 5,
        },
    )
    return app


def _clear_celery_broker_state() -> None:
    vqs_celery._set_default_broker_set_by_installer(value=False)
    vqs_celery._registered_app_queues.clear()
    vqs_celery._registered_queue_subscriptions.clear()
    vqs_celery._embedded_workers.clear()
    vqs_celery._registered_callbacks.clear()
    vqs_celery._push_channels.clear()
    vqs_celery._finalize_hook_state.installed = False


def _reset_celery_broker_state(push_worker: WorkerApp) -> None:
    clear_subscriptions()
    vqs_celery._set_default_broker_set_by_installer(value=False)
    vqs_celery._registered_app_queues.clear()
    vqs_celery._registered_queue_subscriptions.clear()
    vqs_celery._embedded_workers.clear()
    vqs_celery._registered_callbacks.clear()
    vqs_celery._push_channels[:] = list(push_worker.push_channels)
    vqs_celery._finalize_hook_state.installed = False
    vqs_celery.register_celery_app_queues(push_worker.app, start_worker=False)


def _vqs_topic(worker_app: WorkerApp, queue: str) -> str:
    return str(sanitize_name(f"celery-{worker_app.app.main}-{queue}"))


def _deliver_push(
    vqs_server: EmbeddedQueueDevServer,
    worker_app: WorkerApp,
    *,
    queue: str = "emails",
) -> None:
    delivery = next(
        vqs_server.iter_push_deliveries(_vqs_topic(worker_app, queue), "celery", lease_seconds=30),
        None,
    )
    assert delivery is not None
    client = SyncQueueClient(
        token="token",
        region="iad1",
        base_url=vqs_server.base_url,
        deployment=ALL_DEPLOYMENTS,
        timeout=5,
    )
    client._accept_and_handle(
        delivery.body,
        delivery.headers,
        transport=vqs_celery._KombuMessageTransport(),
        lease_duration=30,
    )


def _wait_for_acknowledged(vqs_server: EmbeddedQueueDevServer, worker_app: WorkerApp) -> None:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if vqs_server.state.messages and vqs_server.state.messages[0].acknowledged:
            return
        worker_app.worker.consumer.perform_pending_operations()
        time.sleep(0.01)
    raise AssertionError("queue message was not acknowledged")
