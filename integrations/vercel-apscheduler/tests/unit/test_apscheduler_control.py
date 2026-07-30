from __future__ import annotations

from typing import Any

import sys
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier, Lock
from types import SimpleNamespace
from unittest.mock import patch

import pytest

pytest.importorskip("apscheduler")

from apscheduler.schedulers.blocking import BlockingScheduler

import vercel.integrations.apscheduler.control as control_module
from vercel.integrations.apscheduler import (
    Control,
    ControlBackendConfigurationError,
    RedisControlBackend,
    VercelAPSchedulerOptions,
    WakeupPayload,
    adopt_scheduler,
    register_scheduler,
)
from vercel.integrations.apscheduler._payload import StartPayload
from vercel.queue import (
    DuplicateIdempotencyKeyError,
    Message,
    MessageMetadata,
    SanitizedName,
    get_subscriptions,
)
from vercel.queue.testing import clear_subscriptions

UTC = timezone.utc


class FakeControlBackend:
    def __init__(self) -> None:
        self._lock = Lock()
        self.state: control_module.ControlStateValue = "stopped"
        self.epoch = 0
        self.reference_time: datetime | None = None
        self.seeds: dict[str, tuple[int, str]] = {}
        self.activation_times: dict[str, tuple[int, datetime]] = {}

    def begin_start(
        self,
        deployment: str,
        subscribers: tuple[str, ...],
        reference_time: datetime,
    ) -> control_module._StartDecision:
        del deployment
        with self._lock:
            changed = self.state != "running"
            if changed:
                self.state = "running"
                self.epoch += 1
                self.reference_time = reference_time
            assert self.reference_time is not None
            for subscriber in subscribers:
                seed_epoch, _ = self.seeds.get(subscriber, (-1, ""))
                if seed_epoch != self.epoch:
                    self.seeds[subscriber] = (self.epoch, "pending")
                    self.activation_times.pop(subscriber, None)
            pending = tuple(
                subscriber
                for subscriber in subscribers
                if self.seeds[subscriber] == (self.epoch, "pending")
            )
            return control_module._StartDecision(
                epoch=self.epoch,
                reference_time=self.reference_time,
                changed=changed,
                pending_subscribers=pending,
            )

    def mark_seed_published(self, deployment: str, epoch: int, subscriber: str) -> None:
        del deployment
        with self._lock:
            if self.state == "running" and self.seeds.get(subscriber) == (
                epoch,
                "pending",
            ):
                self.seeds[subscriber] = (epoch, "published")

    def claim_seed(
        self,
        deployment: str,
        epoch: int,
        subscriber: str,
        activation_time: datetime,
    ) -> datetime | None:
        del deployment
        with self._lock:
            seed = self.seeds.get(subscriber)
            can_seed = (
                self.state == "running"
                and self.epoch == epoch
                and seed is not None
                and seed[0] == epoch
                and seed[1] != "active"
            )
            if not can_seed:
                return None
            claimed_epoch, claimed_time = self.activation_times.setdefault(
                subscriber,
                (epoch, activation_time),
            )
            return claimed_time if claimed_epoch == epoch else None

    def mark_seed_active(self, deployment: str, epoch: int, subscriber: str) -> None:
        del deployment
        with self._lock:
            if (
                self.state == "running"
                and self.epoch == epoch
                and self.seeds.get(subscriber, (-1, ""))[0] == epoch
            ):
                self.seeds[subscriber] = (epoch, "active")

    def is_running(self, deployment: str, epoch: int) -> bool:
        del deployment
        with self._lock:
            return self.state == "running" and self.epoch == epoch

    def stop(self, deployment: str, updated_at: datetime) -> bool:
        del deployment, updated_at
        with self._lock:
            changed = self.state == "running"
            self.state = "stopped"
            return changed

    def status(self, deployment: str) -> control_module.ControlStateValue:
        del deployment
        with self._lock:
            return self.state


@pytest.fixture(autouse=True)
def reset_control_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(control_module._CONTROL_STATE, "configured", None)
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_current")
    monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", "scheduler-a")
    monkeypatch.setenv("VERCEL_APSCHEDULER_SUBSCRIBERS", '["scheduler-a"]')
    clear_subscriptions()
    yield
    clear_subscriptions()


def test_start_payload_round_trip_preserves_epoch_and_reference_time() -> None:
    reference_time = datetime.fromisoformat("2026-07-29T10:00:00-07:00")

    restored = StartPayload.from_payload(
        StartPayload(epoch=3, reference_time=reference_time).to_payload()
    )

    assert restored.epoch == 3
    assert restored.reference_time == datetime(2026, 7, 29, 17, 0, tzinfo=UTC)


def test_repeated_start_publishes_only_one_seed() -> None:
    backend = FakeControlBackend()
    control = Control(backend=backend)

    with patch.object(control_module.vqs_sync, "send", return_value="msg_1") as send:
        first = control.start()
        second = control.start()

    assert first.changed is True
    assert second.changed is False
    send.assert_called_once()
    assert send.call_args.args[0] == "__aps_scheduler-a_start"
    assert send.call_args.kwargs == {
        "deployment": "dpl_current",
        "idempotency_key": "aps:start:dpl_current:1:scheduler-a",
        "retention": 604800,
    }
    assert StartPayload.from_payload(send.call_args.args[1]).epoch == 1


def test_concurrent_start_uses_queue_idempotency_as_one_seed() -> None:
    backend = FakeControlBackend()
    control = Control(backend=backend)
    rendezvous = Barrier(2)
    accepted_keys: set[str] = set()
    accepted_lock = Lock()

    def send_once(topic: str, payload: dict[str, Any], **kwargs: Any) -> str:
        del topic, payload
        rendezvous.wait(timeout=5)
        key = kwargs["idempotency_key"]
        with accepted_lock:
            if key in accepted_keys:
                raise DuplicateIdempotencyKeyError("duplicate")
            accepted_keys.add(key)
        return "msg_1"

    with (
        patch.object(control_module.vqs_sync, "send", side_effect=send_once),
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        results = list(executor.map(lambda _: control.start(), range(2)))

    assert accepted_keys == {"aps:start:dpl_current:1:scheduler-a"}
    assert sorted(result.changed for result in results) == [False, True]
    assert backend.seeds["scheduler-a"] == (1, "published")


def test_failed_seed_publish_retries_same_epoch_and_idempotency_key() -> None:
    backend = FakeControlBackend()
    control = Control(backend=backend)

    with patch.object(
        control_module.vqs_sync,
        "send",
        side_effect=[RuntimeError("queue unavailable"), "msg_1"],
    ) as send:
        with pytest.raises(RuntimeError, match="queue unavailable"):
            control.start()
        result = control.start()

    assert result.changed is False
    assert [call.kwargs["idempotency_key"] for call in send.call_args_list] == [
        "aps:start:dpl_current:1:scheduler-a",
        "aps:start:dpl_current:1:scheduler-a",
    ]
    first_payload = StartPayload.from_payload(send.call_args_list[0].args[1])
    second_payload = StartPayload.from_payload(send.call_args_list[1].args[1])
    assert first_payload == second_payload


def test_stop_then_start_creates_a_new_epoch_and_fences_the_old_one() -> None:
    backend = FakeControlBackend()
    control = Control(backend=backend)

    with patch.object(control_module.vqs_sync, "send", return_value="msg") as send:
        control.start()
        stopped = control.stop()
        restarted = control.start()

    assert stopped.changed is True
    assert restarted.changed is True
    assert backend.is_running("dpl_current", 1) is False
    assert backend.is_running("dpl_current", 2) is True
    assert [call.kwargs["idempotency_key"] for call in send.call_args_list] == [
        "aps:start:dpl_current:1:scheduler-a",
        "aps:start:dpl_current:2:scheduler-a",
    ]


def test_start_can_target_an_explicit_deployment() -> None:
    backend = FakeControlBackend()
    control = Control(backend=backend)

    with patch.object(control_module.vqs_sync, "send", return_value="msg") as send:
        result = control.start(deployment="dpl_target")

    assert result.deployment == "dpl_target"
    assert send.call_args.kwargs["deployment"] == "dpl_target"
    assert send.call_args.kwargs["idempotency_key"].startswith("aps:start:dpl_target:")


def test_controlled_start_claims_activation_time_and_marks_seed_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeControlBackend()
    control = Control(backend=backend)
    control_module._configure_control(control)
    reference_time = datetime(2026, 7, 29, 17, 0, tzinfo=UTC)
    decision = backend.begin_start(
        "dpl_current",
        ("scheduler-a",),
        reference_time,
    )
    scheduler = BlockingScheduler(timezone=UTC)
    adapter = adopt_scheduler(scheduler, VercelAPSchedulerOptions())
    register_scheduler(scheduler)
    start_subscription = get_subscriptions()[0]
    message: Message[dict[str, Any]] = Message(
        payload=StartPayload(
            epoch=decision.epoch,
            reference_time=reference_time,
        ).to_payload(),
        metadata=MessageMetadata(
            message_id="msg_start",
            delivery_count=1,
            created_at=reference_time.replace(hour=18),
            topic=start_subscription.topic,
            consumer_group=SanitizedName(start_subscription.consumer_group),
        ),
    )

    activation_time = reference_time.replace(hour=18, minute=1)
    with (
        patch("vercel.integrations.apscheduler._subscriber.datetime") as subscriber_datetime,
        patch.object(adapter, "seed") as seed,
    ):
        subscriber_datetime.now.return_value = activation_time
        start_subscription.func(message)

    seed.assert_called_once_with(now=activation_time, kind="start", epoch=1)
    assert backend.seeds["scheduler-a"] == (1, "active")


def test_seed_activation_time_is_stable_across_delivery_retries() -> None:
    backend = FakeControlBackend()
    reference_time = datetime(2026, 7, 29, 17, 0, tzinfo=UTC)
    first_delivery = reference_time + timedelta(minutes=5)
    retry_delivery = first_delivery + timedelta(minutes=1)
    decision = backend.begin_start(
        "dpl_current",
        ("scheduler-a",),
        reference_time,
    )

    first_claim = backend.claim_seed(
        "dpl_current",
        decision.epoch,
        "scheduler-a",
        first_delivery,
    )
    retry_claim = backend.claim_seed(
        "dpl_current",
        decision.epoch,
        "scheduler-a",
        retry_delivery,
    )

    assert first_claim == first_delivery
    assert retry_claim == first_delivery


def test_controlled_wakeup_is_fenced_again_before_successor_publish() -> None:
    backend = FakeControlBackend()
    control = Control(backend=backend)
    control_module._configure_control(control)
    now = datetime(2026, 7, 29, 17, 0, tzinfo=UTC)
    decision = backend.begin_start("dpl_current", ("scheduler-a",), now)
    scheduler = BlockingScheduler(timezone=UTC)
    adapter = adopt_scheduler(scheduler, VercelAPSchedulerOptions())
    register_scheduler(scheduler)
    wake_subscription = get_subscriptions()[1]
    message: Message[dict[str, Any]] = Message(
        payload=WakeupPayload(
            scheduler_id="scheduler-a",
            logical_time=now,
            epoch=decision.epoch,
        ).to_payload(),
        metadata=MessageMetadata(
            message_id="msg_wake",
            delivery_count=1,
            created_at=now,
            topic=wake_subscription.topic,
            consumer_group=SanitizedName(wake_subscription.consumer_group),
        ),
    )

    with patch.object(adapter, "process_wakeup") as process_wakeup:
        wake_subscription.func(message)

    publish_guard = process_wakeup.call_args.kwargs["publish_guard"]
    assert publish_guard() is True
    control.stop()
    assert publish_guard() is False


def test_stale_epoch_wakeup_is_ignored_after_restart() -> None:
    backend = FakeControlBackend()
    control = Control(backend=backend)
    control_module._configure_control(control)
    now = datetime(2026, 7, 29, 17, 0, tzinfo=UTC)
    backend.begin_start("dpl_current", ("scheduler-a",), now)
    control.stop()
    with patch.object(control_module.vqs_sync, "send", return_value="msg"):
        control.start()
    scheduler = BlockingScheduler(timezone=UTC)
    adapter = adopt_scheduler(scheduler, VercelAPSchedulerOptions())
    register_scheduler(scheduler)
    wake_subscription = get_subscriptions()[1]
    message: Message[dict[str, Any]] = Message(
        payload=WakeupPayload(
            scheduler_id="scheduler-a",
            logical_time=now,
            epoch=1,
        ).to_payload(),
        metadata=MessageMetadata(
            message_id="msg_old",
            delivery_count=1,
            created_at=now,
            topic=wake_subscription.topic,
            consumer_group=SanitizedName(wake_subscription.consumer_group),
        ),
    )

    with patch.object(adapter, "process_wakeup") as process_wakeup:
        wake_subscription.func(message)

    process_wakeup.assert_not_called()


def test_redis_backend_uses_redis_url_lazily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    client = SimpleNamespace(hget=lambda *_: None)

    class FakeRedis:
        @classmethod
        def from_url(cls, url: str, **kwargs: Any) -> Any:
            calls.append((url, kwargs))
            return client

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(Redis=FakeRedis))
    monkeypatch.setenv("REDIS_URL", "redis://example.test:6379/2")
    backend = RedisControlBackend()

    assert calls == []
    assert backend.status("dpl_current") == "stopped"
    assert calls == [
        ("redis://example.test:6379/2", {"decode_responses": True}),
    ]


def test_redis_backend_explicit_host_overrides_redis_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    client = SimpleNamespace(hget=lambda *_: None)

    class FakeRedis:
        def __new__(cls, **kwargs: Any) -> Any:
            calls.append(kwargs)
            return client

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(Redis=FakeRedis))
    monkeypatch.setenv("REDIS_URL", "redis://ignored.test")
    backend = RedisControlBackend(host="redis.internal", ssl=True)

    assert backend.status("dpl_current") == "stopped"
    assert calls == [
        {
            "host": "redis.internal",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": None,
            "ssl": True,
            "decode_responses": True,
        }
    ]


def test_redis_backend_requires_host_or_redis_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRedis:
        pass

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(Redis=FakeRedis))
    monkeypatch.delenv("REDIS_URL", raising=False)

    with pytest.raises(
        ControlBackendConfigurationError,
        match=r"host=.*REDIS_URL",
    ):
        RedisControlBackend().status("dpl_current")
