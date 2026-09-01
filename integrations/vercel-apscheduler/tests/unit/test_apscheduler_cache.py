"""Cache backend conformance: driver semantics and end-to-end flows.

Runs entirely on the Runtime Cache client's in-memory fallback — no
infrastructure. The driver-level tests pin the adoption and fencing rules;
the end-to-end tests drive the real subscription handlers against a real
``CacheDriver``/``CacheJobCoordinator``.
"""

from __future__ import annotations

from typing import Any

from datetime import datetime, timedelta, timezone
from sys import modules
from types import ModuleType
from unittest.mock import patch

import pytest
import time_machine
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.blocking import BlockingScheduler

import vercel.cache.runtime_cache as runtime_cache_module
from vercel.cache import get_cache
from vercel.integrations.apscheduler import (
    _adapter as _adapter_module,
    _subscriber,
    install_vercel_apscheduler_integration,
)
from vercel.integrations.apscheduler._adapter import get_adapter
from vercel.integrations.apscheduler._backends.cache import (
    CacheBackend,
    CacheDriver,
    CacheJobStore,
)
from vercel.integrations.apscheduler._control import LifecyclePayload
from vercel.integrations.apscheduler._options import development_deployment_id
from vercel.integrations.apscheduler._payload import StartPayload, WakeupPayload
from vercel.integrations.apscheduler._subscriber import register_scheduler
from vercel.integrations.apscheduler._types import (
    PROVENANCE_DECLARED,
    APSchedulerConfigurationError,
    NamespaceFencedError,
    WakeToken,
)
from vercel.queue import Message, MessageMetadata, SanitizedName, get_subscriptions
from vercel.queue.testing import clear_subscriptions

UTC = timezone.utc
CACHE_SCHEDULER_MODULE = "cache_test_scheduler"
# No subscriber id env: cache identity falls back to the declared mapping.
CACHE_SCHEDULER_ID = "default"

_EXECUTIONS: list[str] = []


def durable_cache_job() -> None:
    _EXECUTIONS.append("ran")


@pytest.fixture(autouse=True)
def runtime(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_test")
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
    monkeypatch.delenv("VERCEL_PYTHON_SUBSCRIBER_ID", raising=False)
    monkeypatch.delenv("VERCEL_APSCHEDULER_BACKEND", raising=False)
    monkeypatch.delenv("VERCEL_APSCHEDULER_PREVIEW_IDLE_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv(
        "VERCEL_APSCHEDULER_SUBSCRIBERS",
        (f'[{{"id":"{CACHE_SCHEDULER_ID}","entrypoint":"{CACHE_SCHEDULER_MODULE}:scheduler"}}]'),
    )
    monkeypatch.delenv("VERCEL_APSCHEDULER_DISCOVERY", raising=False)
    monkeypatch.delenv("VERCEL_SERVICE_TYPE", raising=False)
    monkeypatch.delenv("VERCEL_SERVICE_TRIGGER", raising=False)
    monkeypatch.delenv("VERCEL_DEV_QUEUE_SERVING", raising=False)
    monkeypatch.setitem(modules, CACHE_SCHEDULER_MODULE, ModuleType(CACHE_SCHEDULER_MODULE))
    # A fresh in-memory cache per test: the client caches its fallback
    # instance in module globals.
    for attribute in (
        "_in_memory_cache_instance",
        "_async_in_memory_cache_instance",
        "_cached_cache_instance",
        "_cached_async_cache_instance",
    ):
        monkeypatch.setattr(runtime_cache_module, attribute, None)
    _EXECUTIONS.clear()
    _clear_registrations()
    install_vercel_apscheduler_integration(register_queues=False)
    yield
    _clear_registrations()


def _clear_registrations() -> None:
    clear_subscriptions()
    _subscriber._registered_schedulers.clear()
    _subscriber._registered_callbacks.clear()
    _adapter_module._ACTIVE_IDENTITIES.clear()
    _adapter_module._PATCH_STATE.register_queues = False


def message(
    payload: dict[str, Any],
    *,
    message_id: str,
    topic: str,
    consumer_group: str,
) -> Message[dict[str, Any]]:
    return Message(
        payload=payload,
        metadata=MessageMetadata(
            message_id=message_id,
            delivery_count=1,
            created_at=datetime.now(UTC),
            topic=topic,
            consumer_group=SanitizedName(consumer_group),
        ),
    )


def cache_driver(deployment: str = "dpl_test") -> CacheDriver:
    return CacheDriver(
        scope="prj_test:production",
        scheduler_id="conformance",
        deployment=deployment,
    )


def test_cache_driver_start_is_idempotent_and_pause_fences() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    first = driver.start(now)
    assert first.changed
    assert first.generation == 1
    assert first.start_status == "pending"

    again = driver.start(now)
    assert not again.changed
    assert again.generation == 1

    assert driver.pause(now)
    assert driver.snapshot().state == "paused"

    resumed = driver.start(now)
    assert resumed.changed
    assert resumed.generation == 2


def test_cache_driver_start_claim_finish_reserves_first_wake() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)
    next_time = now + timedelta(minutes=5)

    decision = driver.start(now)
    driver.mark_start_published(decision.generation, now)

    claim = driver.claim_start(decision.generation, "owner-1", now)
    assert claim.state == "claimed"

    finish = driver.finish_start(decision.generation, "owner-1", next_time, now)
    assert finish.state == "advanced"
    assert finish.wake is not None
    assert finish.wake.sequence == 1
    assert finish.wake.logical_time == next_time

    # The start is settled: a duplicate delivery must not re-run it.
    assert driver.claim_start(decision.generation, "owner-2", now).state == "stale"

    driver.mark_wake_published(decision.generation, 1, now)
    wake_claim = driver.claim_wake(finish.wake, "owner-3", now)
    assert wake_claim.state == "claimed"

    successor = driver.finish_wake(finish.wake, "owner-3", next_time + timedelta(minutes=5), now)
    assert successor.state == "advanced"
    assert successor.wake is not None
    assert successor.wake.sequence == 2


def test_cache_driver_adopts_newer_generation_start() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    # An empty document (eviction, or another process's memory in dev) must
    # not strand the chain: the message is the authority.
    claim = driver.claim_start(3, "owner-1", now)
    assert claim.state == "claimed"
    snapshot = driver.snapshot()
    assert snapshot.generation == 3
    assert snapshot.state == "running"

    # Older start deliveries are fenced by the adopted generation.
    assert driver.claim_start(2, "owner-2", now).state == "stale"


def test_cache_driver_adopts_newer_wake_and_fences_older() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)
    token = WakeToken(generation=2, sequence=5, logical_time=now)

    claim = driver.claim_wake(token, "owner-1", now)
    assert claim.state == "claimed"

    finish = driver.finish_wake(token, "owner-1", now + timedelta(minutes=1), now)
    assert finish.state == "advanced"
    assert finish.wake is not None
    assert finish.wake.sequence == 6

    stale_duplicate = driver.claim_wake(token, "owner-2", now)
    assert stale_duplicate.state == "stale"

    older = WakeToken(generation=1, sequence=9, logical_time=now)
    assert driver.claim_wake(older, "owner-3", now).state == "stale"


def test_cache_driver_resume_generation_revives_paused_document() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    driver.start(now)
    driver.pause(now)

    # Same-generation deliveries stay fenced while paused.
    fenced = WakeToken(generation=1, sequence=1, logical_time=now)
    assert driver.claim_wake(fenced, "owner-1", now).state == "stale"
    assert driver.claim_start(1, "owner-2", now).state == "stale"

    # A resume mints generation 2 elsewhere; its deliveries win here.
    assert driver.claim_start(2, "owner-3", now).state == "claimed"
    assert driver.snapshot().state == "running"


def test_cache_driver_remote_pause_drops_stale_but_honors_evicted_issuer() -> None:
    driver = cache_driver()
    t0 = datetime.now(UTC)

    driver.start(t0)  # generation 1
    driver.pause(t0)
    resumed = driver.start(t0 + timedelta(seconds=10))
    assert resumed.generation == 2

    # The queue redelivers the generation-1 pause after the resume: both
    # indicators date it before the current activation, so it is dropped.
    assert not driver.apply_remote_pause(1, t0, t0 + timedelta(seconds=20))
    assert driver.snapshot().state == "running"

    # A pause from an issuer whose own document was evicted under-reads the
    # generation as 0, but its fresh issue time proves it is not stale.
    assert driver.apply_remote_pause(0, t0 + timedelta(seconds=30), t0 + timedelta(seconds=30))
    assert driver.snapshot().state == "paused"


def test_cache_driver_idle_deadline_demotes_at_claim() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    decision = driver.start(now, idle_timeout_seconds=60)
    driver.mark_start_published(decision.generation, now)
    claim = driver.claim_start(decision.generation, "owner-1", now)
    assert claim.state == "claimed"
    finish = driver.finish_start(decision.generation, "owner-1", now + timedelta(minutes=5), now)
    assert finish.wake is not None

    after_deadline = now + timedelta(seconds=120)
    assert driver.claim_wake(finish.wake, "owner-2", after_deadline).state == "stale"
    assert driver.snapshot().state == "inactive"


def test_cache_driver_production_start_clears_idle_deadline() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    driver.start(now, idle_timeout_seconds=60)
    driver.start(now, idle_timeout_seconds=None)

    much_later = now + timedelta(hours=2)
    assert not driver._demote_if_idle(driver._read(), much_later)
    assert driver.snapshot().state == "running"


def test_cache_driver_repair_overdue_wake() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    decision = driver.start(now)
    driver.claim_start(decision.generation, "owner-1", now)
    finish = driver.finish_start(decision.generation, "owner-1", now, now)
    assert finish.wake is not None
    driver.mark_wake_published(decision.generation, 1, now)

    too_soon = driver.repair_overdue_wake(now + timedelta(minutes=5))
    assert too_soon is None

    repaired = driver.repair_overdue_wake(now + timedelta(minutes=11))
    assert repaired is not None
    assert repaired.status == "pending"


def test_cache_driver_repairs_overdue_published_start() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    decision = driver.start(now)
    driver.mark_start_published(decision.generation, now)

    # Within the grace the message is presumed in flight or merely delayed.
    touched = driver.auto_activate(now + timedelta(minutes=5))
    assert touched.start_status == "published"

    # Past it the activation touch demotes the start so the regular publish
    # path resends it under the same generation.
    repaired = driver.auto_activate(now + timedelta(minutes=11))
    assert not repaired.changed
    assert repaired.generation == decision.generation
    assert repaired.start_status == "pending"


def test_cache_driver_repairs_stranded_processing_start() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    decision = driver.start(now)
    driver.mark_start_published(decision.generation, now)
    assert driver.claim_start(decision.generation, "owner-1", now).state == "claimed"

    # Steady traffic keeps rewriting the document, so updated_at never ages
    # out; the grace must run from the claim's own timestamp instead.
    touched = driver.auto_activate(now + timedelta(minutes=5))
    assert touched.start_status == "processing"

    repaired = driver.auto_activate(now + timedelta(minutes=11))
    assert repaired.start_status == "pending"

    # The republished start is claimable again, and the fresh claim is not
    # instantly demoted by the very next touch.
    driver.mark_start_published(decision.generation, now + timedelta(minutes=11))
    reclaim = driver.claim_start(decision.generation, "owner-2", now + timedelta(minutes=12))
    assert reclaim.state == "claimed"
    after = driver.auto_activate(now + timedelta(minutes=13))
    assert after.start_status == "processing"


def test_cache_driver_foreign_owner_is_fenced_without_takeover() -> None:
    ours = cache_driver("dpl_a")
    theirs = cache_driver("dpl_b")
    now = datetime.now(UTC)

    ours.start(now)
    decision = theirs.auto_activate(now)
    assert not decision.owned

    takeover = theirs.auto_activate(now, takeover_allowed=True)
    assert takeover.owned
    assert takeover.generation == 2
    assert theirs.owner_deployment() == "dpl_b"


def test_cache_driver_mark_reconciled_is_owner_fenced() -> None:
    ours = cache_driver("dpl_a")
    theirs = cache_driver("dpl_b")
    store = CacheJobStore()
    store.bind_namespace(scope="prj_test:production", scheduler_id="conformance")
    ours.attach_store(store)
    theirs.attach_store(store)
    now = datetime.now(UTC)

    ours.start(now)
    assert not theirs.mark_reconciled("dpl_b", now)
    assert ours.reconciled_deployment() is None

    assert ours.mark_reconciled("dpl_a", now)
    assert ours.reconciled_deployment() == "dpl_a"


def test_cache_reconcile_marker_shares_the_jobs_document_fate() -> None:
    driver = cache_driver("dpl_a")
    store = CacheJobStore()
    store.bind_namespace(scope="prj_test:production", scheduler_id="conformance")
    driver.attach_store(store)
    now = datetime.now(UTC)

    driver.start(now)
    assert driver.mark_reconciled("dpl_a", now)
    assert driver.reconciled_deployment() == "dpl_a"

    # Evicting the jobs document must clear the marker with it, so a driver
    # document kept fresh by bridge hops cannot vouch for a reaped store.
    assert store.doc_key is not None
    get_cache().delete(store.doc_key)
    assert driver.reconciled_deployment() is None


def test_cache_paused_document_is_touched_by_the_activation_hook() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    driver.start(now)
    driver.pause(now)
    before = driver._read().get("updated_at")

    later = now + timedelta(minutes=7)
    decision = driver.auto_activate(later)
    assert decision.state == "paused"
    assert not decision.changed
    # The paused document was rewritten: its TTL clock restarted, so pause
    # survives for as long as the deployment serves traffic.
    assert driver._read().get("updated_at") != before


def started_cache_scheduler() -> tuple[BlockingScheduler, Any, StartPayload]:
    scheduler = BlockingScheduler(timezone=UTC)
    scheduler.scheduled_job(
        "interval",
        minutes=5,
        id="tick",
    )(durable_cache_job)
    modules[CACHE_SCHEDULER_MODULE].__dict__["scheduler"] = scheduler
    register_scheduler(scheduler)
    adapter = get_adapter(scheduler)
    assert adapter is not None
    assert adapter.backend.name == "cache"

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_start",
    ) as send:
        scheduler.start()

    send.assert_called_once()
    topic, payload = send.call_args.args[0], send.call_args.args[1]
    assert topic == adapter.identity.start_topic
    return scheduler, adapter, StartPayload.from_payload(payload)


def test_cache_end_to_end_start_activates_and_reserves_first_wake() -> None:
    _scheduler, adapter, start_payload = started_cache_scheduler()
    start_subscription = get_subscriptions()[0]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ) as send:
        start_subscription.func(
            message(
                StartPayload(
                    scheduler_id=start_payload.scheduler_id,
                    generation=start_payload.generation,
                ).to_payload(),
                message_id="start-1",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )

    send.assert_called_once()
    wake_payload = WakeupPayload.from_payload(send.call_args.args[1])
    assert wake_payload.generation == start_payload.generation
    assert wake_payload.sequence == 1

    snapshot = adapter.driver.snapshot()
    assert snapshot.state == "running"
    assert snapshot.start_status == "active"

    jobs, undecodable = adapter.coordinator.get_all_jobs_with_revisions()
    assert undecodable == []
    assert [(job.id, provenance) for job, _, provenance in jobs] == [("tick", PROVENANCE_DECLARED)]


def test_cache_end_to_end_wake_executes_job_and_reserves_successor() -> None:
    _scheduler, _adapter, start_payload = started_cache_scheduler()
    start_subscription, wake_subscription = get_subscriptions()[:2]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ) as send:
        start_subscription.func(
            message(
                start_payload.to_payload(),
                message_id="start-1",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )
    first_wake = WakeupPayload.from_payload(send.call_args.args[1])

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake_2",
    ) as send:
        wake_subscription.func(
            message(
                first_wake.to_payload(),
                message_id="wake-1",
                topic=wake_subscription.topic,
                consumer_group=wake_subscription.consumer_group,
            )
        )

    assert _EXECUTIONS == ["ran"]
    send.assert_called_once()
    successor = WakeupPayload.from_payload(send.call_args.args[1])
    assert successor.sequence == first_wake.sequence + 1
    assert successor.logical_time > first_wake.logical_time

    # A duplicate delivery of the consumed wake is acknowledged silently.
    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
    ) as send:
        wake_subscription.func(
            message(
                first_wake.to_payload(),
                message_id="wake-1-dup",
                topic=wake_subscription.topic,
                consumer_group=wake_subscription.consumer_group,
            )
        )
    send.assert_not_called()
    assert _EXECUTIONS == ["ran"]


def test_cache_pause_publishes_control_message_and_subscriber_applies_it() -> None:
    scheduler, adapter, _payload = started_cache_scheduler()
    start_subscription = get_subscriptions()[0]

    with (
        time_machine.travel(datetime.now(UTC), tick=False),
        patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg_ctl",
        ) as send,
    ):
        scheduler.pause()

    send.assert_called_once()
    control = LifecyclePayload.from_payload(send.call_args.args[1])
    assert control.action == "pause"
    assert control.generation == 1
    assert send.call_args.args[0] == adapter.identity.start_topic

    # Simulate the queue-serving process: wipe the local document (its memory
    # is separate under vercel dev) and apply the control message.
    get_cache().delete(adapter.driver.key)
    assert adapter.driver.snapshot().state == "inactive"

    start_subscription.func(
        message(
            control.to_payload(),
            message_id="ctl-1",
            topic=start_subscription.topic,
            consumer_group=start_subscription.consumer_group,
        )
    )
    assert adapter.driver.snapshot().state == "paused"

    # A resume elsewhere arrives as a start message minting generation 2; a
    # late redelivery of the generation-1 pause must not settle the chain
    # back to paused.
    with (
        time_machine.travel(control.issued_at + timedelta(seconds=1), tick=False),
        patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg_wake_after_resume",
        ),
    ):
        start_subscription.func(
            message(
                StartPayload(
                    scheduler_id=adapter.identity.scheduler_id,
                    generation=2,
                ).to_payload(),
                message_id="start-2",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )
    assert adapter.driver.snapshot().state == "running"
    assert adapter.driver.snapshot().generation == 2

    start_subscription.func(
        message(
            control.to_payload(),
            message_id="ctl-1-dup",
            topic=start_subscription.topic,
            consumer_group=start_subscription.consumer_group,
        )
    )
    assert adapter.driver.snapshot().state == "running"


def test_cache_eviction_self_heals_from_the_next_wake() -> None:
    _scheduler, adapter, start_payload = started_cache_scheduler()
    start_subscription, wake_subscription = get_subscriptions()[:2]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ) as send:
        start_subscription.func(
            message(
                start_payload.to_payload(),
                message_id="start-1",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )
    first_wake = WakeupPayload.from_payload(send.call_args.args[1])

    # Total eviction: both documents disappear.
    get_cache().delete(adapter.driver.key)
    get_cache().delete(adapter.coordinator.store.doc_key)

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake_2",
    ) as send:
        wake_subscription.func(
            message(
                first_wake.to_payload(),
                message_id="wake-1",
                topic=wake_subscription.topic,
                consumer_group=wake_subscription.consumer_group,
            )
        )

    # The wake was adopted, the declared job restored from code, and the
    # chain continued with a successor.
    assert _EXECUTIONS == ["ran"]
    send.assert_called_once()
    successor = WakeupPayload.from_payload(send.call_args.args[1])
    assert successor.sequence == first_wake.sequence + 1

    jobs, _ = adapter.coordinator.get_all_jobs_with_revisions()
    assert [job.id for job, _, _ in jobs] == ["tick"]


def test_cache_coordinator_cas_and_quarantine() -> None:
    _scheduler, adapter, start_payload = started_cache_scheduler()
    start_subscription = get_subscriptions()[0]
    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ):
        start_subscription.func(
            message(
                start_payload.to_payload(),
                message_id="start-1",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )

    coordinator = adapter.coordinator
    jobs, _ = coordinator.get_all_jobs_with_revisions()
    (job, revision, _provenance) = jobs[0]

    assert not coordinator.cas_update_job(job, revision + 41)
    assert coordinator.cas_update_job(job, revision)

    # Corrupt the persisted record: it must be reported undecodable, and due
    # planning must quarantine rather than crash the chain.
    store = coordinator.store
    doc = store._load()
    doc["jobs"]["tick"]["state"] = "bm90LXBpY2tsZQ=="  # b"not-pickle"
    store._store(doc)

    jobs, undecodable = coordinator.get_all_jobs_with_revisions()
    assert jobs == []
    assert [record[0] for record in undecodable] == ["tick"]

    due = coordinator.get_due_jobs_with_revisions(datetime.now(UTC) + timedelta(days=1))
    assert due == []
    assert store._load()["jobs"]["tick"]["quarantined"] is True


def test_cache_jobs_document_eviction_alone_triggers_reconcile() -> None:
    _scheduler, adapter, start_payload = started_cache_scheduler()
    start_subscription, wake_subscription = get_subscriptions()[:2]

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ) as send:
        start_subscription.func(
            message(
                start_payload.to_payload(),
                message_id="start-1",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )
    first_wake = WakeupPayload.from_payload(send.call_args.args[1])

    # Only the jobs document is reaped; the driver document stays fresh
    # (e.g. kept alive by bridge hops on a sparse schedule). The marker
    # lives in the jobs document, so reconciliation must re-run.
    get_cache().delete(adapter.coordinator.store.doc_key)
    assert adapter.driver.reconciled_deployment() is None

    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake_2",
    ) as send:
        wake_subscription.func(
            message(
                first_wake.to_payload(),
                message_id="wake-1",
                topic=wake_subscription.topic,
                consumer_group=wake_subscription.consumer_group,
            )
        )

    assert _EXECUTIONS == ["ran"]
    jobs, _ = adapter.coordinator.get_all_jobs_with_revisions()
    assert [job.id for job, _, _ in jobs] == ["tick"]


def test_cache_dormant_finish_keeps_the_sequence_watermark() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    decision = driver.start(now)
    driver.claim_start(decision.generation, "owner-1", now)
    finish = driver.finish_start(decision.generation, "owner-1", now, now)
    token = finish.wake
    assert token is not None
    assert token.sequence == 1
    driver.mark_wake_published(decision.generation, 1, now)
    driver.claim_wake(token, "owner-2", now)
    dormant = driver.finish_wake(token, "owner-2", None, now)
    assert dormant.state == "advanced"
    assert dormant.wake is None

    # The consumed position stays fenced even with no current token.
    assert driver.claim_wake(token, "owner-3", now).state == "stale"

    # A later rearm continues the sequence instead of reusing consumed
    # idempotency-key positions the queue would silently dedup.
    driver.rearm_wake(now + timedelta(minutes=5), now)
    current = driver.snapshot().current_wake
    assert current is not None
    assert current.sequence == 2


def test_cache_declared_add_rearms_a_dormant_chain() -> None:
    _scheduler, adapter, start_payload = started_cache_scheduler()
    start_subscription = get_subscriptions()[0]
    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="msg_wake",
    ):
        start_subscription.func(
            message(
                start_payload.to_payload(),
                message_id="start-1",
                topic=start_subscription.topic,
                consumer_group=start_subscription.consumer_group,
            )
        )

    # Force dormancy (active generation, consumed watermark, no token) and
    # evict the jobs document — a declaration restored by reconciliation
    # must mint the wake nothing else will.
    doc = adapter.driver._read()
    doc["current"] = None
    doc["last_sequence"] = 4
    adapter.driver._write(doc, datetime.now(UTC))
    assert adapter.coordinator.store.doc_key is not None
    get_cache().delete(adapter.coordinator.store.doc_key)

    declared = adapter._declared_jobs["tick"]
    adapter.coordinator.add_job(declared)

    current = adapter.driver.snapshot().current_wake
    assert current is not None
    assert current.sequence == 5
    assert current.status == "pending"


def test_cache_rearm_during_processing_folds_into_the_successor() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    decision = driver.start(now)
    driver.claim_start(decision.generation, "owner-1", now)
    finish = driver.finish_start(decision.generation, "owner-1", now + timedelta(minutes=10), now)
    token = finish.wake
    assert token is not None
    driver.mark_wake_published(decision.generation, 1, now)
    assert driver.claim_wake(token, "owner-2", now).state == "claimed"

    # A mutation racing the in-flight wake must not replace its token: both
    # sides would publish different payloads under one idempotency key.
    earlier = now + timedelta(minutes=2)
    driver.rearm_wake(earlier, now)
    current = driver.snapshot().current_wake
    assert current is not None
    assert current.sequence == 1
    assert current.status == "processing"

    # The finisher folds the earlier candidate into the one successor.
    successor = driver.finish_wake(token, "owner-2", now + timedelta(minutes=20), now)
    assert successor.wake is not None
    assert successor.wake.sequence == 2
    assert successor.wake.logical_time == earlier


def test_cache_demoted_deployment_job_writes_are_fenced() -> None:
    _scheduler, adapter, _payload = started_cache_scheduler()

    doc = adapter.driver._read()
    doc["owner_deployment"] = "dpl_other"
    adapter.driver._write(doc, datetime.now(UTC))

    with pytest.raises(NamespaceFencedError):
        adapter.coordinator.remove_job("tick")


def test_cache_processing_claim_is_busy_until_the_grace_lapses() -> None:
    driver = cache_driver()
    now = datetime.now(UTC)

    decision = driver.start(now)
    driver.claim_start(decision.generation, "owner-1", now)
    finish = driver.finish_start(decision.generation, "owner-1", now, now)
    token = finish.wake
    assert token is not None
    driver.mark_wake_published(decision.generation, 1, now)
    assert driver.claim_wake(token, "owner-2", now).state == "claimed"

    # A redelivery while the handler is alive retries instead of re-running.
    assert driver.claim_wake(token, "owner-3", now).state == "busy"

    # Past the grace the owner is presumed crashed and the claim is retaken.
    late = now + timedelta(minutes=16)
    assert driver.claim_wake(token, "owner-4", late).state == "claimed"


def test_cache_identity_resolves_from_declared_subscriber_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A publishing process has no VERCEL_PYTHON_SUBSCRIBER_ID; the identity
    # must come from the builder's declared mapping so its start and wake
    # publishes land on the topics the sidecar actually serves.
    monkeypatch.setenv(
        "VERCEL_APSCHEDULER_SUBSCRIBERS",
        f'[{{"id":"jobs_scheduler","entrypoint":"{CACHE_SCHEDULER_MODULE}:scheduler"}}]',
    )
    scheduler = BlockingScheduler()
    monkeypatch.setattr(modules[CACHE_SCHEDULER_MODULE], "scheduler", scheduler, raising=False)

    adapter = get_adapter(scheduler)
    assert adapter is not None
    assert adapter.identity.scheduler_id == "jobs_scheduler"
    assert adapter.identity.start_topic == "__aps_jobs_scheduler_start"


def test_cache_identity_prefers_the_builder_assigned_subscriber_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", "sidecar_identity")
    monkeypatch.setenv(
        "VERCEL_APSCHEDULER_SUBSCRIBERS",
        f'[{{"id":"jobs_scheduler","entrypoint":"{CACHE_SCHEDULER_MODULE}:scheduler"}}]',
    )
    scheduler = BlockingScheduler()
    monkeypatch.setattr(modules[CACHE_SCHEDULER_MODULE], "scheduler", scheduler, raising=False)

    adapter = get_adapter(scheduler)
    assert adapter is not None
    assert adapter.identity.scheduler_id == "sidecar_identity"


def test_cache_identity_ready_waits_for_the_declared_module_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "VERCEL_APSCHEDULER_SUBSCRIBERS",
        f'[{{"id":"jobs_scheduler","entrypoint":"{CACHE_SCHEDULER_MODULE}:scheduler"}}]',
    )
    backend = CacheBackend()
    scheduler = BlockingScheduler()

    # While the declaring module is importing, the variable is unbound and
    # registration must defer rather than cache the "default" fallback.
    assert backend.identity_ready(scheduler) is False

    monkeypatch.setattr(modules[CACHE_SCHEDULER_MODULE], "scheduler", scheduler, raising=False)
    assert backend.identity_ready(scheduler) is True
    assert backend.derive_identity(scheduler).scheduler_id == "jobs_scheduler"


def test_development_derives_a_stable_deployment_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `vercel dev` sets no VERCEL_DEPLOYMENT_ID (SDKs read its presence as
    # "deployed"); development derives a stable synthetic id instead, so the
    # web process and the sidecar share one state scope.
    monkeypatch.delenv("VERCEL_DEPLOYMENT_ID", raising=False)
    monkeypatch.setenv("VERCEL_ENV", "development")

    scheduler = BlockingScheduler()
    adapter = get_adapter(scheduler)
    assert adapter is not None

    assert adapter.deployment == development_deployment_id()
    assert adapter.deployment.startswith("dpl_dev_")
    assert adapter.scope == adapter.deployment


def test_missing_deployment_id_still_fails_when_deployed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VERCEL_DEPLOYMENT_ID", raising=False)
    monkeypatch.setenv("VERCEL_ENV", "preview")

    scheduler = BlockingScheduler()
    adapter = get_adapter(scheduler)
    assert adapter is not None

    with pytest.raises(APSchedulerConfigurationError, match="VERCEL_DEPLOYMENT_ID"):
        _ = adapter.deployment


def test_cache_identity_defaults_without_a_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VERCEL_APSCHEDULER_SUBSCRIBERS", raising=False)
    backend = CacheBackend()
    scheduler = BlockingScheduler()

    assert backend.identity_ready(scheduler) is True
    assert backend.derive_identity(scheduler).scheduler_id == "default"


def test_cache_backend_accepts_source_stores() -> None:
    scheduler = BlockingScheduler(timezone=UTC)
    scheduler.add_jobstore(MemoryJobStore(), "source")

    stores = CacheBackend().validate_configuration(scheduler)

    assert "source" in stores


def test_cache_backend_rejects_a_durable_store_under_a_source_alias() -> None:
    scheduler = BlockingScheduler(timezone=UTC)
    scheduler.add_jobstore(CacheJobStore(), "secondary")

    with pytest.raises(
        APSchedulerConfigurationError,
        match='durable store must be the one named "default"',
    ):
        CacheBackend().validate_configuration(scheduler)
