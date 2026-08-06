from __future__ import annotations

from typing import Any

import pickle  # noqa: S403 - mirrors the store's own persistence format
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from os import environ
from sys import modules
from types import ModuleType
from unittest.mock import patch

import pytest
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.util import datetime_to_utc_timestamp
from redis import Redis

from vercel.integrations.apscheduler import (
    _adapter as _adapter_module,
    install_vercel_apscheduler_integration,
)
from vercel.integrations.apscheduler._adapter import SchedulerAdapter, get_adapter
from vercel.integrations.apscheduler._driver import (
    NamespaceFencedError,
    RedisDriver,
    StartDecision,
)

UTC = timezone.utc
REDIS_URL = environ.get("APSCHEDULER_TEST_REDIS_URL")
TEST_SCHEDULER_MODULE = "test_redis_scheduler"
TEST_SCHEDULER_ID = "scheduler"


def durable_test_job() -> None:
    return


_replacement_run_date: dict[str, datetime] = {}


def replace_sibling_job() -> None:
    scheduler = modules[TEST_SCHEDULER_MODULE].__dict__["scheduler"]
    scheduler.add_job(
        durable_test_job,
        "date",
        run_date=_replacement_run_date["target"],
        id="target",
        replace_existing=True,
    )


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_driver_fences_concurrent_lifecycle_transitions() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        scope="dpl_driver_test",
        scheduler_id="scheduler",
        deployment="dpl_driver_test",
    )
    client.delete(driver.key)
    try:
        now = datetime.now(UTC)
        with ThreadPoolExecutor(max_workers=16) as executor:
            decisions = list(executor.map(driver.start, [now] * 100))

        assert sum(decision.changed for decision in decisions) == 1
        assert {decision.generation for decision in decisions} == {1}

        start = driver.claim_start(1, "start-1", now)
        assert start.state == "claimed"
        assert driver.claim_start(1, "start-2", now).state == "busy"
        start_finish = driver.finish_start(
            1,
            "start-1",
            now + timedelta(minutes=1),
            now,
        )
        assert start_finish.state == "advanced"
        wake = start_finish.wake
        assert wake is not None
        driver.mark_wake_published(1, 1, now)

        assert driver.claim_wake(wake, "wake-1", now).state == "claimed"
        assert driver.pause(now) is True
        resumed = driver.start(now)
        assert resumed.changed is True
        assert resumed.generation == 2
        assert driver.claim_start(2, "start-2", now).state == "busy"

        assert (
            driver.finish_wake(
                wake,
                "wake-1",
                now + timedelta(minutes=2),
                now,
            ).state
            == "fenced"
        )
        assert driver.claim_start(2, "start-2", now).state == "claimed"
        resumed_finish = driver.finish_start(
            2,
            "start-2",
            now + timedelta(minutes=1),
            now,
        )
        assert resumed_finish.state == "advanced"
        current = resumed_finish.wake
        assert current is not None
        assert driver.claim_wake(wake, "stale", now).state == "stale"
        assert driver.claim_wake(current, "current", now).state == "claimed"

        assert driver.pause(now) is True
        assert (
            driver.finish_wake(
                current,
                "current",
                now + timedelta(minutes=2),
                now,
            ).state
            == "fenced"
        )
        assert driver.snapshot().state == "paused"

        _assert_spammed_lifecycle_converges(driver, now)
    finally:
        client.delete(driver.key)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_dormant_start_reserves_no_wake() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        scope="dpl_dormant_test",
        scheduler_id="scheduler",
        deployment="dpl_dormant_test",
    )
    client.delete(driver.key)
    try:
        now = datetime.now(UTC)
        driver.start(now)
        assert driver.claim_start(1, "start", now).state == "claimed"
        finish = driver.finish_start(1, "start", None, now)
        assert finish.state == "advanced"
        assert finish.wake is None
        snapshot = driver.snapshot()
        assert snapshot.state == "running"
        assert snapshot.current_wake is None
    finally:
        client.delete(driver.key)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_lost_owner_is_not_mistaken_for_fence() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        scope="dpl_lost_owner_test",
        scheduler_id="scheduler",
        deployment="dpl_lost_owner_test",
    )
    client.delete(driver.key)
    try:
        now = datetime.now(UTC)
        driver.start(now)
        assert driver.claim_start(1, "start", now).state == "claimed"
        start_finish = driver.finish_start(
            1,
            "start",
            now + timedelta(minutes=1),
            now,
        )
        token = start_finish.wake
        assert token is not None
        assert driver.claim_wake(token, "owner-a", now).state == "claimed"
        reclaimed_at = now + timedelta(minutes=16)
        assert driver.claim_wake(token, "owner-b", reclaimed_at).state == "claimed"

        lost = driver.finish_wake(
            token,
            "owner-a",
            None,
            reclaimed_at,
        )
        assert lost.state == "lost"
        driver.release("owner-a")
        assert driver.renew("owner-b", reclaimed_at)
    finally:
        client.delete(driver.key)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_overdue_published_wake_is_repaired() -> None:
    """A published wake whose message died must become repairable.

    ``published`` asserts a queue message exists; a rollback or an alias
    expiry can falsify that silently. The repair demotes only a wake that is
    well past due with no live owner, so live deliveries and running owners
    are never disturbed.
    """
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        scope="dpl_repair_test",
        scheduler_id="scheduler",
        deployment="dpl_repair_test",
    )
    client.delete(driver.key)
    try:
        now = datetime.now(UTC)
        overdue = now + timedelta(hours=1)
        driver.start(now)
        assert driver.claim_start(1, "start", now).state == "claimed"
        finish = driver.finish_start(1, "start", now + timedelta(minutes=1), now)
        token = finish.wake
        assert token is not None
        driver.release("start")

        # Pending wakes are the existing repair path's job, not this one's.
        assert driver.repair_overdue_wake(overdue) is False

        driver.mark_wake_published(1, 1, now)

        # Not yet overdue: the message is presumed alive.
        assert driver.repair_overdue_wake(now + timedelta(minutes=5)) is False

        # Overdue, published, no owner: the rollback strand. Demote to pending.
        assert driver.repair_overdue_wake(overdue) is True
        wake = driver.snapshot().current_wake
        assert wake is not None
        assert wake.status == "pending"

        # Overdue but the owner holds a live lease: a slow handler, not a loss.
        assert driver.claim_wake(token, "owner", now).state == "claimed"
        assert driver.repair_overdue_wake(now + timedelta(minutes=12)) is False

        # The owner's lease expired without a finish: a crash, repairable.
        assert driver.repair_overdue_wake(overdue) is True
        wake = driver.snapshot().current_wake
        assert wake is not None
        assert wake.status == "pending"

        # Paused chains stay paused.
        driver.mark_wake_published(1, 1, now)
        driver.pause(now)
        assert driver.repair_overdue_wake(overdue) is False
    finally:
        client.delete(driver.key)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_takeover_fences_the_previous_deployment() -> None:
    """A promoted deployment takes the chain; the demoted one goes inert.

    Queue deliveries keep reaching a demoted deployment, so its claims,
    repairs, and rearms against the shared namespace must all turn stale the
    moment another deployment starts. Rolling back is the same operation in
    the other direction.
    """
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    first = RedisDriver(
        client,
        scope="prj_fence:production",
        scheduler_id="scheduler",
        deployment="dpl_first",
    )
    second = RedisDriver(
        client,
        scope="prj_fence:production",
        scheduler_id="scheduler",
        deployment="dpl_second",
    )
    client.delete(first.key)
    try:
        now = datetime.now(UTC)
        overdue = now + timedelta(hours=1)
        assert first.start(now).changed is True
        assert first.claim_start(1, "start", now).state == "claimed"
        finish = first.finish_start(1, "start", now + timedelta(minutes=1), now)
        token = finish.wake
        assert token is not None
        first.release("start")
        first.mark_wake_published(1, 1, now)
        assert first.owner_deployment() == "dpl_first"

        # Promote: the second deployment starts while the chain is running.
        takeover = second.start(now)
        assert takeover.changed is True
        assert takeover.generation == 2
        assert second.owner_deployment() == "dpl_second"

        # The demoted deployment is fenced out of everything.
        assert first.claim_wake(token, "old-wake", now).state == "stale"
        assert first.claim_start(2, "old-start", now).state == "stale"
        assert first.repair_overdue_wake(overdue) is False
        assert first.start(now).changed is True  # explicit start = rollback

        # Rollback: ownership and a fresh generation move back.
        assert first.owner_deployment() == "dpl_first"
        assert second.claim_start(3, "stale-second", now).state == "stale"
        assert second.repair_overdue_wake(overdue) is False
    finally:
        client.delete(first.key)


def _assert_spammed_lifecycle_converges(
    driver: RedisDriver,
    now: datetime,
) -> None:
    def toggle(index: int) -> None:
        if index % 2:
            driver.start(now)
        else:
            driver.pause(now)

    with ThreadPoolExecutor(max_workers=32) as executor:
        list(executor.map(toggle, range(500)))

    driver.pause(now)
    prior_generation = driver.snapshot().generation
    with ThreadPoolExecutor(max_workers=32) as executor:
        restarts = list(executor.map(driver.start, [now] * 100))

    assert sum(decision.changed for decision in restarts) == 1
    assert {decision.generation for decision in restarts} == {prior_generation + 1}

    def claim(index: int) -> tuple[str, str]:
        owner = f"attempt-{index}"
        state = driver.claim_start(
            prior_generation + 1,
            owner,
            now,
        ).state
        return owner, state

    with ThreadPoolExecutor(max_workers=32) as executor:
        claims = list(executor.map(claim, range(100)))

    assert sum(state == "claimed" for _, state in claims) == 1
    assert sum(state == "busy" for _, state in claims) == 99
    winner = next(owner for owner, state in claims if state == "claimed")
    driver.release(winner)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_runtime_mutation_rearms_dormant_chain_monotonically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_mutation_test",
    )
    try:
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="start",
        ):
            scheduler.start()
        now = datetime.now(UTC)
        assert adapter.driver.claim_start(1, "start-owner", now).state == "claimed"
        start_finish = adapter.driver.finish_start(
            1,
            "start-owner",
            None,
            now,
        )
        assert start_finish.state == "advanced"
        assert start_finish.wake is None

        first_time = now + timedelta(minutes=5)
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="wake-1",
        ) as send:
            scheduler.add_job(
                durable_test_job,
                "date",
                run_date=first_time,
                id="first",
            )

        first = adapter.driver.snapshot().current_wake
        assert first is not None
        assert first.sequence == 1
        send.assert_called_once()

        assert adapter.driver.claim_wake(first, "wake-owner", now).state == "claimed"
        terminal = adapter.driver.finish_wake(
            first,
            "wake-owner",
            None,
            now,
        )
        assert terminal.state == "advanced"
        assert terminal.wake is None
        assert adapter.driver.snapshot().current_wake is None

        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="wake-2",
        ):
            scheduler.add_job(
                durable_test_job,
                "date",
                run_date=now + timedelta(minutes=10),
                id="second",
            )

        second = adapter.driver.snapshot().current_wake
        assert second is not None
        assert second.sequence == 2
    finally:
        client.delete(*keys)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_cold_declaration_rearms_an_already_dormant_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_scheduler, first_adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_declaration_test",
    )
    try:
        _activate_dormant_scheduler(first_scheduler, first_adapter)

        assert REDIS_URL is not None
        second_store = _redis_job_store(REDIS_URL)
        monkeypatch.delenv("VERCEL")
        # The second scheduler models a separate cold-started process; the
        # per-process identity registry does not span processes.
        _adapter_module._ACTIVE_IDENTITIES.clear()
        second_scheduler = BlockingScheduler(
            timezone=UTC,
            jobstores={"default": second_store},
        )
        modules[TEST_SCHEDULER_MODULE].__dict__["scheduler"] = second_scheduler
        monkeypatch.setenv("VERCEL", "1")

        second_scheduler.add_job(
            durable_test_job,
            "date",
            run_date=datetime.now(UTC) + timedelta(minutes=5),
            id="declared",
            replace_existing=True,
        )
        second_adapter = get_adapter(second_scheduler)
        assert second_adapter is not None

        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="wake",
        ) as send:
            second_scheduler.start()

        wake = second_adapter.driver.snapshot().current_wake
        assert wake is not None
        assert wake.sequence == 1
        send.assert_called_once()
    finally:
        client.delete(*keys)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_mutation_during_owner_is_folded_into_successor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_dirty_test",
    )
    try:
        _activate_dormant_scheduler(scheduler, adapter)
        now = datetime.now(UTC)
        original_time = now + timedelta(hours=1)
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="wake",
        ):
            scheduler.add_job(
                durable_test_job,
                "date",
                run_date=original_time,
                id="later",
            )
        original = adapter.driver.snapshot().current_wake
        assert original is not None
        assert adapter.driver.claim_wake(original, "owner", now).state == "claimed"

        earlier_time = now + timedelta(minutes=5)
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="unexpected",
        ) as send:
            scheduler.add_job(
                durable_test_job,
                "date",
                run_date=earlier_time,
                id="earlier",
            )
        send.assert_not_called()

        finish = adapter.driver.finish_wake(
            original,
            "owner",
            original_time + timedelta(hours=1),
            now,
        )
        assert finish.state == "advanced"
        assert finish.wake is not None
        assert finish.wake.sequence == original.sequence + 1
        assert finish.wake.logical_time == earlier_time
    finally:
        client.delete(*keys)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_in_job_add_replaces_persisted_sibling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_in_job_replace_test",
    )
    try:
        _activate_dormant_scheduler(scheduler, adapter)
        now = datetime.now(UTC)
        writer_time = now + timedelta(minutes=5)
        original_time = now + timedelta(hours=2)
        replacement_time = now + timedelta(minutes=30)
        _replacement_run_date["target"] = replacement_time
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="wake",
        ):
            scheduler.add_job(
                durable_test_job,
                "date",
                run_date=original_time,
                id="target",
            )
            scheduler.add_job(
                replace_sibling_job,
                "date",
                run_date=writer_time,
                id="writer",
            )

        result = adapter.process_wakeup(writer_time, now=writer_time)

        assert result.due_job_ids == ("writer",)
        persisted = adapter.coordinator.store.lookup_job("target")
        assert persisted is not None
        assert persisted.next_run_time == replacement_time
        assert result.next_wakeup_time == replacement_time
    finally:
        client.delete(*keys)
        _replacement_run_date.clear()


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_cas_prevents_stale_overwrite_and_resurrection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_cas_test",
    )
    try:
        _activate_dormant_scheduler(scheduler, adapter)
        run_time = datetime.now(UTC) + timedelta(minutes=5)
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="wake",
        ):
            scheduler.add_job(
                durable_test_job,
                "date",
                run_date=run_time,
                id="mutable",
            )
        [(stale_job, stale_revision, _)] = adapter.coordinator.get_all_jobs_with_revisions()[0]

        scheduler.modify_job("mutable", name="new-name")
        stale_job._modify(name="stale-name")
        assert not adapter.coordinator.cas_update_job(
            stale_job,
            stale_revision,
        )
        assert scheduler.get_job("mutable").name == "new-name"

        [(removed_job, removed_revision, _)] = adapter.coordinator.get_all_jobs_with_revisions()[0]
        scheduler.remove_job("mutable")
        assert not adapter.coordinator.cas_update_job(
            removed_job,
            removed_revision,
        )
        assert scheduler.get_job("mutable") is None
    finally:
        client.delete(*keys)


def _real_scheduler(
    monkeypatch: pytest.MonkeyPatch,
    *,
    deployment: str,
) -> tuple[BlockingScheduler, SchedulerAdapter, Redis, tuple[str, ...]]:
    assert REDIS_URL is not None
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", deployment)
    if not environ.get("VERCEL_ENV") and not environ.get("VERCEL_TARGET_ENV"):
        monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.delenv("VERCEL_PYTHON_SUBSCRIBER_ID", raising=False)
    monkeypatch.setenv(
        "VERCEL_APSCHEDULER_SUBSCRIBERS",
        (f'[{{"id":"{TEST_SCHEDULER_ID}","entrypoint":"{TEST_SCHEDULER_MODULE}:scheduler"}}]'),
    )
    monkeypatch.delenv("VERCEL", raising=False)
    install_vercel_apscheduler_integration(register_queues=False)
    _adapter_module._ACTIVE_IDENTITIES.clear()
    store = _redis_job_store(REDIS_URL)
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": store},
    )
    module = ModuleType(TEST_SCHEDULER_MODULE)
    module.__dict__["scheduler"] = scheduler
    monkeypatch.setitem(modules, TEST_SCHEDULER_MODULE, module)
    monkeypatch.setenv("VERCEL", "1")
    adapter = get_adapter(scheduler)
    assert adapter is not None
    adapter._bind_runtime()
    keys = adapter.coordinator.keys
    store.redis.delete(*keys)
    return scheduler, adapter, store.redis, keys


def _redis_job_store(url: str) -> RedisJobStore:
    return RedisJobStore(connection_pool=Redis.from_url(url).connection_pool)


def _activate_dormant_scheduler(
    scheduler: BlockingScheduler,
    adapter: SchedulerAdapter,
) -> None:
    with patch(
        "vercel.integrations.apscheduler._adapter.vqs_sync.send",
        return_value="start",
    ):
        scheduler.start()
    now = datetime.now(UTC)
    driver = adapter.driver
    assert driver.claim_start(1, "start-owner", now).state == "claimed"
    finish = driver.finish_start(1, "start-owner", None, now)
    assert finish.state == "advanced"
    assert finish.wake is None


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_takeover_reconciles_declared_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A promoted deployment syncs declared jobs and keeps dynamic ones."""
    assert REDIS_URL is not None
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_reconcile")
    scheduler, adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_reconcile_one",
    )
    try:
        for job_id in ("keep", "gone", "changed"):
            scheduler.add_job(
                durable_test_job,
                "interval",
                hours=1,
                id=job_id,
                replace_existing=True,
            )
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            scheduler.start()
            scheduler.add_job(
                durable_test_job,
                "date",
                run_date=datetime.now(UTC) + timedelta(hours=2),
                id="dynamic",
            )
        before = {
            job.id: job.next_run_time
            for job, _revision, _provenance in (
                adapter.coordinator.get_all_jobs_with_revisions()[0]
            )
        }
        assert adapter.driver.reconciled_deployment() == "dpl_reconcile_one"

        # A new deployment cold-starts against the shared production scope.
        _adapter_module._ACTIVE_IDENTITIES.clear()
        monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_reconcile_two")
        monkeypatch.delenv("VERCEL")
        second = BlockingScheduler(
            timezone=UTC,
            jobstores={"default": _redis_job_store(REDIS_URL)},
        )
        modules[TEST_SCHEDULER_MODULE].__dict__["scheduler"] = second
        monkeypatch.setenv("VERCEL", "1")
        second.add_job(
            durable_test_job,
            "interval",
            hours=1,
            id="keep",
            replace_existing=True,
        )
        second.add_job(
            durable_test_job,
            "interval",
            hours=2,
            id="changed",
            replace_existing=True,
        )
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            second.start()
        second_adapter = get_adapter(second)
        assert second_adapter is not None

        records = {
            job.id: (job, provenance)
            for job, _revision, provenance in (
                second_adapter.coordinator.get_all_jobs_with_revisions()[0]
            )
        }
        assert set(records) == {"keep", "changed", "dynamic"}
        assert records["keep"][0].next_run_time == before["keep"]
        assert records["keep"][1] == "declared"
        assert records["changed"][0].next_run_time != before["changed"]
        assert records["changed"][0].trigger.interval == timedelta(hours=2)
        assert records["dynamic"][1] == "runtime"
        assert second_adapter.driver.reconciled_deployment() == "dpl_reconcile_two"
    finally:
        client.delete(*keys)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_preview_idle_deadline_is_atomic_and_respects_pause() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        scope="dpl_preview_idle_test",
        scheduler_id="scheduler",
        deployment="dpl_preview_idle_test",
    )
    client.delete(driver.key)
    try:
        now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)

        def activate(_: int) -> StartDecision:
            return driver.auto_activate(
                now,
                idle_timeout_seconds=30 * 60,
            )

        with ThreadPoolExecutor(max_workers=16) as executor:
            starts = list(executor.map(activate, range(100)))
        assert sum(decision.changed for decision in starts) == 1
        assert {decision.generation for decision in starts} == {1}
        assert {decision.state for decision in starts} == {"running"}

        renewed = driver.auto_activate(
            now + timedelta(minutes=5),
            idle_timeout_seconds=30 * 60,
        )
        assert not renewed.changed
        assert renewed.generation == 1

        assert driver.claim_start(1, "old-owner", now).state == "claimed"
        assert driver.renew("old-owner", now + timedelta(minutes=25))
        restarted = driver.auto_activate(
            now + timedelta(minutes=36),
            idle_timeout_seconds=30 * 60,
        )
        assert restarted.changed
        assert restarted.generation == 2
        assert (
            driver.claim_start(
                2,
                "new-owner",
                now + timedelta(minutes=36),
            ).state
            == "busy"
        )
        old_finish = driver.finish_start(
            1,
            "old-owner",
            now + timedelta(hours=1),
            now + timedelta(minutes=36),
        )
        assert old_finish.state == "fenced"
        assert (
            driver.claim_start(
                2,
                "new-owner",
                now + timedelta(minutes=36),
            ).state
            == "claimed"
        )
        driver.release("new-owner")

        assert driver.pause(now + timedelta(minutes=37))
        paused = driver.auto_activate(
            now + timedelta(hours=2),
            idle_timeout_seconds=30 * 60,
        )
        assert not paused.changed
        assert paused.state == "paused"
        assert paused.generation == 2
        assert driver.snapshot().state == "paused"

        resumed = driver.start(
            now + timedelta(hours=2),
            idle_timeout_seconds=30 * 60,
        )
        assert resumed.generation == 3
        assert (
            driver.claim_start(
                3,
                "expiring-owner",
                now + timedelta(hours=2),
            ).state
            == "claimed"
        )
        expired_finish = driver.finish_start(
            3,
            "expiring-owner",
            now + timedelta(hours=3),
            now + timedelta(hours=2, minutes=31),
        )
        assert expired_finish.state == "fenced"
        assert driver.snapshot().state == "inactive"
    finally:
        client.delete(driver.key)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_takeover_restores_unloadable_declared_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A declared record that no longer loads is rewritten, not stranded.

    Moving a scheduled function's module makes the persisted record
    unloadable under the new code. The declaration is authoritative, so
    takeover reconciliation must rewrite the record instead of leaving it
    quarantined forever behind an insert-if-absent conflict.
    """
    assert REDIS_URL is not None
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_restore")
    scheduler, _adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_restore_one",
    )
    try:
        scheduler.add_job(
            durable_test_job,
            "interval",
            hours=1,
            id="moved",
            replace_existing=True,
        )
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            scheduler.start()
        store = scheduler._jobstores["default"]
        raw: Any = client.hget(store.jobs_key, "moved")
        state = pickle.loads(raw)  # noqa: S301 - crafted by this test
        state["func"] = "missing_module:missing_function"
        client.hset(store.jobs_key, mapping={"moved": pickle.dumps(state)})

        # A new deployment declaring the same job takes the namespace over.
        _adapter_module._ACTIVE_IDENTITIES.clear()
        monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_restore_two")
        monkeypatch.delenv("VERCEL")
        second = BlockingScheduler(
            timezone=UTC,
            jobstores={"default": _redis_job_store(REDIS_URL)},
        )
        modules[TEST_SCHEDULER_MODULE].__dict__["scheduler"] = second
        monkeypatch.setenv("VERCEL", "1")
        second.add_job(
            durable_test_job,
            "interval",
            hours=1,
            id="moved",
            replace_existing=True,
        )
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            second.start()
        second_adapter = get_adapter(second)
        assert second_adapter is not None

        jobs, undecodable = second_adapter.coordinator.get_all_jobs_with_revisions()
        assert undecodable == []
        [(restored, _revision, provenance)] = jobs
        assert restored.id == "moved"
        assert provenance == "declared"
        assert restored.next_run_time is not None
        second_store = second._jobstores["default"]
        assert client.zscore(second_store.run_times_key, "moved") is not None
    finally:
        client.delete(*keys)


def _take_over_scheduler(
    monkeypatch: pytest.MonkeyPatch,
    deployment: str,
) -> tuple[BlockingScheduler, SchedulerAdapter]:
    """Build a second scheduler over the shared scope, bound but not started."""
    assert REDIS_URL is not None
    _adapter_module._ACTIVE_IDENTITIES.clear()
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", deployment)
    monkeypatch.delenv("VERCEL")
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": _redis_job_store(REDIS_URL)},
    )
    modules[TEST_SCHEDULER_MODULE].__dict__["scheduler"] = scheduler
    monkeypatch.setenv("VERCEL", "1")
    adapter = get_adapter(scheduler)
    assert adapter is not None
    adapter._bind_runtime()
    return scheduler, adapter


def _bump_revision(client: Redis, keys: tuple[str, ...], job_id: str) -> None:
    """Mimic a concurrent owner write moving a job's revision."""
    revision = client.hincrby(keys[3], "job_revision", 1)
    client.hset(keys[2], job_id, str(revision))


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_demoted_deployment_writes_are_fenced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A takeover atomically fences the demoted deployment's late writes.

    An in-flight handler on the demoted deployment may finish after the
    takeover; its job-store commit must be refused so reconciliation can
    never lose to it, and it must not stamp reconciliation as its own.
    """
    assert REDIS_URL is not None
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_fence")
    first_scheduler, first_adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_fence_one",
    )
    try:
        first_scheduler.add_job(
            durable_test_job,
            "interval",
            hours=1,
            id="lingering",
            replace_existing=True,
        )
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            first_scheduler.start()
        [(job, revision, _provenance)], _undecodable = (
            first_adapter.coordinator.get_all_jobs_with_revisions()
        )

        second, second_adapter = _take_over_scheduler(monkeypatch, "dpl_fence_two")
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            second.start()
        second_driver = second_adapter.driver
        assert second_driver.owner_deployment() == "dpl_fence_two"

        # The demoted deployment's in-flight handler writes with its own
        # bound identity; only the environment is restored for the lookup.
        monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_fence_one")
        first_coordinator = first_adapter.coordinator
        with pytest.raises(NamespaceFencedError):
            first_coordinator.cas_update_job(job, revision)
        with pytest.raises(NamespaceFencedError):
            first_coordinator.remove_job("lingering")
        assert not first_adapter.driver.mark_reconciled(
            "dpl_fence_one",
            datetime.now(UTC),
        )
        assert second_driver.reconciled_deployment() == "dpl_fence_two"
    finally:
        client.delete(*keys)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_reconciliation_retries_revision_races(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A revision race reruns the pass instead of losing the removal."""
    assert REDIS_URL is not None
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_retry")
    first_scheduler, _first_adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_retry_one",
    )
    try:
        first_scheduler.add_job(
            durable_test_job,
            "interval",
            hours=1,
            id="gone",
            replace_existing=True,
        )
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            first_scheduler.start()

        second, second_adapter = _take_over_scheduler(monkeypatch, "dpl_retry_two")
        coordinator = second_adapter.coordinator
        original_remove = coordinator.cas_remove_job
        calls: list[str] = []

        def racing_remove(job_id: str, expected_revision: int) -> bool:
            calls.append(job_id)
            if len(calls) == 1:
                _bump_revision(client, coordinator.keys, job_id)
            return original_remove(job_id, expected_revision)

        monkeypatch.setattr(coordinator, "cas_remove_job", racing_remove)
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            second.start()

        assert calls == ["gone", "gone"]
        assert not client.hexists(coordinator.keys[0], "gone")
        assert second_adapter.driver.reconciled_deployment() == "dpl_retry_two"
    finally:
        client.delete(*keys)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_unconverged_reconciliation_defers_and_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconciliation that cannot converge stays unmarked and retries later."""
    assert REDIS_URL is not None
    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_defer")
    first_scheduler, _first_adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_defer_one",
    )
    try:
        first_scheduler.add_job(
            durable_test_job,
            "interval",
            hours=1,
            id="gone",
            replace_existing=True,
        )
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            first_scheduler.start()

        second, second_adapter = _take_over_scheduler(monkeypatch, "dpl_defer_two")
        coordinator = second_adapter.coordinator
        original_remove = coordinator.cas_remove_job

        def always_racing_remove(job_id: str, expected_revision: int) -> bool:
            _bump_revision(client, coordinator.keys, job_id)
            return original_remove(job_id, expected_revision)

        monkeypatch.setattr(coordinator, "cas_remove_job", always_racing_remove)
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            second.start()

        # Every pass lost its race: the job survives and the marker is not
        # stamped, so the sync stays owed rather than silently skipped.
        assert client.hexists(coordinator.keys[0], "gone")
        assert second_adapter.driver.reconciled_deployment() == "dpl_defer_one"
        assert second_adapter._reconciled is False

        monkeypatch.setattr(coordinator, "cas_remove_job", original_remove)
        second_adapter.ensure_local_started()

        assert not client.hexists(coordinator.keys[0], "gone")
        assert second_adapter.driver.reconciled_deployment() == "dpl_defer_two"
        assert second_adapter._reconciled is True
    finally:
        client.delete(*keys)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_quarantine_sidelines_unloadable_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A record that no longer unpickles must not wedge the wake chain."""
    assert REDIS_URL is not None
    scheduler, adapter, client, keys = _real_scheduler(
        monkeypatch,
        deployment="dpl_quarantine",
    )
    try:
        run_date = datetime.now(UTC) + timedelta(minutes=5)
        scheduler.add_job(
            durable_test_job,
            "date",
            run_date=run_date,
            id="good",
            replace_existing=True,
        )
        with patch(
            "vercel.integrations.apscheduler._adapter.vqs_sync.send",
            return_value="msg",
        ):
            scheduler.start()

        store = scheduler._jobstores["default"]
        raw: Any = client.hget(store.jobs_key, "good")
        state = pickle.loads(raw)  # noqa: S301 - crafted by this test
        state["id"] = "broken"
        state["func"] = "missing_module:missing_function"
        client.hset(store.jobs_key, mapping={"broken": pickle.dumps(state)})
        client.zadd(
            store.run_times_key,
            {"broken": datetime_to_utc_timestamp(run_date)},
        )

        due = adapter.coordinator.get_due_jobs_with_revisions(run_date + timedelta(minutes=1))

        assert [job.id for job, _revision in due] == ["good"]
        assert client.zscore(store.run_times_key, "broken") is None
        assert client.hexists(store.jobs_key, "broken")
    finally:
        client.delete(*keys)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_manual_start_renews_a_lapsed_preview_deadline() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        scope="dpl_preview_restart_test",
        scheduler_id="scheduler",
        deployment="dpl_preview_restart_test",
    )
    client.delete(driver.key)
    try:
        now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
        driver.auto_activate(now, idle_timeout_seconds=60)

        # The deadline lapsed; a manual start must renew it, or the new
        # generation would be demoted before its start message could claim.
        restarted = driver.start(
            now + timedelta(minutes=10),
            idle_timeout_seconds=60,
        )
        assert restarted.changed
        assert restarted.generation == 2
        assert restarted.state == "running"
        assert (
            driver.claim_start(
                2,
                "start-owner",
                now + timedelta(minutes=10),
            ).state
            == "claimed"
        )
        assert driver.snapshot().state == "running"
    finally:
        client.delete(driver.key)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_expired_preview_wake_cannot_run() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        scope="dpl_preview_wake_test",
        scheduler_id="scheduler",
        deployment="dpl_preview_wake_test",
    )
    client.delete(driver.key)
    try:
        now = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
        driver.auto_activate(now, idle_timeout_seconds=60)
        assert driver.claim_start(1, "start-owner", now).state == "claimed"
        finished = driver.finish_start(
            1,
            "start-owner",
            now + timedelta(minutes=5),
            now,
        )
        wake = finished.wake
        assert wake is not None

        claim = driver.claim_wake(
            wake,
            "wake-owner",
            now + timedelta(minutes=2),
        )
        assert claim.state == "stale"
        assert driver.snapshot().state == "inactive"
        assert driver.snapshot().current_wake is None
    finally:
        client.delete(driver.key)
