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
from vercel.integrations.apscheduler._driver import RedisDriver

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
        [(stale_job, stale_revision, _)] = adapter.coordinator.get_all_jobs_with_revisions()

        scheduler.modify_job("mutable", name="new-name")
        stale_job._modify(name="stale-name")
        assert not adapter.coordinator.cas_update_job(
            stale_job,
            stale_revision,
        )
        assert scheduler.get_job("mutable").name == "new-name"

        [(removed_job, removed_revision, _)] = adapter.coordinator.get_all_jobs_with_revisions()
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
            for job, _revision, _provenance in (adapter.coordinator.get_all_jobs_with_revisions())
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
                second_adapter.coordinator.get_all_jobs_with_revisions()
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
