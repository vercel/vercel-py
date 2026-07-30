from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from os import environ
from unittest.mock import patch

import pytest
from apscheduler.schedulers.blocking import BlockingScheduler
from redis import Redis

from vercel.integrations.apscheduler import (
    VercelRedisJobStore,
    install_vercel_apscheduler_integration,
)
from vercel.integrations.apscheduler._adapter import SchedulerAdapter, get_adapter
from vercel.integrations.apscheduler._driver import RedisDriver, StartDecision

UTC = timezone.utc
REDIS_URL = environ.get("APSCHEDULER_TEST_REDIS_URL")


def durable_test_job() -> None:
    return


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_driver_fences_concurrent_lifecycle_transitions() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        deployment="dpl_driver_test",
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
def test_real_redis_preview_idle_deadline_is_atomic_and_respects_pause() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        deployment="dpl_preview_idle_test",
        scheduler_id="scheduler",
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

        resumed = driver.start(now + timedelta(hours=2))
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
def test_real_redis_expired_preview_wake_cannot_run() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        deployment="dpl_preview_wake_test",
        scheduler_id="scheduler",
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
        second_store = VercelRedisJobStore(url=REDIS_URL)
        monkeypatch.delenv("VERCEL")
        second_scheduler = BlockingScheduler(
            timezone=UTC,
            jobstores={"default": second_store},
        )
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
        [(stale_job, stale_revision)] = adapter.coordinator.get_all_jobs_with_revisions()

        scheduler.modify_job("mutable", name="new-name")
        stale_job._modify(name="stale-name")
        assert not adapter.coordinator.cas_update_job(
            stale_job,
            stale_revision,
        )
        assert scheduler.get_job("mutable").name == "new-name"

        [(removed_job, removed_revision)] = adapter.coordinator.get_all_jobs_with_revisions()
        scheduler.remove_job("mutable")
        assert not adapter.coordinator.cas_update_job(
            removed_job,
            removed_revision,
        )
        assert scheduler.get_job("mutable") is None
    finally:
        client.delete(*keys)


@pytest.mark.skipif(
    REDIS_URL is None,
    reason="requires a disposable Redis server",
)
def test_real_redis_lost_owner_is_not_mistaken_for_fence() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        deployment="dpl_lost_owner_test",
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


def _real_scheduler(
    monkeypatch: pytest.MonkeyPatch,
    *,
    deployment: str,
) -> tuple[BlockingScheduler, SchedulerAdapter, Redis, tuple[str, ...]]:
    assert REDIS_URL is not None
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", deployment)
    monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", "scheduler")
    monkeypatch.delenv("VERCEL", raising=False)
    install_vercel_apscheduler_integration(register_queues=False)
    store = VercelRedisJobStore(url=REDIS_URL)
    scheduler = BlockingScheduler(
        timezone=UTC,
        jobstores={"default": store},
    )
    monkeypatch.setenv("VERCEL", "1")
    adapter = get_adapter(scheduler)
    assert adapter is not None
    adapter._bind_runtime()
    keys = adapter.coordinator.keys
    store.redis.delete(*keys)
    return scheduler, adapter, store.redis, keys


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
