from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from os import environ
from sys import modules
from types import ModuleType
from unittest.mock import patch

import pytest
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from redis import Redis

from vercel.integrations.apscheduler import install_vercel_apscheduler_integration
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
def test_real_redis_dormant_start_reserves_no_wake() -> None:
    assert REDIS_URL is not None
    client = Redis.from_url(REDIS_URL)
    driver = RedisDriver(
        client,
        deployment="dpl_dormant_test",
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
