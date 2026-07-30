from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from os import environ

import pytest
from redis import Redis

from vercel.integrations.apscheduler._driver import RedisDriver

UTC = timezone.utc


@pytest.mark.skipif(
    "APSCHEDULER_TEST_REDIS_URL" not in environ,
    reason="requires a disposable Redis server",
)
def test_real_redis_driver_fences_concurrent_lifecycle_transitions() -> None:
    client = Redis.from_url(environ["APSCHEDULER_TEST_REDIS_URL"])
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
        wake = driver.finish_start(
            1,
            "start-1",
            now + timedelta(minutes=1),
            now,
        )
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
            )
            is None
        )
        assert driver.claim_start(2, "start-2", now).state == "claimed"
        current = driver.finish_start(
            2,
            "start-2",
            now + timedelta(minutes=1),
            now,
        )
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
            )
            is None
        )
        assert driver.snapshot().state == "paused"

        _assert_spammed_lifecycle_converges(driver, now)
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
