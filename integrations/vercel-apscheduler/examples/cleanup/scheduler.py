from __future__ import annotations

from datetime import datetime, timezone
from os import environ

from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from redis import ConnectionPool

UTC = timezone.utc

scheduler = BlockingScheduler(
    timezone=UTC,
    jobstores={
        "default": RedisJobStore(
            connection_pool=ConnectionPool.from_url(
                environ["REDIS_URL"],
                socket_connect_timeout=5,
                socket_timeout=5,
            ),
        )
    },
)


@scheduler.scheduled_job(
    "interval",
    minutes=1,
    jitter=5,
    start_date=datetime(2026, 7, 10, tzinfo=UTC),
    id="every-minute-cleanup",
)
def cleanup_expired_sessions() -> None:
    print("running every minute with 5 seconds jitter")


@scheduler.scheduled_job(
    "interval",
    seconds=30,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    id="frequent-cleanup",
)
def frequent_cleanup() -> None:
    print("running every 30 seconds")


if __name__ == "__main__":
    scheduler.start()
