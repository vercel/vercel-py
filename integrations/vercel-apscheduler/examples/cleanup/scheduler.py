from __future__ import annotations

from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler

UTC = timezone.utc

scheduler = BlockingScheduler(timezone=UTC)


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
    id="heartbeat",
)
def heartbeat() -> None:
    print("running every 30 seconds")


if __name__ == "__main__":
    scheduler.start()
