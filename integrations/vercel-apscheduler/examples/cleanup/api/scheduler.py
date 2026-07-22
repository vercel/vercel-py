from __future__ import annotations

from datetime import UTC, datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from vercel.integrations.apscheduler import (
    VercelAPSchedulerOptions,
    get_asgi_app,
    install_vercel_apscheduler_integration,
)

OPTIONS = VercelAPSchedulerOptions(
    scheduler_id="cleanup",
    wakeup_topic="__aps_cleanup",
    consumer_group="api/scheduler.py",
)

# Install before constructing the scheduler so job definitions are captured.
install_vercel_apscheduler_integration(options=OPTIONS)
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


app = get_asgi_app(scheduler, options=OPTIONS)


if __name__ == "__main__":
    scheduler.start()
