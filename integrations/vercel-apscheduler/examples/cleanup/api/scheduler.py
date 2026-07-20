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


@scheduler.scheduled_job("cron", hour=4, minute=0, jitter=120, id="nightly-cleanup")
def cleanup_expired_sessions() -> None:
    print("cleaning up expired sessions")


@scheduler.scheduled_job(
    "interval",
    minutes=15,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    id="heartbeat",
)
def heartbeat() -> None:
    print("scheduler heartbeat")


app = get_asgi_app(scheduler, options=OPTIONS)


if __name__ == "__main__":
    scheduler.start()
