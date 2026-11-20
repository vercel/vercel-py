from __future__ import annotations

from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler(timezone="UTC")


@scheduler.scheduled_job("cron", hour=4, id="cleanup-expired-sessions")
def cleanup_expired_sessions() -> None:
    print("deleting expired sessions")


@scheduler.scheduled_job("interval", minutes=15, jitter=30, id="refresh-exchange-rates")
def refresh_exchange_rates() -> None:
    print("refreshing exchange rates")


@scheduler.scheduled_job("cron", day="last", hour=23, minute=30, id="month-end-report")
async def month_end_report() -> None:
    print("sending the month-end report")


if __name__ == "__main__":
    scheduler.start()
