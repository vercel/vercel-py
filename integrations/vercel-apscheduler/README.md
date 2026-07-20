# Vercel APScheduler integration

Run APScheduler 3.x schedules through delayed Vercel Queue messages.

For the current testing path, define an ordinary Python Function and its queue
trigger explicitly:

```python
from apscheduler.schedulers.blocking import BlockingScheduler
from vercel.integrations.apscheduler import (
    VercelAPSchedulerOptions,
    get_asgi_app,
    install_vercel_apscheduler_integration,
)

options = VercelAPSchedulerOptions(
    scheduler_id="cleanup",
    wakeup_topic="__aps_cleanup",
    consumer_group="api/scheduler.py",
)
install_vercel_apscheduler_integration(options=options)

scheduler = BlockingScheduler(timezone="UTC")


@scheduler.scheduled_job("cron", hour=4, jitter=120, id="cleanup")
def cleanup(): ...


app = get_asgi_app(scheduler, options=options)
```

```json
{
    "buildCommand": "uv run python -m vercel.integrations.apscheduler --entrypoint api.scheduler:scheduler",
    "functions": {
        "api/scheduler.py": {
            "experimentalTriggers": [
                {
                    "type": "queue/v2beta",
                    "topic": "__aps_cleanup",
                    "maxConcurrency": 1
                }
            ]
        }
    }
}
```

The temporary build command seeds the first wake. After that, the Function is a
single queue subscriber: queue delivery, acknowledgment, and successor
publication are handled by `vercel.queue` inside the returned ASGI app. There
is no HTTP seed route or watchdog cron.

The chain is durable: a wake is acknowledged only after its successor has been
accepted by Vercel Queues. A publish failure fails the current delivery so Vercel
Queues redelivers it; an idempotency-key conflict proves the successor already
exists and is therefore also safe to acknowledge.

This path does not use `[[tool.vercel.subscribers]]`, builder topic extraction,
or runtime auto-installation. The integration package must be an explicit
dependency and installation must happen before constructing the scheduler.

See [SCHEDULER.md](SCHEDULER.md) for the runtime model, convergence walkthroughs,
and schedule restrictions. A deployable example is in
[examples/cleanup](examples/cleanup).
