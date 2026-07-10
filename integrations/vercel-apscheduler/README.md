# Vercel APScheduler integration

Run APScheduler 3.x schedules on Vercel Queues.

```python
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler(timezone="UTC")

@scheduler.scheduled_job("cron", hour=4, id="cleanup")
def cleanup():
    ...
```

On Vercel, expose the scheduler as a queue subscriber entrypoint:

```toml
[[tool.vercel.subscribers]]
entrypoint = "app.schedule:scheduler"
```

When `topics` is omitted, the Python builder treats the subscriber as an
APScheduler object: it injects `vercel-apscheduler`, registers a private queue
topic, auto-installs the runtime patch before importing the scheduler, and seeds
the first wakeup during pre-deploy.

For non-Vercel environments or explicit adoption, call:

```python
from vercel.integrations.apscheduler import install_vercel_apscheduler_integration

install_vercel_apscheduler_integration()
```

The integration adopts stock APScheduler schedulers, uses one delayed Vercel
Queue wake chain per scheduler, and executes due jobs inline at the wake
message's logical scheduler time.
