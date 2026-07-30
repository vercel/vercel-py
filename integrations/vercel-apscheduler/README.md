# Vercel APScheduler integration

Run APScheduler 3.x schedules through delayed Vercel Queue messages.

Declare the scheduler as a Python subscriber:

```toml
[[tool.vercel.subscribers]]
entrypoint = "scheduler:scheduler"

[tool.vercel.apscheduler.control]
entrypoint = "scheduler:control"
```

```python
from apscheduler.schedulers.blocking import BlockingScheduler
from vercel.integrations.apscheduler.control import Control, RedisControlBackend

scheduler = BlockingScheduler(timezone="UTC")
control = Control(backend=RedisControlBackend())


@scheduler.scheduled_job("cron", hour=4, jitter=120, id="cleanup")
def cleanup() -> None: ...


if __name__ == "__main__":
    scheduler.start()
```

The Python builder detects APScheduler, activates the Vercel adapter before
importing the subscriber, and derives a stable internal identity from the
`module:object` entrypoint. It also injects the discovered APScheduler
subscriber registry into web Functions and scheduler subscriber Functions.
Scheduler topics, consumer groups, and identities are not user configuration.

For `scheduler:scheduler`, scheduler construction registers two subscriptions
in `vercel.queue`:

- `__aps_scheduler_scheduler_start` accepts the control plane's internal start
  envelope and publishes the first delayed wake.
- `__aps_scheduler_scheduler_wakeup` evaluates due jobs and publishes the next
  delayed wake before acknowledging the current one.

The builder reads both topics, their consumer group, and trigger tuning from
`vercel.queue.get_subscriptions()`. It generates the subscriber Function and
`queue/v2beta` triggers; no ASGI app or handwritten `experimentalTriggers`
configuration is needed.

Install the Redis extra and provide `REDIS_URL`:

```toml
dependencies = [
  "APScheduler>=3.10.4,<4",
  "vercel-apscheduler",
  "redis>=5,<7",
]
```

`RedisControlBackend(host="redis://...")` may be used instead. Redis state has
no automatic expiry: losing a stop marker would let a paused chain resume, so
the integration deliberately prefers durable state over silent cleanup.

Call the control object from an API route or administrative command after the
deployment is ready:

```python
control.start()
control.stop()

control.start(deployment="dpl_abc")
control.stop(deployment="dpl_abc")
```

With no `deployment`, the SDK uses `VERCEL_DEPLOYMENT_ID`, so it controls the
deployment executing the call. Nothing is enqueued during the build.
An explicit target uses the caller deployment's injected scheduler registry;
invoke the control route on the target itself if two deployments declare
different scheduler entrypoints.

Redis atomically assigns each start a monotonically increasing internal epoch.
Concurrent or repeated starts use the same Queue idempotency key and cannot
create separate logical chains. Every wake checks the durable epoch before
running and again immediately before publishing its successor. Stopping fences
the old epoch; starting again creates a new one. A restart skips occurrences
whose scheduled times fell inside the stopped interval. It does not replay them
as an immediate catch-up burst, even when the subscriber Function stays warm
or the internal start message is delayed.

The queue remains at-least-once. A delivery may run a job more than once after
a crash, and a job already executing when `stop()` commits may finish. Scheduled
side effects must therefore still be idempotent.

Off Vercel, the same module retains stock APScheduler behavior and
`scheduler.start()` runs normally.

See [SCHEDULER.md](SCHEDULER.md) for the runtime model, convergence walkthroughs,
and schedule restrictions. A deployable example is in
[examples/cleanup](examples/cleanup).
