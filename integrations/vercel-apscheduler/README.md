# Vercel APScheduler integration

Run APScheduler 3.x schedules through delayed Vercel Queue messages.

Declare the scheduler as a Python subscriber:

```toml
[[tool.vercel.subscribers]]
entrypoint = "scheduler:scheduler"
```

```python
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler(timezone="UTC")


@scheduler.scheduled_job("cron", hour=4, jitter=120, id="cleanup")
def cleanup() -> None: ...


if __name__ == "__main__":
    scheduler.start()
```

The Python builder detects APScheduler, activates the Vercel adapter before
importing the subscriber, and derives a stable internal identity from the
`module:object` entrypoint. No Vercel-specific scheduler setup is required.

For `scheduler:scheduler`, scheduler construction registers two subscriptions
in `vercel.queue`:

- `__aps_scheduler_scheduler_start` accepts a manually enqueued `{}` and
  publishes the first delayed wake.
- `__aps_scheduler_scheduler_wakeup` evaluates due jobs and publishes the next
  delayed wake before acknowledging the current one.

The builder reads both topics, their consumer group, and trigger tuning from
`vercel.queue.get_subscriptions()`. It generates the subscriber Function and
`queue/v2beta` triggers; no ASGI app or handwritten `experimentalTriggers`
configuration is needed.

After deploying, manually start the chain:

```bash
uv run python -m vercel.queue send \
  --topic __aps_scheduler_scheduler_start \
  --region iad1 \
  --json '{}'
```

The command targets the linked project's current production deployment by
default. Use `--deployment dpl_...` to choose one explicitly. Nothing is
enqueued during the build.

The chain is durable: a wake is acknowledged only after its successor has been
accepted by Vercel Queues. A publish failure fails the current delivery so
Vercel Queues redelivers it; an idempotency-key conflict proves the successor
already exists and is therefore also safe to acknowledge.

Off Vercel, the same module retains stock APScheduler behavior and
`scheduler.start()` runs normally.

See [SCHEDULER.md](SCHEDULER.md) for the runtime model, convergence walkthroughs,
and schedule restrictions. A deployable example is in
[examples/cleanup](examples/cleanup).
