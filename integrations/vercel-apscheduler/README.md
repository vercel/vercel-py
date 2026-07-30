# Vercel APScheduler integration

Run APScheduler 3.x schedules through delayed Vercel Queue messages, with
Redis as the durable job store and lifecycle coordinator.

## Configure the scheduler

Use `VercelRedisJobStore`, which reads `REDIS_URL` by default:

```python
from apscheduler.schedulers.blocking import BlockingScheduler

from vercel.integrations.apscheduler import VercelRedisJobStore

scheduler = BlockingScheduler(
    timezone="UTC",
    jobstores={"default": VercelRedisJobStore()},
)


@scheduler.scheduled_job(
    "cron",
    hour=4,
    id="cleanup",
    replace_existing=True,
)
def cleanup() -> None: ...


if __name__ == "__main__":
    scheduler.start()
```

Declare that object as a Python subscriber:

```toml
[[tool.vercel.subscribers]]
entrypoint = "scheduler:scheduler"
```

The Vercel Python builder detects APScheduler before importing the subscriber,
installs the integration, and extracts its internal Queue subscriptions from
the same registry used by Celery and Dramatiq. Topic names, consumer groups,
scheduler IDs, and installation hooks are not application configuration.

`VercelRedisJobStore(url="redis://...")` accepts an explicit URL. A stock
APScheduler `RedisJobStore` also works. In v1, every configured job store must
be Redis-backed; memory and other persistent job stores are rejected.

## Start, pause, and resume

On Vercel, the normal APScheduler lifecycle methods operate the durable Queue
driver for the deployment executing the call:

```python
scheduler.start()  # idempotently start
scheduler.pause()  # idempotently pause
scheduler.resume()  # idempotently resume
```

Call them from an authenticated runtime route or another trusted runtime
entrypoint. Builds only discover subscribers and never start a schedule.

```python
from fastapi import FastAPI

from scheduler import scheduler

app = FastAPI()


@app.post("/scheduler/start", status_code=202)
def start_scheduler() -> dict[str, str]:
    scheduler.start()
    return {"state": "running"}


@app.post("/scheduler/pause")
def pause_scheduler() -> dict[str, str]:
    scheduler.pause()
    return {"state": "paused"}


@app.post("/scheduler/resume", status_code=202)
def resume_scheduler() -> dict[str, str]:
    scheduler.resume()
    return {"state": "running"}
```

There is no separate control object, start token, or public message-publishing
API. Each deployment controls only its own scheduler.

Off Vercel, these methods retain their normal APScheduler behavior.

## Guarantees

Redis atomically stores one lifecycle generation and one current wake token per
deployment and subscriber. This gives the driver the following guarantees:

- Concurrent or repeated `start()` calls converge on one start message.
- Only the current wake token can run and reserve one successor.
- `pause()` durably fences the current generation.
- `resume()` creates one new generation, even under concurrent calls.
- Rapid `pause()`/`resume()` cannot overlap a new generation with an in-flight
  handler from the old generation.
- A crash between reserving and publishing a successor is repaired by a retry.
- Occurrences during a pause are skipped on resume instead of replayed in a
  catch-up burst.

These are chain guarantees, not exactly-once job execution. Vercel Queues is
at-least-once, so a delivery interrupted after a job's side effect may run that
job again. Scheduled work must still be idempotent. A job already running when
`pause()` commits may finish, but it cannot extend the paused chain.

Redis lifecycle state has no TTL. Losing or evicting it would violate reliable
pause semantics, so use a durable Redis service rather than an ephemeral
cache. Redis failures fail closed: lifecycle calls raise and Queue deliveries
retry without running unfenced work.

## v1 restrictions

- APScheduler 3.x only.
- Redis-backed job stores only.
- The default inline executor only; custom thread/process executors are
  rejected.
- Jobs declared in code need explicit stable IDs.
- When the same ID already exists in Redis, declare it with
  `replace_existing=True`. `scheduled_job()` already enables replacement.
- Job execution is at-least-once.

See [SCHEDULER.md](SCHEDULER.md) for the state machine and failure model. A
deployable example is in [examples/cleanup](examples/cleanup).
