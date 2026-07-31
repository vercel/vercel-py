# Vercel APScheduler integration

Run APScheduler 3.x schedules through delayed Vercel Queue messages, with
Redis as the durable job store and lifecycle coordinator.

## Configure the scheduler

Use APScheduler's standard Redis job store:

```python
from os import environ

from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from redis import ConnectionPool

scheduler = BlockingScheduler(
    timezone="UTC",
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
    "cron",
    hour=4,
    id="cleanup",
    replace_existing=True,
)
def cleanup() -> None: ...
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

No Vercel-specific job store is required. v1 requires exactly one job store,
named `default`, and it must be APScheduler's Redis-backed `RedisJobStore`.
The integration uses that store's configured Redis client for its internal
lifecycle coordination. A missing `REDIS_URL` fails the import with a
`KeyError`, which is intended: there is no implicit localhost fallback.

Set explicit socket timeouts on the connection pool, as shown above, so an
unreachable Redis fails fast instead of stalling lifecycle calls and Queue
deliveries.

## Start, pause, and resume

On Vercel, the normal APScheduler lifecycle methods operate the durable Queue
driver for the deployment executing the call:

```python
scheduler.start()  # idempotently start
scheduler.pause()  # idempotently pause
scheduler.resume()  # idempotently resume
```

Use these methods when explicit operational control is needed. Call them from
an authenticated runtime route or another trusted runtime entrypoint.

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

## Runtime job changes

The chain sleeps until the next persisted job is due. It emits no idle
heartbeat. `add_job()`, `modify_job()`, `reschedule_job()`, `pause_job()`,
`resume_job()`, and removals update Redis and rearm the one current wake as
needed.

Call `scheduler.start()` first in each Function instance that changes jobs;
before that boundary, `add_job()` calls are treated as module-level
declarations. The call is idempotent:

```python
@app.post("/jobs")
def add_job() -> dict[str, str]:
    scheduler.start()
    scheduler.add_job(
        send_report,
        "date",
        run_date="2026-08-01 09:00:00+00:00",
        id="report-2026-08-01",
    )
    return {"state": "scheduled"}
```

Job writes and wake rearming happen in one Redis transaction. Raw writes to
the APScheduler Redis keys bypass that transaction and are unsupported.

## Guarantees

Redis atomically stores one lifecycle generation and one current wake token per
deployment and subscriber. This gives the driver the following guarantees:

- Concurrent or repeated `start()` calls converge on one start message.
- Only the current wake token can run and reserve one successor.
- `pause()` durably fences the current generation.
- `resume()` creates one new generation, even under concurrent calls.
- Rapid `pause()`/`resume()` cannot overlap a new generation with an in-flight
  handler from the old generation.
- Runtime job changes cannot create a second chain.
- Concurrent job execution and mutation cannot overwrite or resurrect a stale
  job value.
- A crash between reserving and publishing a successor is repaired by a retry.
- Occurrences during a pause are skipped on resume instead of replayed in a
  catch-up burst.

`start()` and job mutation calls are durable after they return successfully.
If a process dies before returning, an idempotent `start()` repairs any pending
publication, and repeating an interrupted mutation republishes the pending
wake even when the retry itself fails on a conflicting job id. With no idle
heartbeat, an ambiguous failure while publishing the first wake for a dormant
scheduler is repaired by a later `start()` or mutation call, not by a periodic
timer. Set an appropriate `misfire_grace_time` (or `None`) for jobs whose
occurrences must remain eligible after a delayed repair.

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
- Exactly one Redis-backed job store named `default`.
- The default inline executor only; custom thread/process executors are
  rejected.
- Jobs declared in code need explicit stable IDs.
- When the same ID already exists in Redis, declare it with
  `replace_existing=True`. `scheduled_job()` already enables replacement.
- Runtime mutation APIs require a prior idempotent `scheduler.start()` in
  that Function instance.
- Job execution is at-least-once.

See [SCHEDULER.md](SCHEDULER.md) for the state machine and failure model. A
deployable example is in [examples/cleanup](examples/cleanup).
