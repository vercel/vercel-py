# Vercel APScheduler integration

Run the jobs of an APScheduler 3.x scheduler as Vercel schedules. Each job
deploys as one schedule, and each firing runs the job in a function. There is
no scheduler process, job store, or queue to operate.

## Declare the jobs

Declare the scheduler and its jobs in a module, with an `id` for each job:

```python
# scheduler.py
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler(timezone="UTC")


@scheduler.scheduled_job("cron", hour=4, id="cleanup")
def cleanup() -> None: ...


@scheduler.scheduled_job("interval", minutes=15, id="refresh")
async def refresh() -> None: ...
```

Name the module in `vercel.json` as a Python schedule:

```json
{
    "schedules": {
        "jobs": { "root": ".", "runtime": "python", "entrypoint": "scheduler" }
    }
}
```

The build imports the module and deploys each job as a schedule named
`<key>.<job id>`, here `jobs.cleanup` and `jobs.refresh`. The Vercel Python
builder installs this package when the project depends on APScheduler. Python
schedules are experimental: set `VERCEL_EXPERIMENTAL_PYTHON_SCHEDULES=1` for
the build.

`start()` and `shutdown()` do nothing on Vercel, so a web app can import the
same scheduler, for example to list its jobs.

## Triggers

A schedule fires at most once a minute, on a five-field cron. Each job deploys
as the narrowest cron that fires in every minute its trigger can run in, and
each firing runs the job only if its trigger runs in that minute.

| Trigger | Deployed cron | Runs |
| --- | --- | --- |
| `cron`, `hour=4` | `0 4 * * *` | Every firing |
| `cron`, `day_of_week="mon-fri"` | `0 0 * * 1-5` | Every firing |
| `cron`, `day="last"` | `0 0 28-31 * *` | On the last day of the month |
| `cron`, `day="3rd fri"` | `0 0 15-21 * *` | On the third Friday |
| `cron` with `week`, `year`, `start_date` or `end_date` | The other fields | When the trigger runs |
| `interval`, `minutes=15` | `*/15 * * * *` | Every firing |
| `interval`, `minutes=7` | `* * * * *` | Every seventh firing |
| `calendarinterval`, `months=1`, `hour=3`, `start_date=...` | `0 3 * * *` | On the interval's days |
| `date`, `run_date=...` | Its minute, hour, day and month | Once, in its year |
| `or` and `and` of the above | Their crons combined | When the trigger runs |
| Any other trigger | `* * * * *` | When the trigger runs |

- An interval without `start_date` counts from the Unix epoch, so
  `minutes=15` runs at :00, :15, :30 and :45.
- A calendar interval longer than a day needs `start_date`, and a date trigger
  needs `run_date`.
- A cron whose timezone has no IANA name, such as a fixed offset, deploys as
  `* * * * *`.
- A trigger that runs more than once a minute is rejected.
- Invoking a schedule by hand, such as from the dashboard, runs its job now.

## Job options

| Option | Behavior |
| --- | --- |
| `misfire_grace_time` | Unset, a job runs however late its firing arrives. Set, a firing later than that is skipped. |
| `max_instances` | Counts the runs in one function instance, as a stock scheduler counts the runs in its process. |
| `jitter` | The firing sleeps for a random time up to `jitter` seconds before running the job. |
| `coalesce` | No effect: each firing runs a job at most once. |
| `next_run_time` | Rejected: a job runs only when its trigger fires. |

A firing arrives up to about a minute after its time, and firings can overlap
or, rarely, repeat. Jobs must be idempotent. Listeners added with
`add_listener()` receive each job's executed, error, missed and
max-instances events. A job that raises fails its firing.

## Pause and resume

`pause()` and `resume()` disable and enable the schedules of every job the
scheduler declares, and `pause_job()` and `resume_job()` change one job's
schedule. Call them from any function of the deployment, such as an
authenticated route:

```python
@app.post("/jobs/pause")
def pause_jobs() -> None:
    scheduler.pause()
```

A disabled schedule stays disabled when you deploy again, and a firing already
running finishes. Each call is safe to repeat, so retry one that fails partway
through. In a preview, a deployment's schedules start disabled, and `resume()`
enables them.

## Jobs are fixed at deploy

A deployment's schedules come from the jobs its module declares, so jobs can't
change at runtime:

- `add_job()` after `start()`, or from a running job, raises.
- `modify_job()`, `reschedule_job()`, `remove_job()` and `remove_all_jobs()`
  raise.
- Job stores other than the memory store raise, because their jobs aren't in
  code.

Change a job by changing its declaration and deploying. For a schedule created
at runtime, use `vercel.schedules.create_schedule()`.

Executors are not used: a firing runs a coroutine function on the event loop
and any other job in a worker thread.
