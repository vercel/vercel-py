# vercel-schedules

Python SDK for Vercel Schedules.

A schedule fires on a cron cadence or once at a fixed instant, and dispatches
to a Vercel Queue topic or function. A schedule is identified by its name
within a namespace. This package manages queue-targeted schedules and parses
function-targeted schedule events.

```sh
pip install vercel-schedules
```

## Usage

```python
from datetime import timedelta
from vercel.schedules import create_schedule, list_schedules

schedule = await create_schedule(
    "cleanup",
    topic="scheduled-cleanup",
    cron="0 * * * *",
    jitter=timedelta(minutes=2),
    payload={"max_age_days": 30},
)

async for schedule in list_schedules(namespace="default"):
    print(schedule.name, schedule.expression, schedule.is_active)
```

`jitter` adds a random delay to each firing. The service represents it as a
whole number of minutes between one and fifteen inclusive.

### Timezones

Cron expressions are evaluated in UTC unless you pass an IANA `timezone`:

```python
await create_schedule(
    "daily-cleanup",
    topic="scheduled-cleanup",
    cron="0 9 * * *",
    timezone="America/Los_Angeles",
)
```

Pass `at=` instead of `cron=` for a schedule that fires once. An aware
`datetime` carrying a `ZoneInfo` (or UTC) supplies the timezone itself; a naive
`datetime` needs an explicit `timezone=`:

```python
from datetime import datetime
from zoneinfo import ZoneInfo

await create_schedule(
    "one-time-cleanup",
    topic="scheduled-cleanup",
    at=datetime(2026, 9, 16, 9, tzinfo=ZoneInfo("America/Los_Angeles")),
)

await create_schedule(
    "one-time-report",
    topic="send-report",
    at=datetime(2026, 9, 16, 9),
    timezone="Europe/Berlin",
)
```

Given both an aware `at` and `timezone`, `at` is converted into that timezone,
so the instant is preserved. Fixed-offset datetimes other than UTC are rejected
because they carry no IANA name.

### Manage schedules

`get_schedule`, `update_schedule`, `enable_schedule`, `disable_schedule`,
`invoke_schedule`, and `delete_schedule` take the schedule name and an optional
`namespace=`. A missing schedule raises `ScheduleNotFoundError`, which is also a
`LookupError`.

```python
from vercel.schedules import invoke_schedule, update_schedule

await update_schedule("cleanup", cron="0 2 * * *", jitter=None)  # jitter=None clears it
await invoke_schedule("cleanup")  # fire now, outside the cadence
```

The same surface is available synchronously, with identical names and arguments:

```python
from vercel.schedules.sync import create_schedule, list_schedules

schedule = create_schedule("cleanup", topic="scheduled-cleanup", cron="0 * * * *")
for schedule in list_schedules():
    ...
```

## Handle a function-targeted schedule

Point a static schedule at a Python callable with a module entrypoint:

```json
{
  "schedules": [
    {
      "name": "cleanup",
      "expression": { "cron": "0 * * * *" },
      "target": { "entrypoint": "jobs.cleanup:run" }
    }
  ]
}
```

The Vercel Python runtime dispatches the schedule to the callable with one
event argument. Static function-targeted schedules do not carry a payload:

```python
# jobs/cleanup.py
async def run(event):
    print(f"Running {event.name}, scheduled at {event.scheduled_at}")
```

The runtime owns the inbound CloudEvent parsing; `vercel-schedules` is only
needed to manage schedules through the API.

## Configuration

Requests authenticate with the deployment's Vercel OIDC token and target the
public Schedules service, overridable with `VERCEL_SCHEDULE_BASE_URL`. Override
either per session:

```python
from vercel.api import session
from vercel.schedules import SchedulesServiceOptions, get_schedule

async with session(service_options=[SchedulesServiceOptions(token="...", base_url="...")]):
    schedule = await get_schedule("cleanup")
```

Use a plain `with` block and `vercel.schedules.sync` together; mixing an async
call into a sync session, or the reverse, is rejected.

## Local development

Run `vercel link` and `vercel env pull` so an OIDC token is available, or pass
`SchedulesServiceOptions(token=...)`.
