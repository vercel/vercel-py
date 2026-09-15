# vercel-schedules

Python SDK for Vercel Schedules.

A schedule fires on a cron cadence or once at a fixed instant, and dispatches
to a Vercel Queue topic or function. This package manages queue-targeted
schedules and parses function-targeted schedule events.

```sh
pip install vercel-schedules
```

## Usage

```python
from datetime import timedelta
from vercel.schedules import create_schedule, list_schedules

schedule_id = await create_schedule(
    topic="scheduled-cleanup",
    cron="0 * * * *",
    name="cleanup",
    jitter=timedelta(seconds=30),
    payload={"max_age_days": 30},
)

async for schedule in list_schedules(namespace="default"):
    print(schedule.name, schedule.expression, schedule.is_active)
```

Pass `at=` instead of `cron=` for a schedule that fires once. It must be a
timezone-aware `datetime`:

```python
from datetime import datetime, timezone

await create_schedule(
    topic="send-report",
    at=datetime(2026, 10, 1, 9, tzinfo=timezone.utc),
)
```

`get_schedule`, `enable_schedule`, `disable_schedule`, and `delete_schedule`
take the schedule id. A missing id raises `ScheduleNotFoundError`, which is also
a `LookupError`.

The same surface is available synchronously, with identical names and arguments:

```python
from vercel.schedules.sync import create_schedule, list_schedules

schedule_id = create_schedule(topic="scheduled-cleanup", cron="0 * * * *")
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
      "payload": { "max_age_days": 30 },
      "target": { "entrypoint": "jobs.cleanup:run" }
    }
  ]
}
```

The callable receives a `ScheduleEvent`. Its generic argument controls payload
validation:

```python
# jobs/cleanup.py
from pydantic import BaseModel
from vercel.schedules import ScheduleEvent


class CleanupPayload(BaseModel):
    max_age_days: int


async def run(event: ScheduleEvent[CleanupPayload]) -> None:
    print(f"Running {event.name}, fired at {event.fired_at}")
    if event.payload:
        await delete_expired_records(event.payload.max_age_days)
```

The Vercel Python runtime receives the schedule's CloudEvent `POST`, validates
the payload, and invokes the entrypoint. An unparameterized `ScheduleEvent`
receives the decoded JSON as-is.

When handling the request in your own framework, use
`parse_schedule_event(headers, body, payload_type=...)` to parse the same event.

## Configuration

Requests authenticate with the deployment's Vercel OIDC token and target the
public Schedules service, overridable with `VERCEL_SCHEDULE_BASE_URL`. Override
either per session:

```python
from vercel.api import session
from vercel.schedules import SchedulesServiceOptions, get_schedule

async with session(service_options=[SchedulesServiceOptions(token="...", base_url="...")]):
    schedule = await get_schedule("sch_123")
```

Use a plain `with` block and `vercel.schedules.sync` together; mixing an async
call into a sync session, or the reverse, is rejected.

## Local development

Run `vercel link` and `vercel env pull` so an OIDC token is available, or pass
`SchedulesServiceOptions(token=...)`.
