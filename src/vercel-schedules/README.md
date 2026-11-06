# vercel-schedules

Python SDK for [Vercel Schedules](https://vercel.com/docs/schedules).

```sh
pip install vercel-schedules
```

## Manage schedules

A schedule fires on a cron cadence or once at a fixed time, and publishes to a
Vercel Queue topic. It is identified by its name within a namespace, which
defaults to `"default"`.

```python
from datetime import timedelta
from vercel.schedules import Cron, create_schedule, list_schedules

schedule = await create_schedule(
    "cleanup",
    topic="scheduled-cleanup",
    cron=Cron(hour=3),  # every day at 03:00
    jitter=timedelta(minutes=2),
    payload={"max_age_days": 30},
)

async for schedule in list_schedules():
    print(schedule.name, schedule.expression, schedule.is_active)
```

`cron=` takes a `Cron` or a crontab string such as `"0 3 * * *"`. Fields you
leave out of a `Cron` default to `*`, except fields less significant than every
field you set, which default to their minimum, as in APScheduler's
`CronTrigger`. `Cron(hour=3)` is `0 3 * * *`, and `Cron(day=1)` is `0 0 1 * *`.
`str(cron)` renders the crontab expression, and `Cron.from_crontab()` parses one.

`jitter` spreads each firing over a random delay of 1 to 15 whole minutes.

### Timezones

Cron expressions are evaluated in UTC unless you pass an IANA `timezone`:

```python
await create_schedule(
    "daily-report",
    topic="send-report",
    cron=Cron(hour=9),
    timezone="America/Los_Angeles",
)
```

Pass `at=` instead of `cron=` for a schedule that fires once. An aware
`datetime` carrying a `ZoneInfo` (or UTC) supplies the timezone itself. A naive
`datetime` needs an explicit `timezone=`:

```python
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

await create_schedule(
    "launch",
    topic="launch",
    at=datetime(2026, 9, 16, 9, tzinfo=ZoneInfo("America/Los_Angeles")),
)

await create_schedule(
    "follow-up",
    topic="send-report",
    at=datetime.now(timezone.utc) + timedelta(hours=1),
)
```

Given both an aware `at` and `timezone`, `at` is converted into that timezone,
so the instant is preserved. One-off schedules have minute precision: a time
with seconds is rounded up to the next minute, so the schedule never fires
before `at`. Fixed-offset datetimes other than UTC are rejected because they
carry no IANA name.

### Change, pause, and run schedules

`get_schedule`, `update_schedule`, `enable_schedule`, `disable_schedule`,
`invoke_schedule`, and `delete_schedule` take the schedule name and an optional
`namespace=`. A missing schedule raises `ScheduleNotFoundError`, which is also a
`LookupError`.

```python
from datetime import datetime, timezone
from vercel.schedules import enable_schedule, invoke_schedule, update_schedule

await update_schedule("cleanup", cron="0 2 * * *", jitter=None)  # jitter=None clears it
await invoke_schedule("cleanup")  # fire now, outside the cadence
await enable_schedule("cleanup", until=datetime(2026, 12, 1, tzinfo=timezone.utc))
```

Schedules declared in `vercel.json` can only be enabled or disabled through the
API. Other API errors raise `SchedulesApiError`, whose `code` names the reason,
such as `"schedule_already_exists"`, and whose `retry_after` holds the seconds
to wait after `"rate_limited"`.

The same surface is available synchronously, with identical names and arguments:

```python
from vercel.schedules.sync import create_schedule, list_schedules

schedule = create_schedule("cleanup", topic="scheduled-cleanup", cron="0 * * * *")
for schedule in list_schedules():
    ...
```

### Configuration

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

Under `vercel dev`, the SDK uses the local Schedules API that `vercel dev`
serves, with the token it provides. Elsewhere, run `vercel link` and
`vercel env pull` so an OIDC token is available, or pass
`SchedulesServiceOptions(token=...)`.

## Read the firing in a schedule command

A schedule with a `command` builds the image from the Dockerfile in `root`, and
runs the command once per firing:

```json
{
  "schedules": {
    "reindex": {
      "schedule": "0 3 * * *",
      "root": "jobs",
      "entrypoint": "Dockerfile",
      "command": ["python", "reindex.py"]
    }
  }
}
```

The firing's metadata is in `VERCEL_SCHEDULE_*` environment variables.
`ScheduleMetadata.from_env()` reads them:

```python
# jobs/reindex.py
from vercel.schedules import ScheduleMetadata

metadata = ScheduleMetadata.from_env()
print(f"Running {metadata.name}, scheduled at {metadata.scheduled_at}")
```

A non-zero exit status marks the firing as failed. Failed firings are not
retried.

Run by hand, without the variables, `from_env()` raises
`NotAScheduleInvocationError`. The package is optional here:
`os.environ["VERCEL_SCHEDULE_NAME"]` works too.

| Variable | `ScheduleMetadata` attribute |
| --- | --- |
| `VERCEL_SCHEDULE_ID` | `schedule_id` |
| `VERCEL_SCHEDULE_EXECUTION_ID` | `execution_id` |
| `VERCEL_SCHEDULE_NAME` | `name` |
| `VERCEL_SCHEDULE_NAMESPACE` | `namespace` |
| `VERCEL_SCHEDULE_SCHEDULED_AT` | `scheduled_at`, as an aware UTC `datetime` |
| `VERCEL_SCHEDULE_SOURCE` | `source` |
| `VERCEL_SCHEDULE_EXPRESSION` | `expression` |
