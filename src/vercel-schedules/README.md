# vercel-schedules

Python SDK for [Vercel Schedules](https://vercel.com/docs/schedules).

```sh
pip install vercel-schedules
```

## Read the firing in a schedule command

A schedule with `run` builds the image from the Dockerfile in `root`, and runs
the command once per firing:

```json
{
  "schedules": {
    "reindex": {
      "schedule": "0 3 * * *",
      "root": "jobs",
      "entrypoint": "Dockerfile",
      "run": ["python", "reindex.py"]
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
