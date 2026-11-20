# APScheduler cleanup example

This example deploys a FastAPI app and the APScheduler jobs in `scheduler.py`
as Vercel schedules.

`vercel.json` names the module that declares the jobs:

```json
{
    "schedules": {
        "jobs": { "root": ".", "runtime": "python", "entrypoint": "scheduler" }
    }
}
```

The build imports `scheduler.py` and deploys each job as a schedule named
after the job's id:

| Schedule | Cron | Timezone |
| --- | --- | --- |
| `jobs.cleanup-expired-sessions` | `0 4 * * *` | UTC |
| `jobs.refresh-exchange-rates` | `*/15 * * * *` | UTC |
| `jobs.month-end-report` | `30 23 28-31 * *` | UTC |

The month-end report fires on days 28 to 31, and runs only on the last day of
the month. The Vercel Python builder installs `vercel-apscheduler` itself, so
the project only depends on APScheduler.

Python schedules are experimental. Set `VERCEL_EXPERIMENTAL_PYTHON_SCHEDULES=1`
for the build, then deploy:

```bash
cd integrations/vercel-apscheduler/examples/cleanup
vc link
vc deploy --prod
```

The app's `/` route lists the jobs from the same scheduler. It never starts the
scheduler: the deployment's schedules run the jobs.
