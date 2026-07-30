# APScheduler cleanup example

This example deploys a FastAPI app and one queue-driven APScheduler subscriber.

The subscriber is declared in `pyproject.toml`:

```toml
[[tool.vercel.subscribers]]
entrypoint = "scheduler:scheduler"
```

`scheduler.py` configures `VercelRedisJobStore()`, which reads `REDIS_URL`.
The Python builder derives the scheduler identity from the subscriber
entrypoint and extracts its internal Queue subscriptions automatically. No
topic or trigger configuration is needed.

Set `REDIS_URL` and `APSCHEDULER_ADMIN_SECRET`, then deploy:

```bash
cd integrations/vercel-apscheduler/examples/cleanup
vc link
vc deploy --prod
```

Start the scheduler after the deployment is ready:

```bash
curl -X POST \
  -H "x-admin-secret: $APSCHEDULER_ADMIN_SECRET" \
  https://your-deployment.example/scheduler/start
```

Pause and resume it through `/scheduler/pause` and `/scheduler/resume`.
Do not expose these routes without authentication.

These calls affect the deployment serving the request. Repeated and concurrent
lifecycle calls are safe: Redis fences generations and permits only one current
wake chain. Pausing does not cancel a job already executing, but it prevents
that generation from publishing another wake. Resuming skips occurrences that
fell inside the paused interval.
