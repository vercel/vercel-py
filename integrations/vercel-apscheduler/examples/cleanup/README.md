# APScheduler cleanup example

This example deploys a FastAPI app and one queue-driven APScheduler subscriber.

The subscriber is declared in `pyproject.toml`:

```toml
[[tool.vercel.subscribers]]
entrypoint = "scheduler:scheduler"
```

`scheduler.py` configures APScheduler's standard `RedisJobStore` with
`REDIS_URL`. The Python builder derives the scheduler identity from the
subscriber entrypoint and extracts its internal Queue subscriptions
automatically. No topic or trigger configuration is needed.

Set `REDIS_URL` and `APSCHEDULER_ADMIN_SECRET`, then deploy:

```bash
cd integrations/vercel-apscheduler/examples/cleanup
vc link
vc deploy --prod
```

Production activates the scheduler automatically on its first request. For
example:

```bash
curl https://your-deployment.example/
```

Previews are inactive by default. To keep a preview active while it is
receiving traffic, add:

```toml
[tool.vercel.apscheduler.previews]
enabled = true
idle_timeout = "30m"
```

The example retains authenticated `/scheduler/start`, `/scheduler/pause`, and
`/scheduler/resume` routes for explicit operational control. Do not expose
these routes without authentication.

These calls affect the deployment serving the request. Repeated and concurrent
lifecycle calls are safe: Redis fences generations and permits only one current
wake chain. Pausing does not cancel a job already executing, but it prevents
that generation from publishing another wake. Resuming skips occurrences that
fell inside the paused interval.
