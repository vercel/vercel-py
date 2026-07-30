# APScheduler cleanup example

This example deploys a FastAPI app and a queue-driven APScheduler subscriber:

```text
/                                             -> main.py
queue __aps_scheduler_scheduler_start          -> scheduler.py -> first delayed wake
queue __aps_scheduler_scheduler_wakeup         -> scheduler.py -> jobs + successor wake
```

The subscriber is declared in `pyproject.toml`:

```toml
[[tool.vercel.subscribers]]
entrypoint = "scheduler:scheduler"

[tool.vercel.apscheduler.control]
entrypoint = "scheduler:control"
```

The Python builder derives the stable internal ID `scheduler_scheduler` from
the declared module/object pair, activates the adapter before importing
`scheduler.py`, and extracts both registered queue topics. It injects that
registry and the control entrypoint into the web and scheduler Functions. There
is no handwritten topic, scheduler identity, or trigger configuration in
`vercel.json`.

Set `REDIS_URL` and `APSCHEDULER_CONTROL_SECRET` before deploying. The Redis
state is the durable epoch fence used by `control.start()` and `control.stop()`.

Deploy and link the directory normally:

```bash
cd integrations/vercel-apscheduler/examples/cleanup
vc link
vc deploy --prod
```

Start the deployment after it is ready:

```bash
curl -X POST \
  -H "x-control-secret: $APSCHEDULER_CONTROL_SECRET" \
  https://your-deployment.example/scheduler/start
```

Stop it with `/scheduler/stop`. Pass `?deployment=dpl_...` to either route to
target a deployment explicitly. Do not expose these routes without
authentication.

`start()` atomically creates an epoch in Redis and publishes the private start
message. The start subscriber uses the epoch's immutable reference time,
publishes the first delayed message, and marks the subscriber active. Every
recurring wake checks the Redis epoch before work and before publishing its
successor.

Every memory-backed job has an explicit ID, and the interval job has an explicit
`start_date`. Those anchors let cold starts reconstruct the same cadence.
