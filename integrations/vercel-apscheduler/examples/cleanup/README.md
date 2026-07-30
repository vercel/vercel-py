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
```

The Python builder derives the stable internal ID `scheduler_scheduler` from
the declared module/object pair, activates the adapter before importing
`scheduler.py`, and extracts both registered queue topics. There is no
Vercel-specific setup in `scheduler.py` and no handwritten trigger
configuration in `vercel.json`.

Deploy and link the directory normally:

```bash
cd integrations/vercel-apscheduler/examples/cleanup
vc link
vc deploy --prod
```

For now, start the chain manually after the deployment is ready:

```bash
uv run python -m vercel.queue send \
  --topic __aps_scheduler_scheduler_start \
  --region iad1 \
  --json '{}'
```

The queue CLI resolves the linked project's current production deployment.
Pass `--deployment dpl_...` to target a deployment explicitly. The start
subscriber uses the message creation time as its deterministic seed time,
publishes the first delayed message on
`__aps_scheduler_scheduler_wakeup`, and then acknowledges the start message.
Every recurring wake publishes its successor before it is acknowledged.

Every memory-backed job has an explicit ID, and the interval job has an explicit
`start_date`. Those anchors let cold starts reconstruct the same cadence.
