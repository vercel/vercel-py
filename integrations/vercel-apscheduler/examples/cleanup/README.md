# APScheduler cleanup example

This example uses two file-based Python Functions:

```text
/                  --rewrite--> api/index.py
queue __aps_cleanup ----------> api/scheduler.py -> get_asgi_app(scheduler)
Vercel Cron -------------------> api/scheduler_watchdog.py -> seed
```

Both files live under `api/`, so Vercel discovers them as separate Functions.
The framework preset is disabled to prevent root FastAPI zero-config from
collapsing the project into one Function. The tracked `public/` directory is
configured as an otherwise empty static output.

There is no `[[tool.vercel.subscribers]]` discovery step. The scheduler values
are declared explicitly in code and `vercel.json`:

| Field | Value |
| --- | --- |
| hello function | `api/index.py` |
| function | `api/scheduler.py` |
| scheduler ID | `cleanup` |
| queue topic | `__aps_cleanup` |
| consumer group | `api/scheduler.py` |
| watchdog function | `api/scheduler_watchdog.py` |
| queue and Function region | `iad1` |

Deploy the directory. The every-minute watchdog cron seeds the final deployment
partition after activation:

```bash
cd integrations/vercel-apscheduler/examples/cleanup
vc link
vc deploy --prod
```

The scheduler Function remains a private queue consumer. The public watchdog
Function exists only for Vercel Cron; it seeds the current deployment and
periodically reconciles the wake frontier. Both Functions run in `iad1`, so
every wake uses the same regional queue endpoint.

`buildCommand` is explicitly `null` so a previously configured Project Settings
command cannot run the obsolete build-time seed.

Every successful wake delivery durably publishes its successor before the
current wake is acknowledged. Repeated watchdog seeds use deterministic keys,
so a healthy chain absorbs them.

`install_vercel_apscheduler_integration()` must run before the scheduler is
constructed. It captures stable job definitions and deterministic jitter
inputs. `get_asgi_app()` then registers the queue callback and exposes the
plain `vercel.queue` ASGI subscriber.

The interval job has an explicit `start_date`, and every memory-backed job has
an explicit ID. Those anchors let cold starts reconstruct the same cadence.
