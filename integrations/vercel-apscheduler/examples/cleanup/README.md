# APScheduler cleanup example

This example uses only the Python Functions contract available today:

```text
vercel.json experimentalTriggers
              |
              v
api/scheduler.py -> get_asgi_app(scheduler)
```

There is no `[[tool.vercel.subscribers]]` discovery step. The same values are
declared explicitly in code and `vercel.json`:

| Field | Value |
| --- | --- |
| function | `api/scheduler.py` |
| scheduler ID | `cleanup` |
| queue topic | `__aps_cleanup` |
| consumer group | `api/scheduler.py` |

The temporary `buildCommand` seeds the first wake during deployment. Deploy the
directory normally:

```bash
cd integrations/vercel-apscheduler/examples/cleanup
vc link
vc deploy --prod
```

After the seed, only queue deliveries invoke this Function. There is no cron or
public HTTP seed endpoint. For this temporary path, deployment must provide the
queue deployment identity and credentials to the build command.

The seed is bootstrap, not a watchdog. Every successful wake delivery durably
publishes its successor before the current wake is acknowledged.

`install_vercel_apscheduler_integration()` must run before the scheduler is
constructed. It captures stable job definitions and deterministic jitter
inputs. `get_asgi_app()` then registers the queue callback and exposes the
plain `vercel.queue` ASGI subscriber.

The interval job has an explicit `start_date`, and every memory-backed job has
an explicit ID. Those anchors let cold starts reconstruct the same cadence.
