# APScheduler Runtime Model

`vercel-apscheduler` turns a stock APScheduler 3.x scheduler into one delayed
Vercel Queue wake chain.

The key mental model is:

```text
a message does not say "run cleanup"
a message says     "evaluate this scheduler at logical time T"
```

Each delivery imports the current deployment's code, restores the small amount
of timing state that MemoryJobStore loses on a cold start, runs due jobs, and
publishes one successor before acknowledging the current message.

## Current Deployment Contract

The testing path uses the same explicit Function contract as the Celery
integration. It does not require `[[tool.vercel.subscribers]]` or builder topic
extraction.

`api/scheduler.py` owns the scheduler and its ASGI adapter:

```python
from datetime import UTC

from apscheduler.schedulers.blocking import BlockingScheduler
from vercel.integrations.apscheduler import (
    VercelAPSchedulerOptions,
    get_asgi_app,
    install_vercel_apscheduler_integration,
)

options = VercelAPSchedulerOptions(
    scheduler_id="cleanup",
    wakeup_topic="__aps_cleanup",
    consumer_group="api/scheduler.py",
)
install_vercel_apscheduler_integration(options=options)

scheduler = BlockingScheduler(timezone=UTC)


@scheduler.scheduled_job("cron", hour=4, jitter=120, id="cleanup")
def cleanup(): ...


app = get_asgi_app(scheduler, options=options)


if __name__ == "__main__":
    scheduler.start()
```

`vercel.json` routes the explicit topic to that Function. A temporary build
command seeds the first wake for this test deployment path:

```json
{
    "buildCommand": "uv run python -m vercel.integrations.apscheduler --entrypoint api.scheduler:scheduler",
    "functions": {
        "api/scheduler.py": {
            "experimentalTriggers": [
                {
                    "type": "queue/v2beta",
                    "topic": "__aps_cleanup",
                    "maxConcurrency": 1
                }
            ]
        }
    }
}
```

These values are one protocol and must agree:

| Value | Python | `vercel.json` |
| --- | --- | --- |
| topic | `wakeup_topic="__aps_cleanup"` | `topic: "__aps_cleanup"` |
| consumer | `consumer_group="api/scheduler.py"` | Function path |
| concurrency | `max_concurrency=1` | `maxConcurrency: 1` |

The scheduler ID is a stable logical name used in payloads and idempotency
keys. Keep it unchanged across deployments of the same scheduler.

### Why installation is explicit

The installer patches scheduler construction and `add_job()` before jobs are
declared. That lets the adapter remember whether an ID and interval anchor were
explicit and build a stable schedule fingerprint. Calling only `get_asgi_app()`
after all jobs exist cannot recover those original arguments reliably.

The installer must therefore run before constructing the scheduler. Automatic
installation can return later when the runtime has a stable scheduler-specific
bootstrap contract; the current Function path does not pretend discovery can
supply information that has already been lost.

## One Delivery

For a wake at logical time `T`:

```text
POST queue delivery
        |
        v
import current scheduler definition
        |
restore matching memory timing entries
        |
run every job due at T inline
        |
compute earliest next logical time U
        |
publish wake(U) -> acknowledge wake(T)
```

Publishing before acknowledgment matters. If publication fails, the delivery
fails and Vercel Queues retries it. A crash after a job succeeds but before the
successor is published can run that job again, so scheduled side effects must
be idempotent.

### Durability invariant

For every non-terminal wake `W(T)`, a successful handler response implies that
its successor `W(U)` already exists durably:

```text
ack(W(T)) => accepted(W(U)) or duplicate-key(W(U))
```

The base case is the deployment seed. For the inductive step, the handler sends
`W(U)` before returning. A send failure escapes the subscriber, produces a
failed delivery, and leaves `W(T)` available for redelivery. A duplicate key is
success because it proves another attempt already persisted the same successor.
Therefore, once seeded, normal queue processing cannot acknowledge the last
copy of a wake without first creating the next one.

When the scheduler has no future jobs, omitting a successor is intentional: the
schedule is terminal until a later deployment seeds a changed definition.

Jobs run through an inline executor. A thread or process executor could let the
request return and freeze while work is still running, so custom executors are
rejected. A scheduled function can enqueue longer work to another queue.

## Memory Timing Cursor

Code remains the source of truth for jobs. The message carries only the timing
state needed to reconstruct MemoryJobStore:

```json
{
    "id:cleanup": {
        "job_id": "cleanup",
        "fingerprint": "sha256:...",
        "state": "scheduled",
        "nominal_run_time": "2026-04-09T04:00:00+00:00",
        "next_run_time": "2026-04-09T04:00:47.120000+00:00"
    }
}
```

Reconciliation is mechanical:

| Current code | Cursor | Result |
| --- | --- | --- |
| same ID and fingerprint | matching entry | restore its timing |
| same ID, changed fingerprint | stale entry | discard and recalculate |
| new job | no entry | calculate from the current trigger |
| deleted job | cursor only | discard; there is no function to run |

## Supported Memory Schedules

A cold start and a cursor-free deployment seed must reproduce the same future schedule.
That gives MemoryJobStore a deliberate boundary:

| Feature | Support |
| --- | --- |
| explicit stable job ID | required |
| `CronTrigger` | yes |
| cron jitter | yes, deterministic |
| string `interval` plus explicit `start_date` | yes |
| interval jitter | yes, deterministic |
| interval without explicit `start_date` | rejected |
| pre-built `IntervalTrigger` object | rejected |
| `DateTrigger` and other finite triggers | use a durable store |

An unanchored interval starts at import time plus its interval. Re-importing it
on every delivery continually pushes it into the future, so it may never fire.
A finite memory job can be recreated after its completion. Both cases need
durable state or a different schedule definition.

## Deterministic Jitter

APScheduler normally samples jitter from process-local randomness. That would
make two cold starts disagree about both run time and wake key. For a memory
schedule, the adapter instead computes:

```text
offset = H(scheduler ID, job ID, fingerprint, nominal time) mapped to [0, jitter]
actual time = nominal time + offset
```

The same occurrence gets the same offset in every process and deployment. The
cursor carries nominal and actual time separately. Anchored intervals retain
their nominal cadence rather than accumulating jitter:

```text
nominal(k) = start_date + k * interval
actual(k)  = nominal(k) + deterministic_offset(k)
```

The adapter also looks backward through the jitter window during a fresh seed.
If deployment happens at `12:00:10` and the `12:00:00` occurrence was
deterministically delayed to `12:00:20`, that pending occurrence is retained.

## Wake Identity

A wake for scheduler `S` at logical time `T` uses:

```text
K(S, T) = aps:<scheduler-id>:<UTC logical time>
```

Example:

```text
aps:cleanup:2026-04-09T04:00:47.120000+00:00
```

Vercel Queue idempotency is scoped to a physical deployment queue. Therefore,
the same key in deployment A and deployment B can both exist. Idempotency does
not prevent a temporary fork during deployment.

It does collapse successors after both paths publish into the current physical
queue. That is convergence, not global deduplication.

## Convergence Argument

Fix the current deployment's schedule definitions. Let `E` be the ordered set
of their deterministic logical occurrences, including deterministic jitter.
Define the transition:

```text
F(t) = the first occurrence in E strictly after t
```

A delivery at `t` runs everything due through `t` and publishes `F(t)`. Two
chains can begin at different times `a <= b`:

```text
chain A: a -> F(a) -> F(F(a)) -> ...
chain B: b -> F(b) -> F(F(b)) -> ...
```

Because `E` is ordered, chain A eventually reaches the first current occurrence
after `b`, which is exactly `F(b)`. Both then publish the same key
`K(S, F(b))` into the current queue. The queue retains one message for that
key, and deterministic transitions keep the paths together afterward.

An old wake time does not need to be a member of `E`. It still maps through
`F(old_time)` to the current schedule. The proof relies on these conditions:

1. Both handlers execute current code.
2. Old cursor entries apply only to unchanged fingerprints.
3. Memory schedule transitions are deterministic.
4. Successors are sent to the currently routed deployment queue.
5. Equal `(scheduler ID, logical time)` values use equal keys.

The result is eventual one-chain convergence. It does not promise exactly-once
job execution during the overlap.

### Traceable deployment example

```text
old chain already contains:       wake(11:00) in queue A
new deployment is seeded:         wake(10:05) in queue B
queue B processes 10:05:          publishes wake(11:00), key K(cleanup, 11:00)
old 11:00 routes to current code: publishes wake(12:00), key K(cleanup, 12:00)
new 11:00 processes current code: publishes wake(12:00), same key
queue B after both sends:         one wake(12:00)
```

The `11:00` jobs may run twice. The `12:00` successor is one message.

## Changes Across Deployments

### Add a job

```text
old code: cleanup at 11:00
new code: cleanup at 11:00 + sync at 10:05
deploy:   10:00
```

The new seed sees `sync` and schedules `10:05`. If the old `11:00` wake also
arrives, it imports the new registry and sees both jobs. The chains converge at
a later shared occurrence.

If the new job is later than the existing next wake, no special message is
needed. At the existing wake, current code includes the new job and selects it
when it becomes the earliest successor.

### Delete a job

An old message may carry that job's cursor entry, but current code has no
matching job. The entry is discarded. A wake is an evaluation request, not a
serialized call to the deleted function, so the deleted function cannot run.

### Change a schedule

```text
old: id="cleanup", cron hour=4, fingerprint=A
new: id="cleanup", cron hour=5, fingerprint=B
```

`A != B`, so timing from the old schedule is ignored. Every handler calculates
from the new trigger. Keeping the same ID is fine; changing the fingerprint is
what invalidates stale timing.

## Deployment Seed

The subscriber cannot receive anything until one initial wake exists. For the
current test path, `vercel.json` runs the seed CLI as a temporary build command:

```text
deployment build -> seed first wake -> queue subscriber -> successor chain
```

The command imports the same scheduler entrypoint and publishes its earliest
wake. It requires the build environment to expose the deployment ID, region,
and queue credentials. The equivalent explicit command is:

```bash
VERCEL=1 \
VERCEL_REGION=iad1 \
VERCEL_DEPLOYMENT_ID=YOUR_DEPLOYMENT_ID \
VERCEL_APSCHEDULER_SCHEDULER_ID=cleanup \
VERCEL_APSCHEDULER_TOPIC=__aps_cleanup \
VERCEL_APSCHEDULER_CONSUMER=api/scheduler.py \
python -m vercel.integrations.apscheduler --entrypoint api.scheduler:scheduler
```

There is deliberately no API route, Vercel Cron, or second subscriber. Once
seeded, each delivery publishes its successor before acknowledging itself.
Future `[[tool.vercel.subscribers]]` support can replace only the temporary seed
command; the queue subscriber and scheduling protocol stay the same.

The seed is bootstrap for a deployment, not periodic repair. The example does
not configure `maxDeliveries`, and the integration does not impose a finite
attempt cap by default. There is no expected "dropped chain" state in the
scheduler protocol and therefore no watchdog path.

## Durable Job Stores

A durable job store remains authoritative for job definitions and
`next_run_time`; its state is not copied into the wake payload. Because another
process can insert an earlier durable job, the adapter wakes at least once per
poll interval, 60 seconds by default:

```text
stored next job: 18:00
logical now:     12:00
next wake:       12:01
```

Set `VERCEL_APSCHEDULER_DURABLE_POLL_INTERVAL_SECONDS` to change the cap.
Runtime Cache is not a job store: it is evictable and lacks the atomic job-store
operations APScheduler expects.

## Celery Lessons Applied

The current integration intentionally copies Celery's deployable shape:

1. A normal Python Function declares `experimentalTriggers` in `vercel.json`.
2. User code registers framework-specific behavior with `vercel.queue`.
3. `vercel.queue` owns ASGI dispatch, retries, and acknowledgment.
4. The framework adapter remains an explicit package dependency.

APScheduler's message means "evaluate time T," while Celery's message carries
a specific task invocation. That difference is why APScheduler carries a small
timing cursor and needs one initial deployment seed.

The complete Function example is in `examples/cleanup`.
