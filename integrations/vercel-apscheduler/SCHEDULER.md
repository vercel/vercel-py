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

## Subscriber Contract

Declare the stock scheduler object as a subscriber in `pyproject.toml`:

```toml
[[tool.vercel.subscribers]]
entrypoint = "scheduler:scheduler"

[tool.vercel.apscheduler.control]
entrypoint = "scheduler:control"
```

`scheduler.py` is ordinary APScheduler code:

```python
from datetime import timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from vercel.integrations.apscheduler.control import Control, RedisControlBackend

scheduler = BlockingScheduler(timezone=timezone.utc)
control = Control(backend=RedisControlBackend())


@scheduler.scheduled_job("cron", hour=4, jitter=120, id="cleanup")
def cleanup() -> None: ...


if __name__ == "__main__":
    scheduler.start()
```

The builder detects the APScheduler dependency and activates the adapter before
importing `scheduler`. It derives the internal scheduler identity from the
canonical subscriber name—the stable `module:object` pair already declared in
`pyproject.toml`. For `scheduler:scheduler`, that identity is
`scheduler_scheduler`.

Scheduler construction then registers both topics in the process-wide
`vercel.queue` registry:

| Topic | Purpose |
| --- | --- |
| `__aps_scheduler_scheduler_start` | Turn one epoch-fenced control message into the first delayed wake |
| `__aps_scheduler_scheduler_wakeup` | Evaluate due jobs and publish the successor wake |

`vercel.queue.get_subscriptions()` supplies the builder with both topics, the
sanitized consumer group, retry behavior, and `max_concurrency=1`. The builder
then generates the queue handler and `queue/v2beta` triggers. `vercel.json`
does not duplicate any of these values.

The derived identity is used in payloads, topics, consumer groups, and
idempotency keys. The builder also injects the exact discovered APScheduler
subscriber names and the control entrypoint into web Functions and scheduler
subscriber Functions. Unrelated queue subscribers do not import the control
module. Users do not configure internal topic names, consumer groups, scheduler
IDs, or epochs.

The adapter patches scheduler construction and `add_job()` before the module is
imported. That lets it remember whether an ID and interval anchor were explicit
and build a stable schedule fingerprint. On Vercel, construction also registers
the start and wake callbacks. Off Vercel, no queue callbacks are registered and
the stock scheduler loop remains available.

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

The base case is the first wake published by the manual start subscriber. For
the inductive step, the handler sends
`W(U)` before returning. A send failure escapes the subscriber, produces a
failed delivery, and leaves `W(T)` available for redelivery. A duplicate key is
success because it proves another attempt already persisted the same successor.
Therefore, once seeded, normal queue processing cannot acknowledge the last
copy of a wake without first creating the next one.

When the scheduler has no future jobs, omitting a successor is intentional. A
controlled deployment remains logically running; call `stop()` followed by
`start()` to create a new epoch if a changed definition needs to be evaluated.

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

A cold start and a cursor-free manual start must reproduce the same future
schedule. That gives MemoryJobStore a deliberate boundary:

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

The adapter also looks backward through the jitter window during a fresh start.
If deployment happens at `12:00:10` and the `12:00:00` occurrence was
deterministically delayed to `12:00:20`, that pending occurrence is retained.

## Wake Identity

A wake for scheduler `S`, control epoch `E`, and logical time `T` uses:

```text
K_wake(S, E, T) = aps:<scheduler-id>:<epoch>:<UTC logical time>
```

Example:

```text
aps:scheduler_scheduler:7:2026-04-09T04:00:47.120000+00:00
```

A start message uses:

```text
K_start(D, E, S) = aps:start:<deployment>:<epoch>:<subscriber>
```

Queue idempotency is scoped to a deployment. Including the epoch deliberately
separates a new run from every stale message left by an older run.

## Durable Control Plane

Configure one durable control object:

```toml
[tool.vercel.apscheduler.control]
entrypoint = "scheduler:control"
```

```python
from vercel.integrations.apscheduler.control import Control, RedisControlBackend

control = Control(backend=RedisControlBackend())
```

`RedisControlBackend()` reads `REDIS_URL` lazily. An explicit host or URL wins:

```python
RedisControlBackend(host="redis://user:password@example:6379/0")
RedisControlBackend(host="redis.internal", port=6379, ssl=True)
```

Install the Redis client alongside the integration:

```toml
dependencies = ["vercel-apscheduler", "redis>=5,<7"]
```

Call it from an authenticated administrative API route or command:

```python
control.start()
control.stop()
control.status()

control.start(deployment="dpl_abc")
control.stop(deployment="dpl_abc")
control.status(deployment="dpl_abc")
```

With no explicit deployment, `VERCEL_DEPLOYMENT_ID` selects the deployment
executing the call. An explicit target uses the caller deployment's injected
scheduler registry. If two deployments declare different scheduler
entrypoints, invoke the control route on the target deployment itself so its
own registry is used.

Redis stores one control hash per deployment:

```text
state = running | stopped
epoch = monotonically increasing integer
reference_time = immutable timestamp for the current epoch
```

Each subscriber also has a seed state for that epoch:

```text
pending -> published -> active
activation_time = immutable first-delivery timestamp
```

`start()` atomically creates an epoch only when the deployment is stopped.
Every concurrent caller sees the same epoch, timestamp, and pending seeds.
They may race to send, but all use the same `K_start`; Queue accepts one. If a
caller crashes before or during publication, the seed stays pending and a
later `start()` retries the same payload and idempotency key.

The start handler checks Redis and atomically claims a per-subscriber activation
time on its first delivery. It derives the first wake from that timestamp and
carries the epoch into every successor. Redis returns the same activation time
to every retry, so duplicate start deliveries calculate the same first wake
key. Using first delivery rather than API-call time also prevents Queue latency
from creating historical catch-up wakes.

Every wake performs two durable checks:

1. Before running jobs, its epoch must be the deployment's current running
   epoch.
2. Immediately before publishing a successor, that same condition must still
   hold.

`stop()` atomically changes the state to stopped. A wake that has not begun
becomes a no-op. A wake already executing may finish its current jobs, but the
second check prevents it from extending the old chain. If `start()` follows,
Redis increments the epoch, so every remaining old start or wake message is
permanently stale.

The new epoch is a fresh scheduling generation. On a warm Function, the adapter
rebases any in-memory or durable `next_run_time` left by the previous epoch to
the first occurrence at or after its activation time. Occurrences inside the
stopped interval are skipped instead of being replayed as immediate wake
messages. A wake also restores its message-carried MemoryJobStore cursor into a
warm scheduler before selecting due jobs, so sequential deliveries may safely
land on different warm instances.

Control keys intentionally have no TTL. Expiring a stopped record could make a
deployment appear startable under an old generation and violate the promise
that schedules never resume spontaneously. Redis failures also fail closed:
control calls raise, and subscriber deliveries fail for Queue retry rather than
running without a fence.

These rules guarantee one current logical chain per deployment. They do not
make job execution exactly once. Queue can redeliver a wake after a crash, so
scheduled side effects must remain idempotent.

## Changes Across Deployments

Every Vercel deployment has its own Queue partition and Redis control record.
Creating or aliasing a new production deployment does not move an existing
chain and does not start the new one.

After promoting a deployment, call `control.start()` through that deployment
to start its schedules. Stop an older deployment explicitly with
`control.stop(deployment="dpl_old")` when it should no longer schedule work.
Targeted calls publish into the named deployment's Queue partition.

This separation is what makes rollback behavior explicit: the old deployment
can be started again under a new epoch without reviving any stale messages from
its earlier run. Deleting a deployment prevents its Functions from receiving
more work, but the Redis control keys deliberately remain until an operator
cleans them up.

Changing a job definition creates a new deployment with a new chain. Its first
seed calculates timing from that deployment's code. Stable IDs are still
required for MemoryJobStore cursor reconciliation within a run.

## Starting After Deployment

Builds cannot enqueue into the deployment's queue consumer registry, so the
integration does not seed during the build. Expose an authenticated route or
run an administrative command that imports the configured object and calls:

```python
from scheduler import control

result = control.start()
```

The SDK publishes the private start envelope only after Redis has durably
created the epoch. Applications should not publish directly to the internal
start topic: doing so bypasses the durable state transition and the subscriber
will ignore the message.

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

1. `[[tool.vercel.subscribers]]` points at a framework object.
2. Importing that module registers framework-specific callbacks with
   `vercel.queue`.
3. The Python builder extracts subscriptions from the queue registry and
   generates the Function triggers.
4. `vercel.queue` owns ASGI dispatch, retries, and acknowledgment.
5. The framework adapter remains an explicit package dependency.

APScheduler's message means "evaluate time T," while Celery's message carries
a specific task invocation. That difference is why APScheduler carries a small
timing cursor and needs one manual start after deployment.

The complete Function example is in `examples/cleanup`.
