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
```

`scheduler.py` is ordinary APScheduler code:

```python
from datetime import timezone

from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler(timezone=timezone.utc)


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
| `__aps_scheduler_scheduler_start` | Turn one manually enqueued message into the first delayed wake |
| `__aps_scheduler_scheduler_wakeup` | Evaluate due jobs and publish the successor wake |

`vercel.queue.get_subscriptions()` supplies the builder with both topics, the
sanitized consumer group, retry behavior, and `max_concurrency=1`. The builder
then generates the queue handler and `queue/v2beta` triggers. `vercel.json`
does not duplicate any of these values.

The derived identity is used in payloads, topics, consumer groups, and
idempotency keys. Keeping the same pyproject entrypoint preserves the chain
across deployments; changing the entrypoint intentionally creates a new
scheduler identity that must be started separately.

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

When the scheduler has no future jobs, omitting a successor is intentional: the
schedule is terminal until another start message evaluates a changed definition.

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
new deployment is manually started: wake(10:05) in queue B
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

The new start sees `sync` and schedules `10:05`. If the old `11:00` wake also
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

## Manual Start

```text
manual send -> __aps_scheduler_scheduler_start -> first delayed wakeup
                                             |
                                             v
                                  recurring successor chain
```

Builds cannot enqueue into the deployment's queue consumer registry, so the
integration does not try to seed during the build. After the deployment is
ready, enqueue an empty JSON object on the start topic:

```bash
uv run python -m vercel.queue send \
  --topic __aps_scheduler_scheduler_start \
  --region iad1 \
  --json '{}'
```

The start callback uses the message's immutable creation timestamp as the
reference time. A redelivery of the same start message therefore attempts the
same first wake and converges through the ordinary wake idempotency key. Once
that first wake exists, the recurring subscriber maintains the chain.

This is intentionally a manual bootstrap for now. Automatic activation seeding
needs a platform hook that runs only after the deployment's queue consumers are
registered and routable; this example also deliberately leaves watchdog healing
out of scope.

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
