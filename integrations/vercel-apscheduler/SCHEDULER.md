# APScheduler runtime model

`vercel-apscheduler` turns an APScheduler 3.x scheduler with a Redis job store
into one durable delayed-message chain per Vercel deployment.

The message model is:

```text
a wake message does not represent one job
a wake message asks the scheduler to evaluate all jobs at logical time T
```

## Subscriber contract

Applications declare an ordinary scheduler object:

```toml
[[tool.vercel.subscribers]]
entrypoint = "scheduler:scheduler"
```

```python
from apscheduler.schedulers.blocking import BlockingScheduler

from vercel.integrations.apscheduler import VercelRedisJobStore

scheduler = BlockingScheduler(
    timezone="UTC",
    jobstores={"default": VercelRedisJobStore()},
)


@scheduler.scheduled_job("cron", hour=4, id="cleanup")
def cleanup() -> None: ...
```

Before importing the subscriber for build-time introspection, the Python
builder activates the integration. Constructing the scheduler then registers
two internal callbacks in `vercel.queue`:

| Internal subscription | Purpose |
| --- | --- |
| start | Activate a durable generation and reserve its first wake |
| wake | Evaluate due jobs and reserve the next wake |

The builder extracts those callbacks and produces the Function and Queue
triggers. It also injects a mapping from each declared `module:object`
entrypoint to its stable subscriber ID. At runtime, calling a lifecycle method
on the object uses that mapping to select its deployment-scoped Redis record.

The topic names and subscriber ID are implementation details. Applications do
not configure or publish to them.

Build-time imports run in discovery mode. Calls to `start()`, `pause()`, or
`resume()` during discovery have no external effect.

## Runtime lifecycle

On Vercel, the adapter changes only three lifecycle methods:

```python
scheduler.start()
scheduler.pause()
scheduler.resume()
```

`start()` makes the scheduler running if it is not already running. `pause()`
makes it paused. `resume()` has the same durable transition as starting a
paused scheduler. Each method is idempotent.

The caller must be executing in the target deployment. v1 has no cross-
deployment control API.

Outside Vercel, APScheduler's original methods are used.

## Durable state

The default Redis job store supplies both:

1. APScheduler's durable jobs and `next_run_time` values.
2. One integration-owned driver hash for the deployment and subscriber.

The driver records:

```text
state              running | paused
generation         monotonically increasing integer
start_status       pending | published | processing | active
current_sequence   monotonically increasing within a generation
current_logical_time
current_status     pending | published | processing
active_owner
active_lease_until
dirty_logical_time
job_revision
```

Redis Lua scripts update these fields atomically. Driver state has no TTL.

The integration namespaces the Redis job-store keys with the deployment ID and
subscriber ID. Two deployments can therefore use the same Redis database
without sharing jobs or driver state.

## Starting

If the driver is already running, `start()` does not create a new generation.
Otherwise it atomically:

1. increments `generation`;
2. sets the state to running;
3. marks the start message pending; and
4. clears the prior generation's current wake.

All concurrent callers observe the same generation and publish the same Queue
payload with the same idempotency key:

```text
aps:start:<deployment>:<subscriber>:<generation>
```

After Queue accepts that message, Redis marks it published. If the process
fails between those operations, a later `start()` retries the same payload and
key. Queue either accepts it or reports the existing key; both outcomes allow
Redis to advance.

The start delivery acquires the driver's single active-owner lease. It starts
APScheduler's internals without starting a background scheduler thread and
materializes code-declared jobs into Redis using insert-if-absent semantics.
This keeps a cold import from overwriting a job changed through a runtime API.
If at least one job is scheduled, start atomically reserves sequence 1 before
publishing it. If the store has no scheduled jobs, the running driver becomes
dormant with no wake message.

## One wake

A wake carries:

```text
subscriber ID
generation
sequence
logical time
```

Before doing work, its delivery must atomically prove all of the following:

- the driver is running;
- its generation is current;
- its sequence and logical time are the current wake token; and
- no other processing attempt owns the active lease.

Only that owner runs due jobs. When it finishes, one Redis transaction verifies
ownership again and replaces the current token with exactly one successor:

```text
sequence = sequence + 1
current logical time = next scheduler wake
status = pending
```

The handler then publishes that successor using:

```text
aps:wake:<deployment>:<subscriber>:<generation>:<sequence>
```

The Redis token is the authoritative duplicate-chain fence. The Queue
idempotency key is an additional publication safeguard.

If the handler reserves a successor and crashes before Queue publication, the
current token remains pending. A retry or stale delivery sees it and republishes
the exact same payload and key. A handler never acknowledges a successfully
processed nonterminal wake without either publishing or making that pending
successor repairable.

The successor is the exact next due job. There are no periodic or idle
heartbeat wakes. Far-future jobs use deterministic bridge wakes, at most 23
hours apart by default, because Queue delay is bounded. Once no job remains,
the chain goes dormant.

## Runtime job mutations

Runtime calls through APScheduler's public APIs are event-driven:

```python
scheduler.start()  # idempotent activation boundary in this Function instance
scheduler.add_job(...)
scheduler.modify_job(...)
scheduler.reschedule_job(...)
scheduler.pause_job(...)
scheduler.resume_job(...)
scheduler.remove_job(...)
```

The integration coordinates the Redis job write and wake rearm in one Lua
transaction:

- while paused, only the job is changed;
- while running with no active owner, an earlier or missing current wake is
  replaced with one new monotonic sequence;
- while a start or wake owns the driver, the mutation records its earliest
  candidate time and the owner folds that value into its one successor.

Moving or removing a job may leave an already published wake in Queue. Queue
messages cannot be canceled, so that now-empty wake is allowed to arrive; it
recomputes the next exact due time and cannot fork the chain.

Each persisted job has a monotonic revision. After executing a job, the wake
updates or removes it only if the revision it read is still current. A
concurrent runtime mutation therefore wins instead of being overwritten or
resurrected by a late handler.

Every cold Function instance that performs a runtime mutation must first call
the idempotent `scheduler.start()`. Before that boundary, `add_job()` calls are
treated as module-level declarations. Raw writes to Redis job-store keys are
unsupported because they bypass atomic wake rearming and revision checks.

The no-heartbeat design has one deliberate liveness contract: `start()` and
mutation calls are durable after they return successfully. If a process dies
after committing Redis but before publishing Queue, an idempotent `start()`
republishes the pending token. The caller must determine whether an interrupted
job mutation committed before repeating that mutation. A completely dormant
scheduler does not wake periodically to repair an otherwise unobserved
ambiguous failure. A delayed repair can pass a job's default misfire window;
jobs whose occurrence must remain eligible should set an appropriate
`misfire_grace_time` or `None`.

## Pausing and resuming

`pause()` atomically changes the state to paused. It does not delete Queue
messages and it cannot cancel a Python function already executing.

After pause commits:

- an unclaimed start or wake is stale and does no work;
- an in-flight handler may finish its current job;
- its final Redis transaction cannot reserve a successor.

`resume()` atomically creates one new generation. Old messages remain
permanently stale because their generation no longer matches.

The active-owner lease is deliberately not removed by pause or resume. If an
old handler is still executing, the new generation's start delivery retries
until that handler releases its lease. This closes the rapid
pause/resume race: the new generation cannot begin while the old generation is
still inside its critical section.

On activation, persisted `next_run_time` values older than the activation time
are advanced to the first occurrence at or after activation. Time spent paused
is skipped, rather than replayed as a burst.

## Race outcomes

| Race | Outcome |
| --- | --- |
| many concurrent `start()` calls while paused | one generation and one start identity |
| many `start()` calls while running | no lifecycle change |
| duplicate delivery of the same wake | one processing-attempt lease wins |
| `pause()` before a wake claim | wake is stale |
| `pause()` while a wake runs | current job may finish; no successor |
| `resume()` while an old wake runs | new start retries until old owner exits |
| crash before Queue send | pending Redis token is republished |
| old message after resume | generation check makes it stale |
| concurrent runtime add/modify | one token is rearmed; no second chain |
| handler finishes after a job mutation | revision check preserves the mutation |

These properties prevent parallel logical chains. They do not provide
exactly-once job side effects. If a process dies after an external side effect
but before the job's updated run time is durably stored, Queue redelivery can
execute it again. Jobs must be idempotent.

## Redis and execution requirements

v1 supports exactly one job store named `default`. It must be APScheduler
`RedisJobStore`, including `VercelRedisJobStore`, and it is also the lifecycle
coordinator.

`VercelRedisJobStore()` reads `REDIS_URL`; an explicit URL can be passed:

```python
VercelRedisJobStore(url="redis://user:password@example:6379/0")
```

Runtime Cache is not suitable. It is evictable and cannot provide the durable
atomic state needed for a pause to remain paused.

Redis failures fail closed:

- lifecycle methods raise instead of claiming success;
- Queue handlers fail and are retried;
- no job runs without acquiring the Redis owner lease.

Jobs use the integration's inline executor so that a wake remains active until
the job has completed and the durable job-store update has happened. Custom
executors are rejected in v1. A scheduled function can enqueue longer work to
another queue.

Code-declared jobs require explicit stable IDs. If a job with that ID is
already persisted, the declaration must permit replacement, but materializing
the declaration does not overwrite the persisted runtime value. The
`scheduled_job()` decorator enables replacement automatically; declaration
calls to `add_job()` should pass `replace_existing=True`.

## Deployment behavior

Every deployment has an independent job namespace, driver generation, and
Queue partition. Creating or promoting a new deployment does not implicitly
start it, and it does not move the old deployment's chain.

Call `scheduler.start()` through an authenticated route on the new deployment
when it should begin scheduling. Call `scheduler.pause()` on an older
deployment before retiring it when its schedules should stop.

Deleting a deployment prevents its Functions from receiving further work.
Redis records intentionally remain unless the operator removes them; automatic
expiry would make reliable pause semantics impossible.
