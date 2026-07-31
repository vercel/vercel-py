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
not configure or publish to them. Build-time imports run in discovery mode;
calls to `start()`, `pause()`, or `resume()` during discovery have no external
effect.

## Runtime lifecycle

On Vercel, the adapter changes only three lifecycle methods:

```python
scheduler.start()
scheduler.pause()
scheduler.resume()
```

`start()` makes the scheduler running if it is not already running. `pause()`
makes it paused. `resume()` has the same durable transition as starting a
paused scheduler. Each method is idempotent. The caller must be executing in
the target deployment; v1 has no cross-deployment control API.

Outside Vercel, APScheduler's original methods are used.

## Durable state

The driver stores one Redis hash per deployment and scheduler:

```text
state              running | paused
generation         monotonically increasing integer
start_status       pending | published | processing | active
current_sequence   monotonically increasing within a generation
current_logical_time
current_status     pending | published | processing
active_owner
active_lease_until
```

Redis Lua scripts update these fields atomically. Driver state has no TTL.

The driver key carries a `{deployment:scheduler}` hash tag, so it always
shares a Redis Cluster slot with the job-store keys that use the same
namespace. Two deployments can use the same Redis database without sharing
driver state.

## Starting

If the driver is already running, `start()` does not create a new generation.
Otherwise it atomically:

1. increments `generation`;
2. sets the state to running;
3. marks the start message pending; and
4. clears the prior generation's current wake.

All concurrent callers observe the same generation, so they publish the same
Queue payload with the same idempotency key. After Queue accepts that message,
`mark_start_published` records it. If the process fails between those
operations, a later `start()` retries the same payload and key.

The start delivery acquires the driver's single active-owner lease through
`claim_start`. It starts APScheduler's internals without starting a background
scheduler thread and materializes code-declared jobs into Redis using
insert-if-absent semantics, so a cold import cannot overwrite a job changed
through a runtime API. `finish_start` then atomically marks the start active
and reserves sequence 1 — or, when no job is scheduled, leaves the driver
dormant with no current wake.

## One wake

A wake carries:

```text
generation
sequence
logical time
```

Before doing work, its delivery must atomically prove all of the following
through `claim_wake`:

- the driver is running;
- its generation is current;
- its sequence and logical time are the current wake token; and
- no other processing attempt owns the active lease.

Only that owner runs work. When it finishes, `finish_wake` verifies ownership
again and replaces the current token with exactly one successor:

```text
sequence = sequence + 1
current logical time = next wake
status = pending
```

The successor is the exact next due time. There are no periodic or idle
heartbeat wakes; once nothing remains scheduled, the chain goes dormant with
no current wake.

If the handler reserves a successor and crashes before Queue publication, the
current token remains pending. A retry or stale delivery sees it and
republishes the exact same payload and key. The Redis token is the
authoritative duplicate-chain fence; the Queue idempotency key is an
additional publication safeguard.

Completion is typed. `finish_start` and `finish_wake` atomically distinguish:

- `advanced` — this owner committed the successor and must publish it;
- `fenced` — the driver was paused, superseded, or already advanced;
  acknowledging the delivery is safe;
- `lost` — the token is still current but another owner replaced this one
  (for example after its lease lapsed mid-execution). The delivery must be
  retried rather than acknowledged, so an unacknowledged message always
  remains as a watchdog while the token is unresolved.

The active-owner lease lasts fifteen minutes and is renewed every minute by
the processing handler. A claim by a different owner succeeds only after the
lease lapses.

## Pausing and resuming

`pause()` atomically changes the state to paused. After pause commits:

- an unclaimed start or wake is stale and does no work;
- an in-flight handler may finish its current work;
- its final Redis transaction cannot reserve a successor.

Resuming reuses `start()`: it atomically creates one new generation. Old
messages remain permanently stale because their generation no longer matches.

The active-owner lease is deliberately not removed by pause or resume. If an
old handler is still executing, the new generation's start delivery retries
until that handler releases its lease. This closes the rapid pause/resume
race: the new generation cannot begin while the old generation is still
inside its critical section.

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

Executing jobs may also mutate the store through the same APIs. An in-job
`add_job()` of an existing id honors `replace_existing=True` by updating the
persisted job, and the finishing wake reads the store again, so the change is
reflected in the successor it reserves.

Every cold Function instance that performs a runtime mutation must first call
the idempotent `scheduler.start()`. Before that boundary, `add_job()` calls
are treated as module-level declarations. Raw writes to Redis job-store keys
are unsupported because they bypass atomic wake rearming and revision checks.

The no-heartbeat design has one deliberate liveness contract: `start()` and
mutation calls are durable after they return successfully. If a process dies
after committing Redis but before publishing Queue, an idempotent `start()`
republishes the pending token. Repeating an interrupted mutation is also safe:
a retried mutation republishes the pending wake even when the retry itself
fails, for example on a conflicting job id. A completely dormant scheduler
does not wake periodically to repair an otherwise unobserved ambiguous
failure. A delayed repair can pass a job's default misfire window; jobs whose
occurrence must remain eligible should set an appropriate
`misfire_grace_time` or `None`.

## Redis and execution requirements

v1 supports exactly one job store named `default`. It must be APScheduler's
Redis-backed `RedisJobStore`, and it is also the lifecycle coordinator. The
integration namespaces the job-store keys with the deployment ID and
subscriber ID, so two deployments can share a Redis database without sharing
jobs or driver state.

Runtime Cache is not suitable. It is evictable and cannot provide the durable
atomic state needed for a pause to remain paused. Redis lifecycle state has no
TTL, so use a durable Redis service rather than an ephemeral cache. Redis
failures fail closed: lifecycle methods raise instead of claiming success,
Queue handlers fail and are retried, and no job runs without acquiring the
Redis owner lease.

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
start it, and it does not move the old deployment's chain. Call
`scheduler.start()` through an authenticated route on the new deployment when
it should begin scheduling, and `scheduler.pause()` on an older deployment
before retiring it when its schedules should stop.

Deleting a deployment prevents its Functions from receiving further work.
Redis records intentionally remain unless the operator removes them; automatic
expiry would make reliable pause semantics impossible.

## Race outcomes

| Race | Outcome |
| --- | --- |
| many concurrent `start()` calls while paused | one generation and one start identity |
| many `start()` calls while running | no lifecycle change |
| duplicate delivery of the same wake | one processing-attempt lease wins |
| `pause()` before a wake claim | wake is stale |
| `pause()` while a wake runs | current work may finish; no successor |
| resume while an old wake runs | new start retries until old owner exits |
| crash before Queue send | pending Redis token is republished |
| old message after resume | generation check makes it stale |
| owner loses its lease mid-execution | `lost` result forces a retry instead of an acknowledgement |
| concurrent runtime add/modify | one token is rearmed; no second chain |
| handler finishes after a job mutation | revision check preserves the mutation |
| retried mutation fails on a conflicting id | the pending wake is still republished |

These properties prevent parallel logical chains. They do not provide
exactly-once job side effects. If a process dies after an external side effect
but before the job's updated run time is durably stored, Queue redelivery can
execute it again. Jobs must be idempotent.
