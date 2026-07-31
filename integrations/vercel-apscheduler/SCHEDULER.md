# APScheduler runtime model

`vercel-apscheduler` turns an APScheduler 3.x scheduler with a Redis job store
into one durable delayed-message chain per Vercel deployment.

The message model is:

```text
a wake message does not represent one job
a wake message asks the scheduler to evaluate all jobs at logical time T
```

This document describes the durable driver: the Redis state machine that
guarantees one logical chain. The APScheduler adapter that publishes and
consumes the actual Queue messages builds on the primitives described here.

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
`claim_start`. `finish_start` atomically marks the start active and reserves
sequence 1 — or, when no work is scheduled, leaves the driver dormant with no
current wake.

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

These properties prevent parallel logical chains. They do not provide
exactly-once side effects: if a process dies after external work but before
its durable bookkeeping, Queue redelivery can execute that work again.
