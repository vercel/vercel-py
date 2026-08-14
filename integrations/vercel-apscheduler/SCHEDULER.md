# APScheduler runtime model

`vercel-apscheduler` turns an APScheduler 3.x scheduler into one durable
delayed-message chain per Vercel deployment, on a managed job store.

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
triggers. The entrypoint is a locator only: the scheduler's durable identity
is the builder-assigned subscriber id, which is refactor-stable. Renaming the
variable or moving the module never orphans the durable namespace or the
queue topics. The `scheduler_id` integration option pins an identity
explicitly.

Two schedulers that derive one identity fail loudly at import; give each
scheduler a distinct identity.

The topic names are implementation details. Applications do not configure or
publish to them. Build-time imports run in discovery mode;
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

Named environments (production and custom environments) additionally register
an automatic activation hook during import. The Vercel Python Runtime
executes it around the application's first real request, after request-scoped
OIDC credentials are available. Automatic activation never overrides an
explicitly paused driver. Preview deployments register the same hook only
when enabled in `pyproject.toml`:

```toml
[tool.vercel.apscheduler.previews]
enabled = true
idle_timeout = "30m"
```

Preview activation is recurring but request-driven. Eligible incoming requests
renew the durable idle deadline, with process-local throttling capped at five
minutes. There is no background timer and no activity Queue topic. Idle
deadlines apply to previews only; a custom environment is a named environment
and never idles.

Outside Vercel, APScheduler's original methods are used.

## Coordination substrate

The managed store runs on the Vercel Runtime Cache: `get`/`set`/`delete` with
TTLs and tags — no compare-and-swap, no transactions, and entries may be
evicted. Each guarantee therefore lives on something that can carry it:

- **The single-successor rule lives in the queue.** Racing finishers compute
  the same canonical successor and the same idempotency key, and the queue
  accepts the publication once. Claims are best-effort filters that shrink,
  but cannot eliminate, duplicate wake *executions*; the contract is
  at-least-once.
- **Chain progress travels in the messages.** A claim adopts a generation or
  wake token that is ahead of the local document, so an evicted document —
  or another process's memory under `vercel dev` — never strands the chain.
  A `paused` document fences only its own and older generations; a resume's
  new generation revives it.
- **Job-store writes are owner-fenced best-effort.** The fence is checked
  against the driver document rather than atomically with the write, so a
  demoted deployment's stale pass aborts, but a narrow read-write race
  remains within the documented best-effort envelope.
- **Code-declared jobs are durable because code is the backup.**
  Reconciliation rewrites them from the declarations whenever the documents
  are missing. Runtime-added jobs and lifecycle flags are best-effort by
  declared policy; `pause()` additionally publishes a queue-borne control
  message so the flag reaches the process serving the chain even where cache
  state does not.

Under `vercel dev` the cache client falls back to per-process memory, so the
integration becomes a zero-infrastructure development mode with the
queue-serving sidecar as the effective scheduler process.

## Durable state

State is scoped by environment for production and custom environments, and by
deployment for previews and development. A named environment's schedules and
wake chain therefore survive promotions, while preview state stays disposable.

The driver keeps one cache document per scope and scheduler:

```text
state                running | paused | inactive
generation           monotonically increasing integer
owner_deployment     the deployment currently driving the chain
start_status         pending | published | processing | active
activation_time      when the current generation activated
current              the one current wake: sequence, logical time, status
last_sequence        dedup watermark that survives dormancy
dirty_logical_time   earliest candidate parked by a concurrent mutation
idle_expires_at      preview idle deadline, when enabled
```

Jobs live in a second document beside it, one record per job with a revision
counter and provenance tag; the takeover reconciliation marker shares the
jobs document so eviction clears them together. Documents are rewritten on
every touch and carry a long TTL, so only an abandoned namespace is reaped;
LRU eviction is survivable by design (see above).

Each persisted job records its provenance: `declared` for jobs materialized
from code declarations, `runtime` for jobs added through the mutation APIs
after `start()`. Code owns declared jobs across deployments; the store owns
runtime jobs.

## Starting

If the driver is already running, `start()` does not create a new generation.
Otherwise it:

1. increments `generation`;
2. sets the state to running;
3. marks the start message pending; and
4. clears the prior generation's current wake.

Concurrent callers converge on one live chain: racing writers publish under
per-generation idempotency keys, the queue accepts each key once, and a
newer generation permanently fences an older one at claim time.

The start delivery claims the driver. It starts APScheduler's internals
without starting a background scheduler thread and materializes code-declared
jobs into the store using insert-if-absent semantics, so a cold import cannot
overwrite a job changed through a runtime API. `finish_start` then marks the
start active and reserves sequence 1 — or, when no job is scheduled, leaves
the driver dormant with no current wake.

Automatic activation performs the same generation reservation, with one
important distinction: it never changes an explicitly paused driver.

## One wake

A wake carries:

```text
generation
sequence
logical time
```

Before doing work, its delivery must prove through `claim_wake` that the
driver is running, its generation and token are current, and no other live
processing attempt holds the claim. A token ahead of the local document is
adopted wholesale — the message is the authority on chain progress.

Only a claimant runs work. When it finishes, `finish_wake` verifies the claim
again and replaces the current token with exactly one successor:

```text
sequence = sequence + 1
current logical time = next wake
status = pending
```

The successor is the exact next due time. There are no periodic or idle
heartbeat wakes; once nothing remains scheduled, the chain goes dormant with
no current wake. The consumed position stays recorded in `last_sequence`, so
a redelivered old wake is stale even on a dormant chain.

If the handler reserves a successor and crashes before Queue publication, the
current token remains pending. A retry or stale delivery sees it and
republishes the exact same payload and key. The Queue idempotency key is the
single-successor fence; the document narrows duplicate executions.

A processing claim younger than fifteen minutes is treated as live and makes
duplicate deliveries retry later; past it, the claimant is presumed crashed
and the claim is retaken.

## Preview idle expiry

An opted-in preview stores `idle_expires_at` in the same driver document.
The first request starts the preview generation. Later requests renew the
deadline without changing the generation.

A manual `start()` renews the deadline in the same transition that starts the
generation. Without that renewal, a `start()` issued after the deadline
lapsed would create a generation whose own start message is already stale.
Outside an opted-in preview, activation clears any stored deadline.

Start and wake claims check the deadline before acquiring the claim.
Completion checks it again before reserving a successor. If it expired, the
transition sets the driver to `inactive`, clears the current token, and fails
closed:

- unclaimed Queue messages do no work;
- work already in flight may finish, but cannot extend the chain; and
- no periodic message keeps an abandoned preview alive.

The next real request changes `inactive` to `running`, increments the
generation, and publishes one start identity. Activation rebases persisted
jobs to that request time, so the inactive interval is skipped rather than
replayed as a burst.

`paused` and `inactive` are deliberately distinct. `paused` records an
explicit operator decision and automatic request activity only renews its
deadline; it does not resume the scheduler.

## Pausing and resuming

`pause()` changes the state to paused and publishes a queue-borne control
message carrying the flag to the process serving the chain. After pause
commits:

- an unclaimed start or wake is stale and does no work;
- an in-flight handler may finish its current work;
- its completion cannot reserve a successor.

Resuming reuses `start()`: it creates one new generation. Old messages remain
permanently stale because their generation no longer matches. Occurrences
that became due while paused are skipped, regardless of misfire settings: the
new generation rebases every job to its next occurrence at or after the
resume.

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

The integration coordinates the job write and wake rearm:

- while paused, only the job is changed;
- while running with no active claimant, an earlier or missing current wake
  is replaced with one new monotonic sequence;
- while a start or wake holds the driver, the mutation records its earliest
  candidate time and the claimant folds that value into its one successor.

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

Automatic activation establishes the boundary before a production request
reaches the application. Without automatic activation, every cold Function
instance that performs a runtime mutation must first call the idempotent
`scheduler.start()`. Before that boundary, `add_job()` calls are treated as
module-level declarations. Raw writes to the store's cache keys are
unsupported because they bypass wake rearming and revision checks.

The no-heartbeat design has one deliberate liveness contract: `start()` and
mutation calls are durable after they return successfully. If a process dies
after committing the store but before publishing Queue, an idempotent
`start()` republishes the pending token. Repeating an interrupted mutation is
also safe: a retried mutation republishes the pending wake even when the
retry itself fails, for example on a conflicting job id. A completely dormant
scheduler does not wake periodically to repair an otherwise unobserved
ambiguous failure. A delayed repair can pass a finite misfire window; jobs
that opt into one and must remain eligible after repairs should size it
accordingly.

## Execution requirements

Exactly one durable job store, the managed one named `default`, coordinates
the lifecycle. Job stores under other aliases are source stores (see below);
a third-party store named `default`, or the managed store type under a
non-default alias, is rejected at validation.

Jobs use the integration's inline executor so that a wake remains active until
the job has completed and the job-store update has happened. Custom
executors are rejected in v1. A scheduled function can enqueue longer work to
another queue.

Queue delivery cannot honor APScheduler's stock one-second misfire grace:
wake delays are rounded up to whole seconds and dispatch adds latency, so a
routine delivery lands about a second late and would skip its occurrence as a
misfire. On Vercel a job whose `misfire_grace_time` is not chosen explicitly
(on the job or in `job_defaults`) therefore defaults to `None`: an occurrence
runs when its wake arrives, however late, and the default `coalesce` collapses
any backlog into one run. Jobs that must not run late opt in with a finite
`misfire_grace_time`; explicit values below five seconds cannot be met by
queue transport and log a warning.

Code-declared jobs require explicit stable IDs. If a job with that ID is
already persisted, the declaration must permit replacement, but materializing
the declaration does not overwrite the persisted runtime value. The
`scheduled_job()` decorator enables replacement automatically; declaration
calls to `add_job()` should pass `replace_existing=True`.

## Source job stores

Any job store added under a non-default alias is a *source store*: a store
whose schedule an external system owns, typically a `BaseJobStore` subclass
that materializes its data into jobs. The integration reads source stores
but never manages them:

- Each wake reads `get_due_jobs()` and runs the due jobs inline with stock
  APScheduler semantics: after a run, a job with a further fire time is
  advanced through the store's `update_job()`, and a finished one is
  removed through `remove_job()`. A store may give `remove_job()` dispatch
  semantics — claim a row, enqueue real work elsewhere — and the
  integration calls it exactly where the stock scheduler loop would.
- Declarations, takeover reconciliation, revision fencing, and the paused
  interval rebase apply only to the durable store. Source entries that came
  due while paused therefore still dispatch on the next evaluation; skip
  semantics belong to the external system.
- `add_job()` and the other mutation APIs may only target `default`;
  targeting a source store raises a configuration error, and a mutation
  whose `jobstore` is unset is pinned to `default` instead of APScheduler's
  fall-through search across every store (a source store's `remove_job()`
  can carry dispatch semantics, so an app-level removal must never reach
  one).

A source store's schedule changes out of band — rows appear or move without
passing through scheduler APIs — and there is no polling to notice that.
Each wake arms the successor at the exact earliest next run time across the
durable store and every source store's `get_next_run_time()`, and a chain
with nothing scheduled goes dormant, source stores or not. An out-of-band
change is therefore picked up at the next chain-scheduled wake, or
immediately when the application signals it: after writing a row whose due
time is earlier than anything the chain knows about, call
`scheduler.wakeup()` from a web Function on the deployment that drives the
chain. The call recomputes the next due time across every store and pulls
the current wake in to it (never later), exactly like APScheduler's native
`wakeup()` recomputes the scheduler thread's wait.

A source store may also appear after a generation is already active — a
runtime `add_jobstore()`, or a process whose activation finds a running
generation created before the store existed. Both paths rearm the current
wake to the store's reported next due time, so a store present at boot
needs no application-level `wakeup()` call for its existing rows.

Delivery to source-store jobs is at least once, and the store owns its own
dedup. Claims are best-effort filters on evictable state, so duplicate wake
executions are possible. A store whose dispatch must not double should claim
its rows conditionally (for example an atomic `UPDATE ... WHERE locked_at IS
NULL`) before acting and treat zero claimed rows as already dispatched.

Store failures are bounded, not fatal: a failing `get_due_jobs()` or
`get_next_run_time()` retries on APScheduler's `jobstore_retry_interval`,
and a failed `update_job()`/`remove_job()` — or a job the wake had to skip,
for example one naming an unknown executor or one whose materialized form
does not load — leaves the entry due and floors that store's contribution
to the successor at the same retry interval, so a persistent failure
retries on a bounded cadence instead of spinning immediate wakes. The
store must still ensure *successfully* dispatched entries leave its due
view; one that keeps reporting an overdue next run time it never clears
will wake the chain immediately and continuously.

## Development under `vercel dev`

`vercel dev` runs each declared subscriber as a local queue-serving sidecar
against the CLI's in-process queue broker, which honors the same delays and
idempotency keys as the hosted queue. Without deployed cache credentials the
Runtime Cache client falls back to per-process memory, so development is a
zero-infrastructure mode with the sidecar as the effective scheduler process.

Activation follows the production rule: the first request to the app
registers and runs the activation hook, which publishes the durable start,
and the hook re-runs on the heal cadence while traffic continues. Declared
jobs therefore start ticking after the app is hit once per `vercel dev`
session. `vercel dev` deliberately sets no
`VERCEL_DEPLOYMENT_ID` (SDKs read its presence as "deployed"), so the
integration derives a stable synthetic id from the project directory every
dev process is spawned in. It is shared by the web process and every
sidecar, names the development state scope, and keeps two projects apart.

Identity is builder-assigned in every mode: sidecars receive their
subscriber id directly, and publishing processes (web functions) resolve it
from the declared `{id, entrypoint}` mapping, so a `pause()` from a request
handler reaches the same topics the sidecar serves.

## Deployment behavior

Production and custom environments share one durable namespace, owned by
exactly one deployment at a time. Queue delivery routing is advisory and may
change underneath this design: today a message is delivered to the deployment
that sent it, promoted or not; when the platform enables queue aliasing for a
project, in-flight messages are instead handed to the environment's current
deployment. Ownership therefore decides who may act, never who received a
delivery: `start()` on a non-owner is a takeover that transfers ownership and
opens a new generation in one step, and a promoted deployment takes
over automatically on its first request that arrives through an environment
alias. From that moment the demoted deployment is fenced out of the
namespace: its deliveries still arrive but every claim turns stale
and acks, it repairs and rearms nothing, its cold starts write no
declarations, its mutation APIs refuse loudly, and its job-store writes are
refused. Its queue simply drains.
A rollback is the same operation in the other direction; the mechanism has
no notion of old and new, only of who owns the chain now. An opted-in
preview activates the same way and remains active only while requests renew
its deadline.

Automatic takeover is traffic-driven: it happens on the first request the
promoted deployment serves through an environment alias, regardless of what
arrived before it. Requests through the deployment's own URL neither take
over nor delay it: while another deployment owns the chain, the activation
hook stays eligible on every invocation and touches the store only for an
alias-routed request or a lapsed sweep interval. After promoting or
rolling back a deployment that receives no organic traffic, send one request
through the environment's domain (or schedule a cron heartbeat) so the chain
hands over promptly. Alias routing is judged by the request host, so do not
point a manually created alias at an old deployment of a scheduler project:
requests through that alias would let the old deployment take the chain.

On takeover the new owner reconciles the
store against its own declarations, before planning any due jobs: a job the
code no longer declares is deleted and never runs, a changed trigger restarts
its schedule, an unchanged job keeps its progress, and `runtime` jobs are
never touched. A declared job whose persisted record no longer loads under
the new code (typically because its function moved) is rewritten from the
declaration and restarts its schedule. A `runtime` job whose definition no
longer loads is quarantined: it leaves the due index, keeps its record for
the operator, and logs an error.

Reconciliation completes only once it converges. A revision race with a
concurrent owner write reruns the pass against fresh state, and only the
owner marks the sync as done, after a clean pass; a reconciliation that
cannot converge stays unmarked and retries on the next activation. In-flight
work is never interrupted: the demoted deployment's running job finishes or
dies with its instance and its late writes are fenced best-effort.
Jobs that run long should enqueue their work to another queue and return, so
a promote is never delayed behind them.

A takeover strands the previous owner's in-flight wake: it is consumed by
the demoted deployment and acked as stale, and the new owner's chain starts
from its own activation. Independently, a `published` or `processing` wake
well past its logical time with no live claimant is presumed lost and
republished by the owner into its own queue. Within a queue the idempotency
key makes a false-positive republish a no-op; across queues the claim fences
duplicates. Repairs run from `start()`, from stale queue deliveries, and log
a warning when they fire, since each one means a message actually died.

Previews and development keep deployment-scoped namespaces: a preview chain
dies with its deployment instead of following the branch.

Deleting a deployment prevents its Functions from receiving further work.

## Race outcomes

| Race | Outcome |
| --- | --- |
| many concurrent `start()` calls while paused | one live chain; newer generations fence older ones |
| many `start()` calls while running | no lifecycle change |
| duplicate delivery of the same wake | one live processing claim wins; duplicates retry and turn stale |
| `pause()` before a wake claim | wake is stale |
| `pause()` while a wake runs | current work may finish; no successor |
| resume while an old wake runs | the old generation cannot reserve a successor |
| crash before Queue send | pending token is republished |
| old message after resume | generation check makes it stale |
| concurrent runtime add/modify | one token is rearmed; no second chain |
| handler finishes after a job mutation | revision check preserves the mutation |
| retried mutation fails on a conflicting id | the pending wake is still republished |
| takeover while a wake is in flight | the demoted deployment consumes it and acks it as stale |
| the owner's wake message dies | the overdue wake is presumed lost and republished by the owner |
| takeover reconciliation races a demoted deployment's handler | the demoted write aborts on the ownership fence |
| reconciliation loses a revision race to a concurrent owner write | the pass reruns with fresh state; completion is marked only once converged |
| a deployment loses the namespace mid-reconciliation | it cannot stamp the marker, and the owner reconciles |
| concurrent first requests | one automatic generation and one start identity |
| request arrives after explicit pause | idle deadline renews but state remains paused |
| preview idle deadline expires before claim | message is stale and no job runs |
| preview idle deadline expires while work runs | current work may finish; no successor |
| request arrives after preview expiry | one new generation; inactive time is skipped |
| `start()` after the preview deadline lapsed | deadline renews; the new generation is claimable |

These properties prevent persistent parallel chains. They do not provide
exactly-once job side effects: claims are best-effort filters on an evictable
document, so duplicate executions are possible, and Queue redelivery can
execute a job again after a crash. Jobs must be idempotent.
