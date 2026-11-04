# vercel-workflow

Durable workflows for Python: workflow bodies are replayed deterministically from an event log, while steps, sleeps, and hooks are recorded events.

Lives in [[src/vercel-workflow]] and is exposed as [[src/vercel-workflow/vercel/workflow/__init__.py|vercel.workflow]]. It speaks the same wire protocol as the TypeScript `@workflow/core`, so runs are visible in the same dashboard and CLI and can be read from either SDK.

## Registry and entry points

A `Workflows` registry owns the workflows and steps it serves and exposes the HTTP handler Vercel invokes for its queue topic.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#Workflows]] registers async workflow bodies and steps, and can isolate its messages under a queue `namespace`. Steps can only be called from inside a workflow. [[src/vercel-workflow/vercel/workflow/_internal/run.py#start]] creates a run and returns a [[src/vercel-workflow/vercel/workflow/_internal/run.py#Run]] handle for status, return value, attributes, and streams. `python -m vercel.workflow` emits a manifest of registered workflows for external tooling ([[src/vercel-workflow/vercel/workflow/_internal/cli.py]]).

## Execution model

Each queue delivery either executes one step or replays the workflow body against the run's recorded events until it next suspends.

[[src/vercel-workflow/vercel/workflow/_internal/runtime.py#workflow_handler]] distinguishes the two. A replay pass runs the body from the start, resolving each step call, sleep, or hook wait from the event log when its result is already recorded. When the body reaches an operation with no result yet, it suspends. The runtime then records the pending operations, enqueues step executions, and schedules wake-ups. Sleeps longer than about 23 hours are chained as delayed re-enqueues, deduplicated on the wait's correlation ID.

Correlation IDs are assigned by position, so the body must issue the same operations in the same order on every replay. Divergence raises [[src/vercel-workflow/vercel/workflow/_internal/runtime.py#NondeterminismError]] and fails the run with `REPLAY_DIVERGENCE`, rather than attaching recorded results to the wrong calls.

Step failures retry up to `max_retries`. [[src/vercel-workflow/vercel/workflow/_internal/errors.py#FatalError]] fails the step immediately, and [[src/vercel-workflow/vercel/workflow/_internal/errors.py#RetryableError]] retries after a chosen delay.

## Deterministic sandbox

Workflow bodies run in an isolated event loop inside a module sandbox that blocks sources of nondeterminism.

[[src/vercel-workflow/vercel/workflow/_internal/loop.py#WorkflowLoop]] is a custom asyncio loop with no real I/O; its clock and timers come from the replay. The body runs in a fresh loop with the host loop hidden. [[src/vercel-workflow/vercel/workflow/_internal/py_sandbox.py#Sandbox]] gives each run its own `sys.modules` table, so user modules are re-imported per run and runs cannot share module state. It also proxies modules such as `datetime` and `random` to enforce restrictions. Code outside a workflow context still sees the real modules.

[[src/vercel-workflow/vercel/workflow/_internal/py_sandbox.py#SandboxPolicy]], exposed through [[src/vercel-workflow/vercel/workflow/sandbox.py|vercel.workflow.sandbox]], allows passthrough modules served from the host and sandbox reuse across runs. It also accepts cleanup handlers that purge host caches which would otherwise pin a run's module graph. Passthrough modules are not checked for nondeterminism.

## Worlds

A World is the storage and queueing backend for runs, chosen once per process.

[[src/vercel-workflow/vercel/workflow/_internal/world.py#World]] defines the contract: run and step entities, events, hooks, streams, and queue sends. [[src/vercel-workflow/vercel/workflow/_internal/world.py#create_world]] selects it from `WORKFLOW_TARGET_WORLD`, defaulting to the Vercel world when `VERCEL_DEPLOYMENT_ID` is set and the local world otherwise.

- [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld]] talks to the hosted Workflow service and sends through [[vercel-queue]].
- [[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld]] writes JSON files that the `workflow` CLI can inspect, and delivers through the embedded queue from [[vercel-queue#Local emulation]]. Event and step files live one directory per run (`events/<runId>/`, `steps/<runId>/`), matching `@workflow/world-local`. On startup [[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#_wipe_flat_layout]] deletes a data directory holding only the old flat layout and raises [[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#DataDirLayoutError]] for mixed or unknown contents.

## Payloads and encryption

Inputs, results, hook payloads, and stream chunks use `@workflow/core`'s encoding, so data round-trips between the Python and TypeScript SDKs.

Values are encoded with a port of devalue ([[src/vercel-workflow/vercel/workflow/_internal/devalue]]), which carries `datetime`, `bytes`, `set`, and repeated references. [[src/vercel-workflow/vercel/workflow/_internal/serde.py#serializable]] registers custom classes. Pydantic models and dataclasses are validated against the signatures of steps and workflows by [[src/vercel-workflow/vercel/workflow/_internal/signature_codec.py#SignatureCodec]]; a mismatch raises a fatal `TypeValidationError`.

Payloads may be compressed ([[src/vercel-workflow/vercel/workflow/_internal/compression.py]]) and encrypted with a per-run key derived from the deployment key ([[src/vercel-workflow/vercel/workflow/_internal/encryption.py#derive_run_key]]). Run attributes are deliberately plaintext and size-limited ([[src/vercel-workflow/vercel/workflow/_internal/attributes.py]]).

CI checks these encodings against the real JavaScript implementations: the devalue interop tests run the published package, and local-world tests run the `workflow` CLI against files written by Python.

## Hooks and streams

Hooks pause a run until an external caller resumes it, and streams let steps publish incremental output that clients read while the run is in progress.

A [[src/vercel-workflow/vercel/workflow/_internal/core.py#BaseHook]] subclass waits under a generated or caller-chosen token, and resuming it records the payload as an event. Hook metadata is written once at registration and is not part of the payload.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#BaseHook#wait]] accepts `experimental_min_retention`, resolved against the workflow's deterministic clock. The recorded deadline is reused on replay and exposed on the public [[src/vercel-workflow/vercel/workflow/_internal/core.py#Hook]] as `token_retention_until`. A terminal run keeps the token reserved until that deadline; explicit disposal releases it immediately. A retained terminal hook cannot receive further payloads.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld]] enforces a default 30-day retention limit, configurable with `WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS`. It releases expired claims only after their runs become terminal, serializes token handoffs across processes, and protects successor claims from delayed cleanup.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld#_events_create_impl]] checks retention limits in hook creation after run-state validation and before changing token claims. Committed resume retries remain idempotent on terminal runs; new payloads are rejected before a resume claim is written.

Retention tests follow the existing hook test boundaries: [[src/vercel-workflow/tests/unit/test_workflow_hook_registration.py]] covers deadline calculation and replay; [[src/vercel-workflow/tests/unit/test_workflow_hook_conflict.py]] covers reserved tokens, expiry, limits, and competing claims; [[src/vercel-workflow/tests/unit/test_workflow_hook_disposal.py]] covers release and terminal cleanup; [[src/vercel-workflow/tests/unit/test_workflow_hook_created_models.py]] covers wire encoding.

A run's stream ([[src/vercel-workflow/vercel/workflow/_internal/streams.py]]) is an append-only, indexed log of chunks. Only steps can write to it, and nothing closes it automatically. A step is not recorded as complete until its chunks are flushed, and a retried step writes its chunks again. Readers resume exactly from a chunk index and reconnect when the server ends a long read.

## Local file leases

Local filesystem writers can share directory leases with TypeScript's proper-lockfile library without requiring the protected file to exist.

[[src/vercel-workflow/vercel/workflow/_internal/file_lock.py#file_lock]] implements the `realpath: false` protocol: atomic creation of a sibling `.lock` directory, millisecond-aware heartbeat timestamps, bounded retries, and stale recovery. A stale observation is revalidated against directory identity and mtime before removal.

[[src/vercel-workflow/vercel/workflow/_internal/file_lock.py#FileLock#check]] detects a missing or replaced directory or a changed heartbeat marker. Ownership loss cancels the protected async body and raises [[src/vercel-workflow/vercel/workflow/_internal/file_lock.py#FileLockCompromisedError]]; release never intentionally removes a successor's lease. Participants must agree on the stale threshold and allow heartbeat tasks to run. As with proper-lockfile, this cooperative lease cannot fence a paused writer's filesystem operations atomically.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld#_hook_token_lock]] applies these leases to hook token creation, disposal, and terminal cleanup with TypeScript world-local's 30-second stale threshold, one-second heartbeat, and 35-second acquisition timeout. It translates lease failures into world errors. Async token locks are acquired before the synchronous per-run mutex; cleanup awaits happen after that mutex is released.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld#_delete_hook]] removes a token claim only when its run and hook IDs still match the captured hook. Permanent disposal, wait, and attribute claim markers retain their exclusive-create semantics. [[src/vercel-workflow/tests/unit/test_workflow_local_hook_locking.py]] covers lease integration, mutation serialization, and successor-claim protection independently of retention.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld#_hook_token_claim_releasable]] lets a new claimant recover a token whose owner run is terminal and retention has ended, or whose hook has a durable disposal marker, even if cleanup was interrupted. Recovery holds the token lease; a missing owner run alone does not release a claim. Disposal rejects an initially missing hook rather than deleting a concurrently published hook without its token lock. Regression tests cover cancellation, timeout, committed disposal debris, and publication after the initial missing read.

[[src/vercel-workflow/tests/unit/test_workflow_file_lock.py]] covers contention, cancellation, stale recovery, changed observations, heartbeat renewal, and ownership loss. [[src/vercel-workflow/tests/integration/test_file_lock_interop.py]] runs the pinned proper-lockfile npm package to verify mutual exclusion, heartbeat compatibility, release, and recovery after either runtime crashes.

The interop fixture resolves npm's executable path before installing the pinned package so Windows can launch its command shim as well as Unix executables. Its subprocess readiness handshake accepts both Unix and Windows line endings.
