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

[[src/vercel-workflow/vercel/workflow/_internal/compression.py#CompressionMode]] explicitly selects `none` to skip compression, `auto` to apply the existing codec policy, or `gzip` to restrict compression to that codec. Size, savings, and environment-disable gates still apply. [[src/vercel-workflow/vercel/workflow/_internal/serialization.py#PayloadEncoder]] carries this mode; boolean workflow options are converted when constructing the encoder.

CI checks these encodings against the real JavaScript implementations: the devalue interop tests run the published package, and local-world tests run the `workflow` CLI against files written by Python.

## Hooks and streams

Hooks pause a run until an external caller resumes it, and streams let steps publish incremental output that clients read while the run is in progress.

A [[src/vercel-workflow/vercel/workflow/_internal/core.py#BaseHook]] subclass waits under a generated or caller-chosen token, and resuming it records the payload as an event. Hook metadata is written once at registration and is not part of the payload.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#BaseHook#wait]] accepts `experimental_min_retention`, resolved against the workflow's deterministic clock. The recorded deadline is reused on replay and exposed on the public [[src/vercel-workflow/vercel/workflow/_internal/core.py#Hook]] as `token_retention_until`. A terminal run keeps the token reserved until that deadline; explicit disposal releases it immediately. A retained terminal hook cannot receive further payloads.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld]] enforces a default 30-day retention limit, configurable with `WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS`. It releases expired claims only after their runs become terminal, serializes token handoffs across processes, and protects successor claims from delayed cleanup.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld#_events_create_impl]] checks retention limits in hook creation after run-state validation and before changing token claims. Committed resume retries remain idempotent on terminal runs; new payloads are rejected before a resume claim is written.

Retention tests follow the existing hook test boundaries: [[src/vercel-workflow/tests/unit/test_workflow_hook_registration.py]] covers deadline calculation and replay; [[src/vercel-workflow/tests/unit/test_workflow_hook_conflict.py]] covers reserved tokens, expiry, limits, and competing claims; [[src/vercel-workflow/tests/unit/test_workflow_hook_disposal.py]] covers release and terminal cleanup; [[src/vercel-workflow/tests/unit/test_workflow_hook_created_models.py]] covers wire encoding.

A run's stream ([[src/vercel-workflow/vercel/workflow/_internal/streams.py]]) is an append-only, indexed log of chunks. Only steps can write to it, and nothing closes it automatically. A step is not recorded as complete until its chunks are flushed, and a retried step writes its chunks again. Readers resume exactly from a chunk index and reconnect when the server ends a long read.

## Configured workflows

Workflow references carry immutable, reusable startup configuration while startup remains an ordinary coroutine.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#_ConfigurableWorkflow#without_argument_compression]] is inherited by both [[src/vercel-workflow/vercel/workflow/_internal/core.py#Workflow]] and [[src/vercel-workflow/vercel/workflow/_internal/core.py#ConfiguredWorkflow]]. Each provides its immutable configuration through the base class contract; the shared method returns a modified copy without changing either reference. Workflow parameter and result types are preserved through configuration and [[src/vercel-workflow/vercel/workflow/_internal/run.py#start]], which accepts either reference type and remains a native async function. Binding and execution retain their existing deferred behavior.

[[src/vercel-workflow/vercel/workflow/_internal/run.py#start]] normalizes configuration and binds arguments before preparing a run. [[src/vercel-workflow/vercel/workflow/_internal/run.py#_RunStart#prepare]] retains the immutable workflow configuration alongside the encoded input, execution context, run identity, and resolved queue name; [[src/vercel-workflow/vercel/workflow/_internal/run.py#_RunStart#submit]] assembles the event and queue payload separately from those fields, creating the run before enqueueing it and returning a typed run handle with the workflow result validator. Preparation creates no workflow run, though cross-deployment capability probes use the queue and a synthetic response stream. Run creation failure prevents invocation delivery.

The compression option only controls the argument encoder during startup. The opt-out skips compression; unconfigured workflows retain [[src/vercel-workflow/vercel/workflow/_internal/compression.py#maybe_compress|the existing size, savings, codec, and environment policy]]. The encoded bytes are shared by run creation and queue delivery. Other payload encoders are unchanged.

[[src/vercel-workflow/tests/unit/test_workflow_run.py]] verifies immutable configuration branches, reuse across independent starts, native coroutine scheduling, argument and result typing, and the separation of startup options from identically named workflow arguments. It checks actual payload envelopes for default workflow behavior, default configuration, and compression opt-out, including environment opt-out and the small-payload threshold.

## Regional routing

Reusable workflow configuration pins run data and queue dispatch to a region.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#_ConfigurableWorkflow#with_region]] returns an immutable configuration and preserves argument compression settings. [[src/vercel-workflow/vercel/workflow/_internal/run.py#start]] passes the region hint to run-ID creation and queue delivery.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_create_run_id]] embeds TypeScript-compatible region metadata in Vercel run IDs and preserves monotonic ordering across region changes. Region selection prefers a recognized explicit hint, then the environment, then iad1. [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_region_from_tagged_run_id]] keeps later queue deliveries in the run’s region when no valid override is present. Queue clients are isolated by region. Local worlds ignore region hints and keep ordinary ULIDs.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_RunIdState]] keeps the generator and last emitted ID in thread-local state, preserving monotonicity across region changes within each thread without a shared mutex. Cross-thread ordering is not guaranteed.

When correcting ordering, [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_create_run_id]] advances the last emitted ID's timestamp and preserves its low random bits to match TypeScript. [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_region_from_tagged_run_id]] accepts ASCII Base32 in either case and rejects non-ASCII characters before case normalization.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld#_queue_client]] sends the selected region in the `x-vercel-queue-region` header when using the fixed queue proxy URL. Direct clients use regional hosts instead. Each client receives its own headers so regional routing does not mutate shared World headers.

The region option controls data and queue placement. Functions follow the application’s configured compute regions; if the requested region has no application functions, steps execute in the nearest configured compute region.

[[src/vercel-workflow/tests/unit/test_workflow_queue_transport.py]] checks TypeScript-generated region vectors, fallback, monotonicity, and queue region preservation. [[src/vercel-workflow/tests/unit/test_workflow_run.py]] checks immutable option composition, reuse, namespace preservation, and region hint propagation.

## Deployment targeting

Configured workflows can start on an explicit deployment or resolve the latest deployment in the current deployment's environment, negotiating the target's wire capabilities first.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#_ConfigurableWorkflow#with_deployment_id]] composes immutably with region and argument compression configuration. [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld#resolve_latest_deployment_id]] resolves the latest deployment through the Vercel API, preferring deployment OIDC identity inside Vercel and scoping requests to the configured team. Other worlds default to their current deployment and warn once that latest is a no-op. Explicit targets may be used without a current deployment identity; default and latest starts require one.

Latest-deployment credential resolution attempts OIDC at most once. [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld#resolve_latest_deployment_id]] falls back on expected OIDC token errors and propagates unexpected exceptions.

[[src/vercel-workflow/vercel/workflow/_internal/deployment.py#_probe]] sends a JSON health message with a probeRunId and reads the synthetic health response stream. [[src/vercel-workflow/vercel/workflow/_internal/world.py#HealthCheckPayload]] names the field `probe_run_id` internally, emits `probeRunId`, and accepts both `probeRunId` and the legacy `runId` on the wire. Its per-World, deployment-and-namespace cache keeps answers for ten minutes without reusing run public keys, bounds entries to 256, and reduces the ten-second probe budget to two seconds for one minute after a miss.

[[src/vercel-workflow/vercel/workflow/_internal/deployment.py#resolve]] caps the target's spec at the caller's supported write version, stamps only attested target runtime markers, and gates argument compression on both spec and target core/runtime versions. JSON responses without valid versions select spec 3, old text responses select spec 2, and missed probes use the TypeScript slot-identity fallback capped to this caller's spec. A probed public key seals arguments without fetching a symmetric key; cached answers use the regular run-key lookup. [[src/vercel-workflow/vercel/workflow/_internal/deployment.py#Target#encoder]] selects the compatible compression and encryption envelope. Explicitly reported spec 1 targets are rejected before run creation because the Python writer supports event-sourced runs.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld#queue]] uses JSON for probes and pre-CBOR targets, omitting inline run input for those targets. [[src/vercel-workflow/tests/unit/test_workflow_run.py]] covers negotiation cutoffs, sealed startup, and deployment configuration. [[src/vercel-workflow/tests/unit/test_workflow_health_check.py]] checks probe caching and retry budgets; [[src/vercel-workflow/tests/unit/test_workflow_queue_transport.py]] checks JSON probe transport, preserved legacy invocation fields, latest-deployment credential priority, and API failures.

## Start attributes

Configured workflows can seed plaintext run attributes at creation, using the same validation and wire fields as the TypeScript SDK.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#_ConfigurableWorkflow#with_attributes]] accepts an optional positional mapping and keyword attributes, with keywords winning on duplicate keys, matching [[src/vercel-workflow/vercel/workflow/_internal/runtime.py#set_attributes]]. It replaces the configured attribute map with a read-only copy and optionally permits reserved keys. It composes with deployment, region, and compression configuration without changing workflow argument or result types.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#_ConfigurableWorkflow#with_attributes]] rejects non-string values and applies [[src/vercel-workflow/vercel/workflow/_internal/attributes.py#validate_attribute_changes|shared attribute validation]] immediately. [[src/vercel-workflow/vercel/workflow/_internal/run.py#_RunStart#prepare]] requires target spec version 4 or later for nonempty attributes after deployment negotiation. Empty maps behave like omitted attributes, including on older targets.

[[src/vercel-workflow/vercel/workflow/_internal/run.py#_RunStart#submit]] includes the attributes and reserved-key opt-in in both the creation event and resilient startup input so recovery preserves them. [[src/vercel-workflow/tests/unit/test_workflow_run.py#test_start_attributes_seed_both_payloads]] checks both wire payloads and reusable configuration; [[src/vercel-workflow/tests/unit/test_workflow_run.py#test_invalid_start_attributes_prevent_writes]] checks validation, [[src/vercel-workflow/tests/unit/test_workflow_run.py#test_start_attributes_use_target_spec]] covers negotiated versions, and [[src/vercel-workflow/tests/unit/test_workflow_run.py#test_start_attributes_persist_locally]] verifies stored attributes through the public run handle.
## Local file leases

Local filesystem writers can share directory leases with TypeScript's proper-lockfile library without requiring the protected file to exist.

[[src/vercel-workflow/vercel/workflow/_internal/file_lock.py#file_lock]] implements the `realpath: false` protocol: atomic creation of a sibling `.lock` directory, millisecond-aware heartbeat timestamps, bounded retries, and stale recovery. A stale observation is revalidated against directory identity and mtime before removal.

[[src/vercel-workflow/vercel/workflow/_internal/file_lock.py#FileLock#check]] detects a missing or replaced directory or a changed heartbeat marker. Ownership loss cancels the protected async body and raises [[src/vercel-workflow/vercel/workflow/_internal/file_lock.py#FileLockCompromisedError]]; release never intentionally removes a successor's lease. Participants must agree on the stale threshold and allow heartbeat tasks to run. As with proper-lockfile, this cooperative lease cannot fence a paused writer's filesystem operations atomically.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld#_hook_token_lock]] applies these leases to hook token creation, disposal, and terminal cleanup with TypeScript world-local's 30-second stale threshold, one-second heartbeat, and 35-second acquisition timeout. It translates lease failures into world errors. Async token locks are acquired before the synchronous per-run mutex; cleanup awaits happen after that mutex is released.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld#_delete_hook]] removes a token claim only when its run and hook IDs still match the captured hook. Permanent disposal, wait, and attribute claim markers retain their exclusive-create semantics. [[src/vercel-workflow/tests/unit/test_workflow_local_hook_locking.py]] covers lease integration, mutation serialization, and successor-claim protection independently of retention.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/local.py#LocalWorld#_hook_token_claim_releasable]] lets a new claimant recover a token whose owner run is terminal and retention has ended, or whose hook has a durable disposal marker, even if cleanup was interrupted. Recovery holds the token lease; a missing owner run alone does not release a claim. Disposal rejects an initially missing hook rather than deleting a concurrently published hook without its token lock. Regression tests cover cancellation, timeout, committed disposal debris, and publication after the initial missing read.

[[src/vercel-workflow/tests/unit/test_workflow_file_lock.py]] covers contention, cancellation, stale recovery, changed observations, heartbeat renewal, and ownership loss. [[src/vercel-workflow/tests/integration/test_file_lock_interop.py]] runs the pinned proper-lockfile npm package to verify mutual exclusion, heartbeat compatibility, release, and recovery after either runtime crashes.

The interop fixture resolves npm's executable path before installing the pinned package so Windows can launch its command shim as well as Unix executables. Its subprocess readiness handshake accepts both Unix and Windows line endings.
