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

[[src/vercel-workflow/vercel/workflow/_internal/compression.py#CompressionMode]] explicitly selects `none` to skip compression or `auto` to apply the existing codec, size, savings, and environment policy. [[src/vercel-workflow/vercel/workflow/_internal/serialization.py#PayloadEncoder]] carries this mode; boolean workflow options are converted when constructing the encoder.

CI checks these encodings against the real JavaScript implementations: the devalue interop tests run the published package, and local-world tests run the `workflow` CLI against files written by Python.

## Hooks and streams

Hooks pause a run until an external caller resumes it, and streams let steps publish incremental output that clients read while the run is in progress.

A `BaseHook` subclass waits under a generated or caller-chosen token, and resuming it records the payload as an event. Hook metadata is written once at registration and is not part of the payload.

A run's stream ([[src/vercel-workflow/vercel/workflow/_internal/streams.py]]) is an append-only, indexed log of chunks. Only steps can write to it, and nothing closes it automatically. A step is not recorded as complete until its chunks are flushed, and a retried step writes its chunks again. Readers resume exactly from a chunk index and reconnect when the server ends a long read.

## Configured workflows

Workflow references carry immutable, reusable startup configuration while startup remains an ordinary coroutine.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#_ConfigurableWorkflow#without_argument_compression]] is inherited by both [[src/vercel-workflow/vercel/workflow/_internal/core.py#Workflow]] and [[src/vercel-workflow/vercel/workflow/_internal/core.py#ConfiguredWorkflow]]. Each provides its immutable configuration through the base class contract; the shared method returns a modified copy without changing either reference. Workflow parameter and result types are preserved through configuration and [[src/vercel-workflow/vercel/workflow/_internal/run.py#start]], which accepts either reference type and remains a native async function. Binding and execution retain their existing deferred behavior.

[[src/vercel-workflow/vercel/workflow/_internal/run.py#start]] normalizes configuration and binds arguments before preparing a run. [[src/vercel-workflow/vercel/workflow/_internal/run.py#_RunStart#prepare]] retains the immutable workflow configuration alongside the encoded input, execution context, run identity, and resolved queue name; [[src/vercel-workflow/vercel/workflow/_internal/run.py#_RunStart#submit]] assembles the event and queue payload separately from those fields, creating the run before enqueueing it and returning a typed run handle with the workflow result validator. Preparation writes no durable state, and creation failure prevents queue delivery.

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
