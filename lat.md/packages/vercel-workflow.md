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

### HTTP request retries

The Vercel world retries safe Workflow API requests after transport failures so a brief network interruption can recover within the current call.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_send_with_transport_retries]] makes at most three attempts, waiting between retries with exponential backoff and jitter. [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_is_retryable_transport_error]] allows GET, HEAD, and OPTIONS requests to retry after any transport error. Other methods retry only connection errors, connection timeouts, or pool timeouts, which occur before the request is sent. An uncertain write is left to the caller because repeating a step start can increment its attempt counter twice. HTTP error responses are not retried.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld#_cbor_request]] owns the HTTP client lifetime and passes one client to the retry helper, so all attempts within a call share its connection pool.

[[src/vercel-workflow/tests/unit/test_workflow_queue_transport.py#test_step_start_retries_before_the_request_is_sent|HTTP transport tests]] cover client reuse, safe write retries, uncertain writes, read retries, the attempt limit, and HTTP error responses.

### Queue handler failures

Failed Vercel workflow deliveries request a retry after a short delay instead of waiting for the default five-minute processing lease to expire.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld#create_queue_handler]] turns handler exceptions into the queue SDK's `RetryAfter` directive. Explicit queue directives pass through unchanged. [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_handler_error_retry_after_seconds]] starts at one second and increases the delay with the delivery count, with jitter and a 900-second cap. Large delivery counts select the capped delay before exponentiation, so the calculation cannot allocate an unbounded integer. A positive server retry delay sets a lower bound, also capped at 900 seconds. The queue SDK retries the visibility update; if those attempts fail, the existing lease remains the fallback.

[[src/vercel-workflow/tests/unit/test_workflow_queue_transport.py#test_failed_delivery_requests_a_short_retry|Queue delivery tests]] cover short failure retries, explicit directive passthrough, bounded jitter, and server retry delays. [[src/vercel-workflow/tests/unit/test_workflow_queue_transport.py#test_redelivery_backoff_caps_large_counts_before_exponentiation]] checks the cap boundary and extremely large counts.

## Payloads and encryption

Inputs, results, hook payloads, and stream chunks use `@workflow/core`'s encoding, so data round-trips between the Python and TypeScript SDKs.

Values are encoded with a port of devalue ([[src/vercel-workflow/vercel/workflow/_internal/devalue]]), which carries `datetime`, `bytes`, `set`, and repeated references. [[src/vercel-workflow/vercel/workflow/_internal/serde.py#serializable]] registers custom classes. Pydantic models and dataclasses are validated against the signatures of steps and workflows by [[src/vercel-workflow/vercel/workflow/_internal/signature_codec.py#SignatureCodec]]; a mismatch raises a fatal `TypeValidationError`.

Payloads may be compressed ([[src/vercel-workflow/vercel/workflow/_internal/compression.py]]) and encrypted with a per-run key derived from the deployment key ([[src/vercel-workflow/vercel/workflow/_internal/encryption.py#derive_run_key]]). Run attributes are deliberately plaintext and size-limited ([[src/vercel-workflow/vercel/workflow/_internal/attributes.py]]).

[[src/vercel-workflow/vercel/workflow/_internal/compression.py#CompressionMode]] explicitly selects `none` to skip compression, `auto` to apply the existing codec policy, or `gzip` to restrict compression to that codec. Size, savings, and environment-disable gates still apply. [[src/vercel-workflow/vercel/workflow/_internal/serialization.py#PayloadEncoder]] carries this mode; boolean workflow options are converted when constructing the encoder.

CI checks these encodings against the real JavaScript implementations: the devalue interop tests run the published package, and local-world tests run the `workflow` CLI against files written by Python.

## Hooks and streams

Hooks pause a run until an external caller resumes it, and streams let steps publish incremental output that clients read while the run is in progress.

A `BaseHook` subclass waits under a generated or caller-chosen token, and resuming it records the payload as an event. Hook metadata is written once at registration and is not part of the payload.

A run's stream ([[src/vercel-workflow/vercel/workflow/_internal/streams.py]]) is an append-only, indexed log of chunks. Only steps can write to it, and nothing closes it automatically. A step is not recorded as complete until its chunks are flushed, and a retried step writes its chunks again. Readers resume exactly from a chunk index and reconnect when the server ends a long read.

## Configured workflows

Workflow references carry immutable, reusable startup configuration while startup remains an ordinary coroutine.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#Configuration]] stores immutable startup options independently of any workflow. Its fields are keyword-only so adding options does not introduce positional ordering contracts. [[src/vercel-workflow/vercel/workflow/_internal/core.py#Workflow]] returns default options through [[src/vercel-workflow/vercel/workflow/_internal/core.py#Workflow#get_config]], while [[src/vercel-workflow/vercel/workflow/_internal/core.py#ConfiguredWorkflow#get_config]] returns the stored configuration. Their shared [[src/vercel-workflow/vercel/workflow/_internal/core.py#_Configurable#configure]] contract replaces the entire configuration, allowing options to be copied between workflows without changing either reference. On a configuration object, it returns the supplied replacement.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#_Configurable#without_argument_compression]] is inherited by configuration objects and both workflow reference types. It returns a modified copy with the same result type as their configuration contract. Workflow parameter and result types are preserved through configuration and [[src/vercel-workflow/vercel/workflow/_internal/run.py#start]], which reads options through the shared contract, accepts either reference type, and remains a native async function. Binding and execution retain their existing deferred behavior.

[[src/vercel-workflow/vercel/workflow/_internal/run.py#start]] extracts the workflow and configuration and binds arguments before preparing a run. [[src/vercel-workflow/vercel/workflow/_internal/run.py#_RunStart#prepare]] retains the workflow definition and immutable startup options as separate fields alongside the encoded input, execution context, run identity, and resolved queue name; [[src/vercel-workflow/vercel/workflow/_internal/run.py#_RunStart#submit]] assembles the event and queue payload separately from those fields, creating the run before enqueueing it and returning a typed run handle with the workflow result validator. Preparation creates no workflow run, though cross-deployment capability probes use the queue and a synthetic response stream. Run creation failure prevents invocation delivery.

The compression option only controls the argument encoder during startup. The opt-out skips compression; unconfigured workflows retain [[src/vercel-workflow/vercel/workflow/_internal/compression.py#maybe_compress|the existing size, savings, codec, and environment policy]]. The encoded bytes are shared by run creation and queue delivery. Other payload encoders are unchanged.

[[src/vercel-workflow/tests/unit/test_workflow_run.py]] verifies immutable configuration branches, reuse across independent starts, native coroutine scheduling, argument and result typing, and the separation of startup options from identically named workflow arguments. [[src/vercel-workflow/tests/unit/test_workflow_run.py#test_configuration_can_be_copied_and_replaced_between_workflows]] checks that a copied configuration applies to the receiving workflow and can be replaced without modifying the source. The tests check actual payload envelopes for default workflow behavior, default configuration, and compression opt-out, including environment opt-out and the small-payload threshold.

## ULID utilities

Workflow identifiers share a Crockford Base32 codec and monotonic generator.

[[src/vercel-workflow/vercel/workflow/_internal/ulid.py#TIME_FIELD_BITS|The timestamp width]] and [[src/vercel-workflow/vercel/workflow/_internal/ulid.py#RANDOM_FIELD_BITS|the randomness-field width]] determine [[src/vercel-workflow/vercel/workflow/_internal/ulid.py#VALUE_LIMIT_EXCLUSIVE|the exclusive integer upper bound]], the timestamp limit, and encoded field lengths. The Vercel world reuses this layout when tagging IDs and advancing their timestamps.

[[src/vercel-workflow/vercel/workflow/_internal/ulid.py#monotonic_factory]] generates ordinary ULIDs, and [[src/vercel-workflow/vercel/workflow/_internal/ulid.py#decode_time]] reads their timestamps. [[src/vercel-workflow/vercel/workflow/_internal/ulid.py#to_int]] and [[src/vercel-workflow/vercel/workflow/_internal/ulid.py#from_int]] convert between 26-character ULIDs and unsigned 128-bit integers. Decoding accepts ASCII in either case and rejects invalid lengths, characters, and overflow; encoding rejects out-of-range integers and emits uppercase text. Vercel run-ID creation and region decoding share this codec, while region tagging stays in the Vercel world.

[[src/vercel-workflow/tests/unit/test_ulid.py#TestIntegerConversion]] verifies known numeric values, case handling, malformed strings, and integer bounds.

## Regional routing

Reusable workflow configuration pins run data and queue dispatch to a region.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#_Configurable#with_region]] returns an immutable configuration and preserves argument compression settings. [[src/vercel-workflow/vercel/workflow/_internal/run.py#start]] passes the region hint to run-ID creation and queue delivery.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_create_run_id]] embeds TypeScript-compatible region metadata in Vercel run IDs and preserves monotonic ordering across region changes. Region selection prefers a recognized explicit hint, then the environment, then iad1. [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_region_from_tagged_run_id]] keeps later queue deliveries in the run’s region when no valid override is present. Queue clients are isolated by region. Local worlds ignore region hints and keep ordinary ULIDs.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_RunIdState]] keeps the generator and last emitted ID in thread-local state, preserving monotonicity across region changes within each thread without a shared mutex. Cross-thread ordering is not guaranteed.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_encode_run_id]] sets [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_ULID_TAG_BIT|the tag bit]] in the original timestamp field and replaces the high bits of the original randomness field with region/version metadata, leaving all other bits intact. Encoding ANDs the ID with [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_RUN_ID_METADATA_CLEAR_MASK|a module-level mask that clears metadata]], while decoding drops the random bits before extracting the region. Both derive their layout from [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_RUN_ID_REGION_BITS|the region width]] and [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_RUN_ID_VERSION_BITS|the version width]]. When correcting ordering, [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_bump_above_metadata]] advances the last emitted ID's timestamp before re-encoding it, matching TypeScript; timestamp exhaustion raises an error. [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#_region_from_tagged_run_id]] accepts ASCII Base32 in either case and rejects non-ASCII characters before case normalization.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld#_queue_client]] sends the selected region in the `x-vercel-queue-region` header when using the fixed queue proxy URL. Direct clients use regional hosts instead. Each client receives its own headers so regional routing does not mutate shared World headers.

The region option controls data and queue placement. Functions follow the application’s configured compute regions; if the requested region has no application functions, steps execute in the nearest configured compute region.

[[src/vercel-workflow/tests/unit/test_workflow_queue_transport.py]] checks TypeScript-generated region vectors, fallback, monotonicity, and queue region preservation. [[src/vercel-workflow/tests/unit/test_workflow_run.py]] checks immutable option composition, reuse, namespace preservation, and region hint propagation.

## Deployment targeting

Configured workflows can start on an explicit deployment or resolve the latest deployment in the current deployment's environment, negotiating the target's wire capabilities first.

[[src/vercel-workflow/vercel/workflow/_internal/core.py#_Configurable#with_deployment_id]] composes immutably with region and argument compression configuration on configuration objects and both workflow reference types. [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld#resolve_latest_deployment_id]] resolves the latest deployment through the Vercel API, preferring deployment OIDC identity inside Vercel and scoping requests to the configured team. Other worlds default to their current deployment and warn once that latest is a no-op. Explicit targets may be used without a current deployment identity; default and latest starts require one.

Latest-deployment credential resolution attempts OIDC at most once. [[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld#resolve_latest_deployment_id]] falls back on expected OIDC token errors and propagates unexpected exceptions.

[[src/vercel-workflow/vercel/workflow/_internal/deployment.py#_probe]] sends a JSON health message with a probeRunId and reads the synthetic health response stream. [[src/vercel-workflow/vercel/workflow/_internal/world.py#HealthCheckPayload]] names the field `probe_run_id` internally, emits `probeRunId`, and accepts both `probeRunId` and the legacy `runId` on the wire. Its per-World, deployment-and-namespace cache keeps answers for ten minutes without reusing run public keys, bounds entries to 256, and reduces the ten-second probe budget to two seconds for one minute after a miss.

[[src/vercel-workflow/vercel/workflow/_internal/deployment.py#resolve]] caps the target's spec at the caller's supported write version, stamps only attested target runtime markers, and gates argument compression on both spec and target core/runtime versions. JSON responses without valid versions select spec 3, old text responses select spec 2, and missed probes use the TypeScript slot-identity fallback capped to this caller's spec. A probed public key seals arguments without fetching a symmetric key; cached answers use the regular run-key lookup. [[src/vercel-workflow/vercel/workflow/_internal/deployment.py#Target#encoder]] selects the compatible compression and encryption envelope. Explicitly reported spec 1 targets are rejected before run creation because the Python writer supports event-sourced runs.

[[src/vercel-workflow/vercel/workflow/_internal/worlds/vercel.py#VercelWorld#queue]] uses JSON for probes and pre-CBOR targets, omitting inline run input for those targets. [[src/vercel-workflow/tests/unit/test_workflow_run.py]] covers negotiation cutoffs, sealed startup, and deployment configuration. [[src/vercel-workflow/tests/unit/test_workflow_health_check.py]] checks probe caching and retry budgets; [[src/vercel-workflow/tests/unit/test_workflow_queue_transport.py]] checks JSON probe transport, preserved legacy invocation fields, latest-deployment credential priority, and API failures.
