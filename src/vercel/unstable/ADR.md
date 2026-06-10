# ADR: `vercel.unstable` SDK API Design

## Status

Proposed.

## Context

The unstable SDK is a maintainer-facing namespace for designing the next Vercel
Python SDK API. It should support runtime services such as Sandbox, generated
resource APIs, scoped configuration, and both async and sync callers without
freezing premature implementation details.

This ADR records the target API design. It does not describe current
implementation gaps.

## Decision 1: Document Role

`src/vercel/unstable/README.md` is a design README for SDK maintainers and early
internal consumers.

It is not stable public documentation. It should describe the intended user
model with concrete examples while avoiding migration guarantees.

## Decision 2: Module-level Service APIs

Service APIs are exposed as module-level functions under domain packages.

Examples:

```python
from vercel.unstable import sandbox
from vercel.unstable.sandbox import SandboxQueryByName, TagFilter

sandbox_ = await sandbox.create_sandbox(runtime="python3.13")
sandbox_ = await sandbox.get_sandbox(name="preview")
sandboxes = [
    item
    async for item in sandbox.query_sandboxes(
        query=SandboxQueryByName(
            name_prefix="preview-",
            tag=TagFilter(key="env", value="prod"),
        )
    )
]
```

Endpoint input dataclasses such as `SandboxCreateParams` are not public API.
Service methods accept typed keyword arguments directly. `query_sandboxes()`
is the bounded exception: its `query` keyword accepts one typed query variant
for each supported index path (`SandboxQueryByCreatedAt`,
`SandboxQueryByName`, `SandboxQueryByStatusUpdatedAt`, or
`SandboxQueryByCurrentSnapshotId`). This prevents callers from constructing
name-prefix or tag-filter combinations that the selected ordering cannot
support.

Rationale:

Module functions make common usage direct while preserving strict typing and
domain package boundaries. They also keep SDK sessions as runtime context
rather than user-facing service factories.

Implementation note:

Sandbox wire responses are validated and converted into immutable neutral
domain state in its endpoint API client. `SandboxService` performs shared
async-first domain orchestration over that state, including polling and
compound results, without constructing public handles. The Sandbox package
owns its service constructor, including options, origin, credentials, and
orchestration wiring. It requests generic runtime capabilities from the SDK
session and caches one `SandboxService` through that session. Separate async
and sync runtime modules construct their matching public handles and consume
command-log streams; sync runtime entry points use `iter_coroutine()` to drive
service operations.

## Decision 3: Mode-specific Default Sessions And Scoped Overrides

The SDK has separate process default sessions for async and sync APIs:
`SdkSession` owns async resources and `SyncSdkSession` owns sync resources.
Module-level service functions use the default session for their API mode
unless a scoped override is active.

Scoped overrides use `vercel.session(...)`:

```python
from vercel import unstable as vercel

async with vercel.session(httpx_client_factory=client_factory):
    ...

with vercel.session(httpx_client_factory=sync_client_factory):
    ...
```

There is no public "configure default session" API. Callers either use the
appropriate mode's default session as-is or create a scoped override.

Session overrides are implemented with `ContextVar`. `async with
vercel.session(...)` binds an `SdkSession`; `with vercel.session(...)` binds a
`SyncSdkSession`. Service calls resolve the effective session at call time.
Calling a sync service facade while an explicit async session is active, or an
async facade while an explicit sync session is active, raises a session error
rather than silently constructing another pool or ignoring scoped
configuration.

Rationale:

Mode-specific default sessions keep simple scripts small while preventing async
and sync clients from being mixed in one lifetime owner. Context-local
overrides support tests, framework request scopes, nested behavior, and
concurrent tasks without mutating process-global configuration. Rejecting
opposite-mode calls makes service options and client-pool selection
deterministic.

## Decision 4: Nested Session Inheritance

Nested `vercel.session(...)` contexts of the same mode inherit from the
currently active session. Values supplied to the nested context replace
inherited values.

An omitted `httpx_client_factory` inherits its same-mode parent value. An
explicit `httpx_client_factory=None` replaces an inherited factory with
SDK-default HTTPX client construction. Entering a nested session in the
opposite mode is invalid and raises `VercelSessionError`.

Service option inheritance is by concrete option type. A nested
`SandboxServiceOptions(...)` replaces the inherited `SandboxServiceOptions`
object. Individual option fields do not merge.

Every service also owns a default options object. If no scoped option is
configured for a service, calls use that default rather than treating the
service as unavailable.

Rationale:

Inheritance lets callers override one concern without restating unrelated
settings. Whole-object replacement avoids hidden precedence and field-level
merge rules.

## Decision 5: Service Options

Service configuration belongs to the SDK session.

```python
async with vercel.session(
    service_options=[
        SandboxServiceOptions(base_url="https://sandbox-proxy.example.com"),
    ],
):
    ...
```

Each service option class inherits from a common service-options marker base
class. The SDK session validates at runtime that a `service_options` list
contains at most one instance of each concrete option type.

The session stores service options in a map keyed by concrete type.

Rationale:

Session-level service options let service functions remain focused on endpoint
semantics. Runtime validation catches ambiguous configuration early.

## Decision 6: Session-owned Transports And HTTP Client Pools

Each `SdkSession` lazily owns one async transport wrapping one
`httpx.AsyncClient` connection pool. Each `SyncSdkSession` lazily owns one
sync transport wrapping one `httpx.Client` connection pool. Services and
endpoint API clients borrow the matching transport; they do not allocate or
close a transport or pool for each service.

The transport and pool are origin-neutral: they are not configured with a
service `base_url`.
Endpoint API clients own their configured origins and build absolute request
URLs when sending through the shared session transport. A single service may
therefore reach more than one origin, and unrelated services may share the
same pool without sharing routing configuration.

`httpx_client_factory` is session configuration. In an async session it must
construct an `httpx.AsyncClient`; in a sync session it must construct an
`httpx.Client`. When a session first needs its transport, it validates the
factory result and wraps it in its matching transport. The context-manager
form selects the required return type:

```python
async with vercel.session(httpx_client_factory=async_client_factory):
    ...

with vercel.session(httpx_client_factory=sync_client_factory):
    ...
```

Overloads may catch mismatches for explicitly typed factories. They are not
sufficient for untyped factories, `Any`, or every use of one
`vercel.session(...)` constructor that supports both `with` and `async with`.
Transport initialization therefore validates the constructed client type and
raises a session configuration error for a mismatched factory before sending a
request.

The session configuration error is the existing public `VercelSessionError`;
this design does not add a new exported error subtype.

Rationale:

Transport and connection-pool lifetime are session concerns, whereas base URLs
are endpoint configuration. Separating them avoids creating a pool per service
or per origin and permits future endpoints such as token, usage, or JWKS
requests to reuse the same connections. Runtime validation prevents a
sync/async factory mismatch from failing later in an endpoint call.

Implementation note:

The transport remains useful for the common request envelope and for exposing
`httpx.Client.send()` through an async-shaped call when sync services drive
shared business logic through `iter_coroutine()`. Its current base-URL-bound
construction must change for the unstable session design: the session creates
an origin-neutral transport around the factory-produced client, owns its
shutdown, and lends it to endpoint clients. The transport may retain generic
request encoding, streaming mechanics, and bearer-header injection; endpoint
clients own origin selection, credential selection, and domain error
translation.

SDK sessions expose domain-neutral runtime capabilities to service packages:
open-state validation, option lookup, one cached instance per requested service
implementation type, a mode-specific shared transport, and an async-shaped
sleep operation. The sync sleep implementation blocks directly so shared
async-first service logic remains compatible with `iter_coroutine()`.

Each domain package owns its assembly function beside its neutral service. A
new unstable service should:

1. Define `<Domain>ServiceOptions(ServiceOptions)` in the domain package.
2. Implement an async-first neutral `<Domain>Service`.
3. Add `get_<domain>_service(session)` beside that service.
4. Construct domain endpoint clients from generic session capabilities.
5. Cache the result through `session.get_or_create_service(...)`.
6. Resolve active async or sync sessions only in public domain facades.

No central service registry or descriptor abstraction is needed until multiple
services demonstrate additional repeated structure.

## Decision 7: No Operation Policy Objects

Service methods do not expose complex timeout, retry, transport, or polling
policy objects.

Callers compose those policies outside the service call:

```python
async with asyncio.timeout(90):
    sandbox_ = await sandbox.create_sandbox(runtime="python3.13")
```

Rationale:

Timeout and retry policy often belongs to the application, framework, or job
runner. Keeping it outside service methods avoids freezing cross-service policy
types before the SDK has evidence for them.

## Decision 8: Sandbox Creation Waiting

`sandbox.create_sandbox(...)` always waits until the sandbox reaches a ready
state or a terminal state.

There is no `wait` argument. Terminal states raise typed sandbox errors.
Operation time limits are composed by the caller.

Rationale:

A created sandbox handle is expected to be ready for immediate use. Returning
half-created handles would push the common state-machine concern into every
caller.

## Decision 9: Sandbox Snapshot Expiration

Sandbox creation accepts `snapshot_expiration` for the platform-side lifetime of
snapshots owned by the sandbox.

```python
sandbox_ = await sandbox.create_sandbox(
    runtime="python3.13",
    name="preview",
    execution_time_limit=timedelta(minutes=5),
    snapshot_expiration=timedelta(days=1),
)
```

`snapshot_expiration` is remote sandbox data. It is not an SDK operation
timeout. `execution_time_limit` is the public sandbox execution limit name and
serializes to the backend `timeout` property.

All unstable Sandbox duration inputs accept numeric seconds or `timedelta`.
SDK state stores normalized `timedelta` values, and the HTTP boundary serializes
the backend's millisecond wire representation.
Snapshot expiration values accept `0` for no expiration or values from one day
through ten years inclusive.

Rationale:

The name keeps platform retention policy distinct from local call duration.

## Decision 10: Handle Validity

SDK handles retain the `SandboxService` created by their originating SDK
session. That session-owned service is the only local authority boundary for
follow-up requests.

Operational sandbox resources are stable mutable handles, not response model
values. State properties are read-only to callers and contain the most recent
state successfully observed through that particular handle. An operation
targeting a handle and receiving newer state for the same remote identity
updates the receiver in place and returns it. Operations that create, retrieve,
query, or list resources construct new independent handles; those aliases are
not synchronized automatically.

`Sandbox.current_session` is a nested bound handle. Sandbox convenience
operations targeting that current session update and return the existing
matching nested handle.

A named sandbox has at most one active current runtime session.
`Sandbox.session()` resolves that session through the get-or-resume endpoint.
It returns the existing session while it remains usable; otherwise the backend
creates a replacement from the latest snapshot and updates the sandbox's
`currentSessionId`. Concurrent resume requests converge on the same replacement
session. Previous sessions remain historical resources, not additional active
sessions.

`Sandbox.session()` returns an independent runtime session handle. It does not
apply the get-or-resume response to the existing `Sandbox` handle, so a
replacement backend session does not automatically replace that handle's
cached `current_session`. Creating a snapshot returns a new snapshot handle
while updating the addressed existing runtime session handle with the session
state included in the successful response.

Closing a `vercel.session(...)` scope closes its `SdkSession` or
`SyncSdkSession`; later requests through its handles or an already-captured
service raise `VercelSessionClosedError`.

Terminal state is observed state rather than local revocation.
Context-managed cleanup and explicit `Sandbox.destroy()`,
`SandboxRuntimeSession.stop()`, and `Snapshot.delete()` do not locally revoke
handles. Snapshot responses reporting a stopped runtime likewise do not revoke
the previous runtime-session handle. Remote existence and terminal state are
server-authoritative, so retained handles may receive ordinary API success or
error responses while their SDK session remains open.

`SandboxInvalidHandleError` remains for unattached or incorrectly mode-bound
handle objects, not closed sessions or remote lifecycle state.

Rationale:

Handles are bound to a session-owned service and its configuration.
Requiring reacquisition after session close keeps that binding explicit and
prevents stale handles from silently using the wrong configuration, without
guessing at server-side resource lifetime.

## Decision 11: Context-managed Cleanup

Context managers own remote cleanup.

Creating a sandbox as a context manager destroys that sandbox on exit:

```python
async with sandbox.create_sandbox(runtime="python3.13") as sandbox_:
    ...
```

Resolving a sandbox runtime session as a context manager stops that session on
exit:

```python
async with sandbox_.session() as session:
    ...
```

Context manager exit awaits cleanup and surfaces cleanup failures. It expresses
remote cleanup ownership, not reliable knowledge of future resource usability.
When cleanup succeeds, its response is applied to the managed handle before
exit returns.
Callers who do not want that cleanup requested should not use the context
manager.

Live verification observes sandbox cleanup as absence: a post-exit
`get_sandbox(name=..., resume=False)` must return not found. Runtime-session
cleanup has a different observable contract: after its context exits, the
retained runtime-session handle records `SandboxStatus.STOPPED` from the stop
response.

The same remote cleanup can be requested explicitly with `Sandbox.destroy()` or
`SandboxRuntimeSession.stop()`. Sandbox identity behavior belongs on `Sandbox`,
session-scoped lifecycle and command behavior belongs on
`SandboxRuntimeSession`, filesystem behavior belongs on each handle's `fs`
capability, and endpoint composition belongs in the internal Sandbox service.

Rationale:

The context manager syntax should mean scoped ownership. Awaiting cleanup makes
the lifecycle deterministic and visible.

## Decision 12: Sandbox Defaults Vs Session Controls

Named sandbox defaults and running session controls are separate public
operations.

`Sandbox.update(...)` changes persistent named sandbox configuration such as
runtime, resources, ports, tags, snapshot expiration, retention, environment,
and the current snapshot source. These values affect later session creation and
named sandbox metadata.
Passing `snapshot_retention=None` explicitly clears retention by sending
`keepLastSnapshots: null`; omitting the keyword preserves the existing policy.

`SandboxRuntimeSession.extend_execution_time_limit(...)` and
`SandboxRuntimeSession.update_network_policy(...)` target one running session.
`Sandbox.extend_execution_time_limit(...)` and
`Sandbox.update_network_policy(...)` are convenience methods for the sandbox's
current session, not persistent default updates.

Project-wide session listing is a module-level operation
`sandbox.query_sessions(...)`; named sandbox-scoped listing lives on
`Sandbox.list_sessions(...)`.

`Sandbox.run_process(...)`, `Sandbox.create_process(...)`, and their
`SandboxRuntimeSession` equivalents accept `kill_after` as a numeric duration
in seconds or a `timedelta`. This is a per-command exec-time limit: expiry
kills that command with `SIGKILL`, including commands returned immediately by
`create_process(...)`. The name intentionally distinguishes this server-side
command policy from session `execution_time_limit` and local waiting policy.

Rationale:

The v2 REST API has both named sandbox configuration and session-centered
mutation routes. Keeping them distinct prevents a caller from accidentally
assuming that a running-session change persists as a sandbox default.

## Decision 13: Domain-owned Sync Mirror

Async is the primary API. Sync support mirrors each domain inside that domain
package.

Example:

```python
from vercel.unstable.sandbox import sync as sandbox

sandbox_ = sandbox.create_sandbox(runtime="python3.13")
```

The sync mirror resolves `SyncSdkSession` rather than `SdkSession`, and follows
the same service option, waiting, cleanup, and session-bound handle rules as
the async API.

Rationale:

Keeping sync under the domain package avoids cluttering async modules with
`*_sync` functions and avoids a separate top-level sync namespace.

## Decision 14: Error Root

All unstable SDK exceptions inherit from `vercel.unstable.VercelError`.

Session errors inherit from `VercelSessionError`. Domain errors inherit from
domain-specific bases such as `SandboxError`.

Unattached or mode-invalid sandbox handles raise `SandboxInvalidHandleError`.
Requests through a closed SDK session raise `VercelSessionClosedError`.
Sandbox context manager cleanup failures raise `SandboxCleanupError`; the error carries
`resource_type`, `resource_id`, and the underlying `cause`. Sandbox v2 API
errors raise `SandboxApiError`, which preserves `status_code`, structured
`data`, and the v2 error `code` when available.

Rationale:

A single SDK root gives callers one catch point. Domain bases preserve useful
service-level handling.

## Decision 15: Process Output Observation

`Process.logs()` and its sync mirror yield only output records.
`ProcessLog.stream` is the exported string-compatible
`ProcessLogStream` enum, with `STDOUT` and `STDERR` members serialized
to the wire strings `"stdout"` and `"stderr"`. A valid structured wire error
record terminates iteration by raising `SandboxStreamError`, which inherits
from `SandboxError` and carries the wire `code`. Malformed and unsupported
records do not become public events.

Each `Process.logs()` call opens a fresh combined ordered stream. `Process`
also owns stable one-shot `stdout` and `stderr` text readers. Each reader lazily
filters one shared combined-log request. Reader iteration and
`receive()` yield logical lines preserving newlines; `read()` and `readline()`
share one cursor per stream. Explicitly closing one reader preserves its peer.

Readers reject concurrent reads with `anyio.BusyResourceError`, reads after
explicit closure with `anyio.ClosedResourceError`, and `receive()` at EOF with
`anyio.EndOfStream`. Cancellation, transport failure, and in-band stream errors
break both readers; later reads raise `anyio.BrokenResourceError`. Process
output is not cached.

Rationale:

Combined logs preserve backend ordering while a shared transport provides the
Python-familiar per-stream interface with one backend request.

## Decision 16: Sandbox Filesystem Capability

Unstable workspace filesystem access is exposed through `Sandbox.fs` and
`SandboxRuntimeSession.fs`, implemented by `SandboxFilesystem` and its sync
mirror `SyncSandboxFilesystem`. Direct `mkdir`, `read_file`, `read_text`, and
`write_files` methods and public write-file value types do not exist.

`Sandbox.fs` targets `current_session_id` at operation time so retained
capabilities follow current-session changes. `SandboxRuntimeSession.fs` is
bound to that runtime session identity. Single-file `write_bytes()` and
`write_text()` methods submit one file. `batch()` stages files synchronously
inside an async or sync context and submits one tarball only on clean, non-empty
exit. The capability also includes native-backed directory creation and reads,
plus command-backed predicates, direct-child listing, removal, and rename.
`DirectoryEntry` is a passive, sorted listing result with coarse `file`,
`directory`, `symlink`, or `other` classification.

Command-backed methods execute fixed portable shell scripts with user paths
passed as positional arguments. Relative operands are prefixed within those
scripts before utilities receive them, preventing leading `-` path components
from becoming options. `rename()` deliberately retains ordinary `mv`
overwrite behavior.

`SandboxFilesystemError` is the filesystem error root.
`SandboxPathNotFoundError` is produced for native endpoint failures only when
structured backend error data proves a missing path; otherwise the ordinary
`SandboxApiError` is preserved. Predicate non-matches return `False`.
`SandboxFilesystemCommandError` reports failed command-backed list, remove,
and rename operations with operation name, paths, exit code, stdout, and
stderr.
`SandboxFilesystemWriteError` wraps native batch-write API failures and retains
the submitted paths, resolved cwd, and original `SandboxApiError`. Tar uploads
remain memory-buffered; converting the completed archive to the transport body
currently makes one additional copy until streaming uploads are available.

Rationale:

A capability object keeps the sandbox and runtime-session handle surfaces
focused on lifecycle and commands while giving workspace editing a coherent
API. Native endpoints are retained where they exist, while command-backed
operations provide a bounded initial contract without claiming rich POSIX
metadata or new server endpoints.

## Consequences

Positive consequences:

- Common usage is direct: import a service module and call functions.
- Scoped configuration works for tests, frameworks, and nested service behavior.
- Service configuration is centralized at SDK session boundaries.
- Each session owns one transport and HTTP pool for its runtime mode; endpoint
  clients own their origins.
- Service methods stay small and endpoint-focused.
- Context managers express remote cleanup ownership while remote lifecycle
  remains server-authoritative.
- Sync support exists without weakening the async-first API.

Costs:

- Context-local session resolution requires careful tests.
- Wrong-mode scoped service calls and client factories require explicit runtime
  errors.
- Handles retain their originating SDK session for close checks.
- Nested session inheritance must be precise and well documented.
- Sync mirrors add duplicate API surface.
- Avoiding built-in retry and timeout policy pushes that composition to callers.

## Open Questions

- Whether future session-level HTTP configuration should extend
  `httpx_client_factory` or introduce additional narrowly scoped settings.
