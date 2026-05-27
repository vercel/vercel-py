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

sandbox_ = await sandbox.create_sandbox(runtime="python3.13")
sandbox_ = await sandbox.get_sandbox(name="preview")
sandboxes = [item async for item in sandbox.query_sandboxes()]
```

Endpoint input dataclasses such as `SandboxCreateParams` are not public API.
Service methods accept typed keyword arguments directly.

Rationale:

Module functions make common usage direct while preserving strict typing and
domain package boundaries. They also keep SDK sessions as runtime context
rather than user-facing service factories.

## Decision 3: Default Session And Scoped Overrides

The SDK has a process default session. Module-level service functions use that
default session unless a scoped override is active.

Scoped overrides use `vercel.session(...)`:

```python
from vercel import unstable as vercel

async with vercel.session(httpx_client_factory=client_factory):
    ...
```

There is no public "configure default session" API. Callers either use the
default session as-is or create a scoped override.

Session overrides are implemented with `ContextVar`. Service calls resolve the
effective session at call time.

Rationale:

The default session keeps simple scripts small. Context-local overrides support
tests, framework request scopes, nested behavior, and concurrent tasks without
mutating process-global configuration.

## Decision 4: Nested Session Inheritance

Nested `vercel.session(...)` contexts inherit from the currently active session.
Values supplied to the nested context replace inherited values.

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

## Decision 6: No Operation Policy Objects

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

## Decision 7: Sandbox Creation Waiting

`sandbox.create_sandbox(...)` always waits until the sandbox reaches a ready
state or a terminal state.

There is no `wait` argument. Terminal states raise typed sandbox errors.
Operation time limits are composed by the caller.

Rationale:

A created sandbox handle is expected to be ready for immediate use. Returning
half-created handles would push the common state-machine concern into every
caller.

## Decision 8: Sandbox Snapshot Expiration

Sandbox creation accepts `snapshot_expiration` for the platform-side lifetime of
snapshots owned by the sandbox.

```python
sandbox_ = await sandbox.create_sandbox(
    runtime="python3.13",
    name="preview",
    execution_time_limit=timedelta(minutes=5),
    snapshot_expiration=timedelta(minutes=20),
)
```

`snapshot_expiration` is remote sandbox data. It is not an SDK operation
timeout. `execution_time_limit` is the public sandbox execution limit name and
serializes to the backend `timeout` property.

Rationale:

The name keeps platform retention policy distinct from local call duration.

## Decision 9: Handle Validity

SDK handles carry an internal alive marker.

A handle is valid only while:

- its owning SDK session is alive
- its own context-managed remote resource has not exited
- explicit cleanup such as `destroy()` or `stop()` has not succeeded

Handles created inside a `vercel.session(...)` context are invalidated when that
session context exits. The remote resource may still exist, but the old Python
handle is not usable.

Context-managed sandboxes and sandbox runtime sessions invalidate their handles
after cleanup.

Explicit `Sandbox.destroy()` and `SandboxRuntimeSession.stop()` follow the same
invalidation rule after successful remote cleanup.

The design specifies the invalidation behavior but does not freeze the exact
exception name yet.

Rationale:

Handles are bound to SDK runtime resources such as clients, options, and close
hooks. Requiring reacquisition after session close keeps that binding explicit
and prevents stale handles from silently using the wrong configuration.

## Decision 10: Context-managed Cleanup

Context managers own remote cleanup.

Creating a sandbox as a context manager destroys that sandbox on exit:

```python
async with sandbox.create_sandbox(runtime="python3.13") as sandbox_:
    ...
```

Creating a sandbox runtime session as a context manager destroys that runtime
session on exit:

```python
async with sandbox_.session() as session:
    ...
```

Context manager exit awaits cleanup and surfaces cleanup failures. Callers who
want a sandbox or runtime session to survive should not use its context manager.

The same remote cleanup can be requested explicitly with `Sandbox.destroy()` or
`SandboxRuntimeSession.stop()`. Sandbox identity behavior belongs on `Sandbox`,
session-scoped behavior belongs on `SandboxRuntimeSession`, and endpoint
composition belongs in the internal Sandbox service.

Rationale:

The context manager syntax should mean scoped ownership. Awaiting cleanup makes
the lifecycle deterministic and visible.

## Decision 11: Sandbox Defaults vs Session Controls

Named sandbox defaults and running session controls are separate public
operations.

`Sandbox.update(...)` changes persistent named sandbox configuration such as
runtime, resources, ports, tags, snapshot expiration, retention, environment,
and the current snapshot source. These values affect later session creation and
named sandbox metadata.

`SandboxRuntimeSession.extend_execution_time_limit(...)` and
`SandboxRuntimeSession.update_network_policy(...)` target one running session.
`Sandbox.extend_execution_time_limit(...)` and
`Sandbox.update_network_policy(...)` are convenience methods for the sandbox's
current session, not persistent default updates.

Project-wide session listing is a module-level operation
`sandbox.query_sessions(...)`; named sandbox-scoped listing lives on
`Sandbox.list_sessions(...)`.

Rationale:

The v2 REST API has both named sandbox configuration and session-centered
mutation routes. Keeping them distinct prevents a caller from accidentally
assuming that a running-session change persists as a sandbox default.

## Decision 12: Domain-owned Sync Mirror

Async is the primary API. Sync support mirrors each domain inside that domain
package.

Example:

```python
from vercel.unstable.sandbox import sync as sandbox

sandbox_ = sandbox.create_sandbox(runtime="python3.13")
```

The sync mirror follows the same session resolution, service option, waiting,
cleanup, and invalidation rules as the async API.

Rationale:

Keeping sync under the domain package avoids cluttering async modules with
`*_sync` functions and avoids a separate top-level sync namespace.

## Decision 13: Error Root

All unstable SDK exceptions inherit from `vercel.unstable.VercelError`.

Session errors inherit from `VercelSessionError`. Domain errors inherit from
domain-specific bases such as `SandboxError`.

Sandbox invalid handles raise `SandboxInvalidHandleError`. Sandbox context
manager cleanup failures raise `SandboxCleanupError`; the error carries
`resource_type`, `resource_id`, and the underlying `cause`. Sandbox v2 API
errors raise `SandboxApiError`, which preserves `status_code`, structured
`data`, and the v2 error `code` when available.

Rationale:

A single SDK root gives callers one catch point. Domain bases preserve useful
service-level handling.

## Consequences

Positive consequences:

- Common usage is direct: import a service module and call functions.
- Scoped configuration works for tests, frameworks, and nested service behavior.
- Service configuration is centralized at SDK session boundaries.
- Service methods stay small and endpoint-focused.
- Resource lifetime follows Python context manager ownership.
- Sync support exists without weakening the async-first API.

Costs:

- Context-local session resolution requires careful tests.
- Handles need explicit validity tracking.
- Nested session inheritance must be precise and well documented.
- Sync mirrors add duplicate API surface.
- Avoiding built-in retry and timeout policy pushes that composition to callers.

## Open Questions

- Exact transport/session option names beyond `httpx_client_factory`.
