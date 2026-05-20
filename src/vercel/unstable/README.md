# `vercel.unstable` SDK Design

This package is a maintainer-facing design workbench for the next Vercel Python
SDK API. It is intentionally not polished external user documentation.

APIs under `vercel.unstable` may change without migration support. The goal is
to ratify the design shape, prove it against concrete services, and then decide
what should graduate into a stable public surface.

Sandbox is the first implementation target. Other examples in this document,
especially management resources such as projects, describe the intended
architecture and are not an implementation commitment.

Code examples in this document use the proposed `vercel.unstable` API shape.
When a snippet imports the unstable facade, it aliases it as `vercel` so call
sites read like the future stable API while still making the import source
explicit:

```python
from vercel import unstable as vercel
```

## Target Usage

Simple async usage goes through the default session for the active context:

```python
from datetime import timedelta

from vercel import unstable as vercel
from vercel.unstable.sandbox import SandboxCreateParams

sandbox = await vercel.sandbox.create(
    SandboxCreateParams(runtime="python3.12"),
    timeout=timedelta(seconds=90),
)
result = await sandbox.run_command("python --version")
```

Method domain data is passed through typed params objects. Keyword arguments on
methods are reserved for SDK behavior, not API payload fields. In the example
above, `SandboxCreateParams.runtime` is sandbox creation data, while the method
keyword `timeout` is the maximum time the SDK call may take before returning or
raising.

Configured applications create explicit sessions:

```python
from datetime import timedelta

from vercel.unstable import Session, SessionOptions
from vercel.unstable.sandbox import SandboxCreateParams

async with Session(
    options=SessionOptions(request_timeout=timedelta(seconds=30))
) as session:
    sandbox = await session.sandbox.create(
        SandboxCreateParams(runtime="python3.12"),
        timeout=timedelta(seconds=90),
    )
```

The default facade can be rebound to an explicit session for a scoped async
context. This is implemented with context-local state, so concurrent tasks can
bind different sessions without mutating process-global state:

```python
from datetime import timedelta

from vercel import unstable as vercel
from vercel.unstable import Session, SessionOptions
from vercel.unstable.sandbox import SandboxCreateParams

async with Session(
    options=SessionOptions(request_timeout=timedelta(seconds=30))
) as session:
    async with vercel.use_session(session):
        sandbox = await vercel.sandbox.create(
            SandboxCreateParams(runtime="python3.12"),
            timeout=timedelta(seconds=90),
        )
```

Long-lived applications should own sessions and close them deterministically.
For FastAPI, the app lifespan is the natural place to create and close the SDK
session while still allowing request handlers to use the default facade:

```python
from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI, Request
from vercel import unstable as vercel
from vercel.unstable import Session, SessionOptions
from vercel.unstable.sandbox import SandboxCreateParams


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with Session(
        options=SessionOptions(request_timeout=timedelta(seconds=30))
    ) as session:
        app.state.vercel_session = session
        yield


app = FastAPI(lifespan=lifespan)


@app.post("/sandboxes")
async def create_sandbox(request: Request):
    async with vercel.use_session(request.app.state.vercel_session):
        sandbox = await vercel.sandbox.create(
            SandboxCreateParams(runtime="python3.12"),
            timeout=timedelta(seconds=90),
        )
        return {"name": sandbox.name}
```

The top-level Sandbox accessor is singular because the product name is Vercel
Sandbox:

```python
from datetime import timedelta

from vercel import unstable as vercel
from vercel.unstable.sandbox import SandboxCreateParams

sandbox = await vercel.sandbox.create(
    SandboxCreateParams(runtime="python3.12"),
    timeout=timedelta(seconds=90),
)
snapshot = await sandbox.snapshot()

async for snapshot in sandbox.list_snapshots():
    ...
```

Sandbox snapshots are service objects, not resource-shaped child accessors.
`sandbox.snapshot()` is a verb that creates a snapshot. `sandbox.list_snapshots()`
returns a paginated async collection of instantiated `Snapshot` handles.

Management resources are illustrative in this README. They show the proposed
shape for generated REST resource APIs, not a commitment that resources ship
with the first Sandbox implementation. They should live under an explicit
resources namespace and use access-token-only credentials:

```python
from vercel import unstable as vercel

project = await vercel.resources.projects.get("prj_123")
```

The same shape works with configured sessions:

```python
from vercel.unstable import Session
from vercel.unstable.sandbox import SandboxCreateParams

async with Session() as session:
    sandbox = await session.sandbox.create(SandboxCreateParams())
    project = await session.resources.projects.get("prj_123")
```

Sync support is explicit and async remains the conceptual default:

```python
from vercel.unstable import SyncSession
from vercel.unstable.sandbox import SandboxCreateParams

with SyncSession() as session:
    sandbox = session.sandbox.create(SandboxCreateParams())
```

There is no default sync facade initially. If sync default usage proves useful,
we can add a separate default-bound sync shape later.

The sync surface is derived from the async implementation through internal
tooling (`vercel._internal.iter_coroutine`) rather than a hand-maintained
parallel hierarchy. This is an implementation detail; users should not depend
on it.

## Package Shape

`vercel.unstable` is a curated facade. It should export only top-of-funnel
concepts:

- `Session`
- `SyncSession`
- `SessionOptions`
- `setup_default_session(...)`
- `get_default_session()`
- `use_session(...)`
- `VercelError`
- default-bound accessors such as `sandbox` and `resources`

Domain-specific types stay in domain packages. A repeated class name is allowed
when the module boundary carries the distinction; for example,
`vercel.unstable.Session` is the SDK lifecycle session, while
`vercel.unstable.sandbox.Session` is a Sandbox runtime session returned by the
Sandbox API and normally reached through `sandbox.current_session`:

```python
from vercel.unstable.sandbox import (
    Sandbox,
    SandboxCreateParams,
    SandboxOptions,
    SandboxStatus,
    Session as SandboxRuntimeSession,
    Snapshot,
)
```

Auth types stay under `vercel.unstable.auth`:

```python
from vercel.unstable.auth import (
    AccessTokenCredentials,
    OIDCCredentials,
    StaticCredentialProvider,
)
```

Testing-only default-session controls do not belong in the curated facade:

```python
from vercel.unstable.testing import reset_default_session
```

## Accessors And Default Binding

Accessors represent configured service clients bound to a session. They are not
modules with functions and they are not domain objects.

`session.sandbox` should return the same default accessor instance each time:

```python
session.sandbox is session.sandbox  # True
```

Top-level facade accessors such as `vercel.sandbox` are exported proxy objects.
They are not modules and they are not accessor instances bound to a concrete
session. Importing the facade or reading `vercel.sandbox` must not create or
initialize the default session.

The proxy object is default-bound lazily. On the first operation, the proxy
resolves the effective default session:

1. The context-local session bound by `use_session(...)`.
2. The process-global lazy default session.

After resolving the session, the proxy delegates to that session's real
accessor:

```python
from vercel import unstable as vercel

await vercel.sandbox.create(params)

# Conceptually equivalent to:
session = vercel.get_default_session()
await session.sandbox.create(params)
```

This is implementable in Python with a small object that defines methods and
properties matching the accessor protocol. Each proxy method resolves the
effective session and forwards the call to the bound accessor. The proxy should
not use module-level `__getattr__` as the primary mechanism because the exported
object itself needs a stable type for autocomplete and type checkers:

```python
class _DefaultBoundSandboxAccessor:
    def __init__(self, options=None):
        self._options = options

    def _accessor(self):
        accessor = get_default_session().sandbox
        if self._options is not None:
            accessor = accessor.with_options(self._options)
        return accessor

    async def create(self, params, **kwargs):
        return await self._accessor().create(params, **kwargs)

    def with_options(self, options=None, **kwargs):
        return type(self)(merge_options(self._options, options, kwargs))

    def with_session(self, session):
        accessor = session.sandbox
        if self._options is not None:
            accessor = accessor.with_options(self._options)
        return accessor
```

The concrete implementation can reduce forwarding boilerplate with internal
helpers, but the public behavior should stay object-shaped and typed.
Configuration-only methods such as `with_options(...)` can return another
default-bound proxy without resolving the default session. I/O methods and
explicit rebinding through `with_session(...)` resolve to a real accessor. This
means `vercel.sandbox.create(...)` and `session.sandbox.create(...)` use the
same accessor implementation after binding, but differ in how the session is
chosen.

Accessors are immutable-ish configured handles. `with_options(...)` returns a
clone with merged service options, even when called without arguments:

```python
clone = session.sandbox.with_options()
assert clone is not session.sandbox
```

Each accessor instance owns its own lazily initialized service client state, but
borrows session-level pools. Cloned accessors are cheap until first use.

Accessors support explicit session rebinding with `with_session(...)`:

```python
from vercel import unstable as vercel

sandbox = vercel.sandbox.with_session(session)
```

Domain handles are also default-bound for methods. A directly constructed handle
such as `Snapshot(id="snap_123")` stores only its identifier until an I/O method
is called. Methods resolve the effective default session unless the handle has
been explicitly rebound:

```python
from vercel.unstable.sandbox import Snapshot

snapshot = Snapshot(id="snap_123")
await snapshot.delete()

snapshot = Snapshot(id="snap_123").with_session(session)
await snapshot.delete()
```

Properties never resolve sessions or perform hidden I/O. If a required property
has not been loaded yet, reading it raises a typed unloaded-state error. Call
`refresh()` to load state explicitly.

## Architecture

The core layering is:

```text
Session
  owns lifecycle resources:
  - low-level transport pools
  - observability and logging primitives
  - close hooks for initialized accessors
  - lazy initialization state

Service accessor
  owns service policy:
  - options
  - credential provider policy
  - a credential-aware RequestClient
  - lazy service-specific initialization

RequestClient
  owns request execution policy:
  - per-request credential resolution
  - credential validation and normalization
  - authenticated and anonymous request paths
  - transport calls

Transport
  owns low-level HTTP execution and pooling integration

Domain handle / service method
  owns API semantics:
  - request construction
  - response parsing
  - state transitions
  - cached state updates
```

`RequestClient` and `Transport` are internal implementation concepts for now.
Transport customization should happen through typed options such as
`request_timeout`, base URL, proxy, and limits until a public transport
abstraction proves necessary.

## Sessions

`Session` is the runtime and lifecycle container. It is not the universal auth
policy container.

Sessions should be as lazy as possible. Constructing a session must not read the
environment, open network clients, or initialize service accessors. Side effects
happen at explicit initialization or first use.

```python
from datetime import timedelta

session = Session(options=SessionOptions(request_timeout=timedelta(seconds=30)))

await session.initialize()
await session.close()
```

`Session.initialize()` initializes direct session dependencies only. It may
prepare shared lifecycle resources such as transport pools and observability
hooks. It must not initialize `session.sandbox`, `session.resources.projects`,
or any other service accessor.

Each level initializes its own direct dependencies. Some duplicated setup across
services is acceptable at the design level; caching and deduplication are
implementation optimizations, not conceptual coupling.

Accessors also expose explicit initialization:

```python
await session.sandbox.initialize()
```

Accessor initialization ensures the parent session is initialized enough to
provide shared resources, then initializes only that accessor's direct
dependencies. It should select or construct the credential provider and
`RequestClient`, but it should not resolve credentials.

Credential resolution happens per authenticated request.

`Session.close()` closes all lifecycle state owned by the session, including
registered close hooks from initialized accessors. Individual sessions are
permanently closed; future use should raise a typed lifecycle error. Create a
new session instead.

## Default Sessions

`use_session(session)` binds an already-created session to the current async
context. It does not create or close the session:

```python
from vercel import unstable as vercel
from vercel.unstable import Session
from vercel.unstable.sandbox import SandboxCreateParams

async with Session() as session:
    async with vercel.use_session(session):
        await vercel.sandbox.create(SandboxCreateParams())
```

`setup_default_session(...)` configures the process-global fallback factory:

```python
from datetime import timedelta

from vercel import unstable as vercel
from vercel.unstable import SessionOptions

vercel.setup_default_session(
    options=SessionOptions(request_timeout=timedelta(seconds=30)),
)
```

It affects only the process-global fallback, not an active context-local
session. Multiple pre-initialization calls are allowed; the last call wins.
Once the process-global fallback session has been initialized, reconfiguration
is rejected until testing/internal reset clears it.

`get_default_session()` returns the effective default session: context-local
when present, otherwise the process-global fallback. Calling it may create a
`Session` object, but it must not initialize transports, read service
configuration, or resolve credentials.

The process-global fallback is treated as process-lifetime state for simple
usage. Best-effort interpreter-exit cleanup is acceptable, but correctness must
not depend on it. Deterministic cleanup comes from explicit session ownership or
testing helpers.

## Options, Params, And Method Behavior

Use `Options` terminology for user-supplied service or session policy. Options
may include inert values and dependency objects such as credential providers.
Runtime state does not belong in options:

- no initialized HTTP clients
- no opened pools
- no cached service clients
- no mutable request state

Options classes should be frozen dataclasses.

```python
from datetime import timedelta

from vercel.unstable.sandbox import SandboxOptions

sandbox = session.sandbox.with_options(
    SandboxOptions(
        api_endpoint="https://api.vercel.com",
        project_id="prj_123",
        team_id="team_123",
        request_timeout=timedelta(seconds=30),
    )
)
```

`SandboxOptions` represents service policy: API endpoint, credential provider,
project/team scope, and request-level defaults. It does not contain sandbox
creation payload fields such as runtime, ports, env, resources, network policy,
or sandbox lifetime timeout.

Use `Params` terminology for method domain data. Params objects should be frozen
dataclasses and should keep product/API field names even when those names would
collide with SDK behavior keywords outside the object:

```python
from datetime import timedelta

from vercel.unstable.sandbox import SandboxCreateParams

params = SandboxCreateParams(
    runtime="python3.12",
    timeout=timedelta(minutes=20),
)

sandbox = await session.sandbox.create(
    params,
    timeout=timedelta(seconds=90),
)
```

In this example, `params.timeout` is the remote sandbox lifetime. The method
keyword `timeout` is the SDK operation budget.

Methods should accept params objects as the first positional argument and as
`params=`:

```python
await session.sandbox.create(SandboxCreateParams(runtime="python3.12"))
await session.sandbox.create(params=SandboxCreateParams(runtime="python3.12"))
```

Keyword arguments on methods are reserved for behavior and other non-data
controls. Initially:

- `timeout`: whole-method budget as a `timedelta`, including transport,
  retries, uploads/downloads owned by the method, polling, and final state
  reconciliation
- `wait`: lifecycle behavior toggle for mutations that have natural completion
  states

Do not add broad method behavior defaults yet. A future call-policy mechanism
may provide repeated behavior configuration across many calls, but per-method
kwargs are enough for the first unstable surface.

Simple retrieval selectors may remain keyword-shaped rather than requiring a
params object. For v2 Sandbox, `name` selects the named/persistent sandbox
identity. Runtime session IDs belong to the attached
`vercel.unstable.sandbox.Session` and session-scoped operations, not to the
primary `Sandbox` handle identity:

```python
sandbox = await session.sandbox.get(name="preview-db")
session_id = sandbox.current_session.id
```

Options merging should use an explicit unset sentinel internally. Omitted values
inherit. `None` is a real value only for fields whose type allows it. The unset
sentinel should not be part of the top-level curated facade.

## Auth

Auth lives under `vercel.unstable.auth`.

Credential objects are nominal, even when multiple forms ultimately produce
bearer auth:

```python
AccessTokenCredentials
OIDCCredentials
```

These may share a public protocol inside `vercel.unstable.auth`, but services
should type the credential forms they accept:

```python
SandboxCredentials = OIDCCredentials | AccessTokenCredentials
ResourceCredentials = AccessTokenCredentials
```

Credential providers are generic over the credentials they return:

```python
class CredentialProvider[T_Credentials](Protocol):
    async def resolve(self) -> T_Credentials: ...

class SyncCredentialProvider[T_Credentials](Protocol):
    def resolve(self) -> T_Credentials: ...
```

The verb is `resolve()` because providers may do more than look up a static
value. A default provider may read request-local headers, read environment
variables, perform token exchange, or fall back through multiple credential
sources.

The SDK must call `resolve()` for every authenticated request. Providers may
cache internally, but request paths must support fresh credentials injected per
request.

`resolve()` takes no arguments initially. Providers that need ambient request
context can use runtime-specific mechanisms. If explicit request context becomes
necessary, add a richer provider protocol later.

Credential provider configuration belongs on service/resource options, not on
`Session`. Each accessor owns its credential policy because accepted credential
forms vary by service.

Unsupported credential forms should be rejected locally before making HTTP
requests:

```python
UnsupportedCredentialsError
```

Auth errors should be typed under `vercel.unstable.auth`, for example:

```python
AuthError
CredentialResolutionError
UnsupportedCredentialsError
```

## Services And Resources

The design distinguishes behavioral service surfaces from generated management
resources.

Service surfaces expose product behavior and may create handles to remote
objects with methods and cached state. Sandbox is the first concrete example:
`session.sandbox.create(...)` returns a `Sandbox` handle, and
`sandbox.snapshot()` returns a `Snapshot` handle.

Management resources represent REST resource APIs. Generated OpenAPI-backed
resources should eventually live under `session.resources` and may return
Pydantic output models where the API is record-shaped. These examples are
future/illustrative; Sandbox remains the first concrete implementation target:

```python
await session.sandbox.create(SandboxCreateParams())
await session.resources.projects.get("prj_123")
```

Auth is one major way services and resources vary, but it is not the category
definition. Each accessor owns credential policy, endpoint policy, retry/call
policy, and resource behavior appropriate to its surface.

We considered a separate top-level `sdk` package for generated management
resources. The current recommendation is to keep them under
`vercel.unstable.resources` until the split proves too large for one package.

## Models And Handles

Do not force every surface into one model technology.

Method inputs should be ergonomic and strictly typed `*Params` dataclasses.

Generated management resource outputs may be Pydantic models where the API is
record-shaped.

Service objects should be custom behavioral handles. A `Sandbox` is not just a
Pydantic model; it is a handle to a remote object with actions, lifecycle
methods, cached state, and related service methods.

Handles may keep remote state in private typed models:

```python
class Sandbox:
    _state: _SandboxState | None

    @property
    def name(self) -> str: ...

    @property
    def current_session(self) -> vercel.unstable.sandbox.Session: ...

    @property
    def status(self) -> SandboxStatus: ...
```

The sandbox-domain `Session` type intentionally relies on the module boundary
instead of a stuttering public name such as `SandboxSession`. It must not be
re-exported from the top-level `vercel.unstable` facade; examples that import
both session types should alias at least one of them.

Expose stable first-class properties for commonly used fields. Avoid requiring
users to dig through `.data` for the main workflow.

Directly constructed handles are keyword-only and may be partially loaded:

```python
from vercel.unstable.sandbox import Sandbox, Snapshot

sandbox = Sandbox(name="preview-db")
snapshot = Snapshot(id="snap_123")
```

Only known constructor fields are readable before `refresh()`. Required cached
fields that are not loaded raise a typed unloaded-state error.

## Sandbox Lifecycle

Sandbox is the first proving ground for this design.

`Sandbox` objects are stable handles with explicit mutable cached state:

- object identity represents the remote sandbox identity
- property access reads cached state only
- property access must not perform hidden network I/O
- explicit methods may refresh or mutate cached state
- methods that receive updated state from the API should apply it before
  returning

`refresh()` updates the object in place and returns `self`:

```python
sandbox = await session.sandbox.get(name="preview-db")
await sandbox.refresh()
```

Lifecycle methods return the same domain object:

```python
await sandbox.stop()
assert await sandbox.stop() is sandbox
```

Many lifecycle mutations are asynchronous on the server. Methods wait for
their natural completion status by default; pass `wait=False` to return
immediately after the API call:

```python
await sandbox.stop()             # wait for STOPPED
await sandbox.stop(wait=False)   # return after issuing the API call
```

Each lifecycle method has an implicit target status: `create` waits for
`READY`, `stop` waits for `STOPPED`, and so on. Per-method targets are
defined alongside the implementation.

Methods accept an overall `timeout=` argument that bounds the entire call,
including transport, retries, and the state-transition wait:

```python
from datetime import timedelta

await sandbox.stop(timeout=timedelta(seconds=60))
```

Timeouts and other durations in the unstable API are `timedelta` values. Do not
accept numeric seconds or milliseconds in the new API surface.

If the method-level timeout expires after a mutation request may have succeeded,
raise a typed operation timeout error. When possible, the error should carry the
latest known handle or state, because server-side work may continue after the
Python call has stopped waiting.

Per-method default timeouts are sized for the typical case and adjusted
based on telemetry. `timeout` is a per-call argument for now; if patterns
emerge that warrant blanket policy, a separate call-policy mechanism can be
added later.

When more control is needed, such as waiting on multiple acceptable statuses,
polling without issuing a mutation, or applying a custom wait policy, combine
`wait=False` with the low-level primitive:

```python
from datetime import timedelta

from vercel.unstable.sandbox import SandboxStatus

await sandbox.stop(wait=False)
await sandbox.wait_for_status(
    SandboxStatus.STOPPED,
    timeout=timedelta(seconds=60),
)
```

`wait_for_status(...)` accepts one or more statuses:

```python
await sandbox.wait_for_status(
    {SandboxStatus.READY, SandboxStatus.STOPPED},
)
```

It understands the sandbox state machine. If the sandbox reaches a terminal
state outside the accepted target set, it raises a typed error. Timeout is
distinct from terminal failure:

```python
SandboxError
SandboxWaitError
SandboxTerminalStateError
SandboxWaitTimeoutError
```

`SandboxError` extends the SDK-wide `VercelError`. Polling refreshes the
same `Sandbox` object in place; on success, timeout, or terminal failure,
the object reflects the latest fetched state.

### Snapshots

Snapshots are behavioral service handles.

```python
from datetime import timedelta

from vercel.unstable.sandbox import SandboxCreateParams

sandbox = await session.sandbox.create(
    SandboxCreateParams(timeout=timedelta(minutes=20)),
    timeout=timedelta(seconds=90),
)

snapshot = await sandbox.snapshot()
await snapshot.refresh()
await snapshot.delete()
```

`snapshot()` is a verb on `Sandbox` because it creates a snapshot of that
sandbox. It returns a `Snapshot` handle and applies any updated sandbox state
returned by the API to the same `Sandbox` instance.

`sandbox.list_snapshots()` returns a paginated async collection of instantiated
`Snapshot` handles scoped to the sandbox. It should be backed by a real backend
filter such as `source_sandbox_id`, not by SDK-side filtering over project-level
pages.

```python
async for snapshot in sandbox.list_snapshots():
    await snapshot.delete()
```

Service-level snapshot operations also belong on the Sandbox accessor and return
handles:

```python
snapshot = await session.sandbox.get_snapshot("snap_123")

async for snapshot in session.sandbox.list_snapshots():
    ...
```

Creating a sandbox from a snapshot remains a Sandbox service operation. The
snapshot handle can be used as source data in params:

```python
snapshot = await session.sandbox.get_snapshot("snap_123")
sandbox = await session.sandbox.create(SandboxCreateParams(source=snapshot))
```

`Snapshot` supports cached properties, `refresh()`, and `delete()`. `delete()`
updates the same handle with returned state and returns `self`.

### Cancellation

Cancellation does not roll back server-side state. If a method raises
`CancelledError` because its `timeout` expired or because an outer cancel scope
cancelled it, the underlying API call may have already succeeded. The cancelled
method raises and returns no normal result.

Sandbox creation is a named operation. After a cancelled
`create(SandboxCreateParams(name=...))`, the sandbox may already exist
server-side, and a retry with the same name may return a conflict. Callers
requiring at-least-once semantics should reconcile via
`session.sandbox.get(name=...)` before retrying. A future `get_or_create(...)`
upsert flow is planned to absorb this case.

## Observability

The SDK uses widely available Python primitives for logs and traces.

Logging goes through the stdlib `logging` module under the namespace
`vercel.unstable.*`, with a logger per domain package
(`vercel.unstable.sandbox`, `vercel.unstable.auth`, etc.). The SDK does not
install handlers; users configure logging through stdlib mechanisms.

Tracing uses OpenTelemetry. Spans are emitted lazily, only when
`opentelemetry-api` is importable in the user's environment. Span names and
attributes follow OTel semantic conventions where they exist.

Span attributes describe the operation (sandbox name, session id, region,
runtime, and similar fields), not SDK plumbing. Session identity is
intentionally not part of span data: two sessions that differ only in pool size
or lazy state are
indistinguishable in traces. If a use case for opt-in span scoping by
deployment unit emerges, it can be added later without changing existing
instrumentation.

## Errors

All exceptions extend `VercelError`, exported from `vercel.unstable`.
Domain-specific errors (`SandboxError`, `AuthError`, and their subclasses)
inherit from it directly; there is no intermediate service/resource layer.
Catching `VercelError` reliably scopes a handler to SDK errors:

```python
from vercel import unstable as vercel
from vercel.unstable.sandbox import SandboxCreateParams

try:
    sandbox = await vercel.sandbox.create(SandboxCreateParams())
except vercel.VercelError:
    ...
```

Expected error families include:

```python
OperationTimeoutError
UnloadedStateError
UnsupportedCredentialsError
```

Names may move under domain packages as implementation proves the hierarchy, but
the behaviors are part of the design.

## Rationale And Rejected Alternatives

We are following the boto3 idea that a session is the home for shared runtime
configuration and lifecycle, and that simple usage can rely on defaults. We are
not copying boto3's stringly typed `client("s3")` / `resource("s3")` factories
because strict typing, autocomplete, and nominal service boundaries matter more
for this SDK.

We chose named accessors such as `session.sandbox` and
`session.resources.projects` instead of generic factories.

We chose singular `sandbox` for the top-level accessor because the product name
is Vercel Sandbox, even though plural collection grammar would normally be
preferable.

We chose explicit service methods such as `sandbox.list_snapshots()` instead of
child accessors such as `sandbox.snapshots`. `sandbox.snapshots` reads like a
loaded property, while listing snapshots performs a scoped query and returns
behavioral handles.

We chose context-local default sessions over per-operation default sessions
because returned domain objects need a coherent runtime context for later
actions such as `refresh()`, `stop()`, commands, logs, and snapshots.

We chose accessor-owned auth policy instead of session-wide credentials because
credential requirements depend on service boundaries.

We chose `Options` instead of `Config` because settings may include dependency
objects such as credential providers. Runtime state remains outside options.

We chose `Params` objects for method domain data and reserved method keyword
arguments for behavior. This avoids collisions such as sandbox creation
`params.timeout` versus method `timeout`.

We chose `wait=True` as the lifecycle default with `wait=False` to opt out,
instead of opt-in waiting through a `WAIT` sentinel and a `WaitOptions` object.
The 80% case is to wait, and a `bool`-shaped argument keeps the call site
readable. `wait_for_status(...)` is the primitive for callers who need anything
more than a single target status.

## Non-goals

- Do not document a stable public API.
- Do not redesign every existing package immediately.
- Do not preserve compatibility with current static/classmethod-heavy APIs
  inside `unstable`.
- Do not expose `RequestClient` or transport internals yet.
- Do not introduce a public transport injection interface until needed.
- Do not settle generated resource implementation details beyond the high-level
  stance on inputs and outputs.
- Do not add SDK API without a concrete use case.

## Open Questions

- Exact default credential lookup order for each service/resource boundary.
- Exact `SessionOptions` and `SandboxOptions` field sets.
- Exact params class inventory for Sandbox and Snapshot operations.
- The full generated resource namespace shape under `resources`.
- Whether context-local defaults need sync equivalents.
- Whether any auth or wait concepts should later be promoted to the top-level
  facade after more than one service uses them unchanged.
- Whether `get_or_create(...)` and analogous upserts for named resources land
  alongside the first sandbox release or as follow-ups.
- Whether method-level `timeout` includes streamed iterator consumption after a
  method returns an iterator, or only work performed before the method returns.
