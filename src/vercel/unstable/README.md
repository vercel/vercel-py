# `vercel.unstable`

`vercel.unstable` is the design space for the next Vercel Python SDK API.
Everything in this namespace may change without migration support.

This document describes the current unstable SDK shape. It is not a stable API
contract.

## Design Summary

- Most callers use module-level service functions such as
  `sandbox.create_sandbox(...)`.
- Async and sync service functions resolve separate SDK session types from
  `ContextVar` state, falling back to their mode's process default session.
- There is no public "configure default session" API. Use the default session
  as-is or create a scoped override with `vercel.session(...)`.
- `async with vercel.session(...)` owns an async transport and
  `httpx.AsyncClient` pool; `with vercel.session(...)` owns their sync
  equivalents.
- Endpoint clients own service base URLs and send absolute URLs through the
  session transport, so multiple services and origins can share one pool.
- Service configuration lives on the SDK session through
  `service_options=[...]`.
- Each service has a default options object, so simple calls work without a
  configured session; scoped options replace that default for the service.
- Endpoint calls use direct keyword arguments, not public `*Param` dataclasses.
- Service methods avoid complex retry, timeout, and polling policy. Compose
  those outside the call.
- Handles are valid only while their owning SDK session and resource context
  are alive.
- Async is primary. Sync support mirrors each domain under that domain package,
  for example `vercel.unstable.sandbox.sync`.

## Primary Async Shape

```python
from datetime import timedelta
import asyncio

from vercel import unstable as vercel
from vercel.unstable import sandbox
from vercel.unstable.sandbox import (
    GitSource,
    SandboxResources,
    SandboxServiceOptions,
    SnapshotRetention,
    TagFilter,
)


async def main() -> None:
    # Most callers import a service module and call module-level functions.
    # Calls use the active SDK session, or the process default session when no
    # scoped override is active.
    default_sandbox = await sandbox.create_sandbox(
        runtime="python3.13",
        name="default-session-preview",
        snapshot_expiration=timedelta(minutes=20),
    )

    # Service methods do not expose complex timeout/retry policy.
    # Compose operation policy outside the SDK call.
    async with asyncio.timeout(90):
        ready_sandbox = await sandbox.create_sandbox(
            runtime="python3.13",
            name="ready-or-terminal",
        )

        # create_sandbox always waits for ready or terminal.
        # There is no wait=True / wait=False argument.
        await ready_sandbox.run_command("python", ["--version"])

    # `async with` creates an async scoped SDK session. Its client factory
    # must return httpx.AsyncClient; the session validates before first use.
    async with vercel.session(
        httpx_client_factory=client_factory,
        service_options=[
            SandboxServiceOptions(base_url="https://sandbox-proxy.example.com"),
        ],
    ):
        # Endpoint inputs are keyword arguments, not *Param dataclasses.
        preview = await sandbox.create_sandbox(
            runtime="python3.13",
            name="preview",
            source=GitSource(url="https://github.com/vercel/vercel-py"),
            execution_time_limit=timedelta(minutes=5),
            resources=SandboxResources(vcpus=2, memory=4096),
            # Platform-side retention for sandbox-owned snapshots/state.
            # This is not an SDK operation timeout.
            snapshot_expiration=timedelta(minutes=20),
            snapshot_retention=SnapshotRetention(
                count=3,
                expiration=timedelta(days=1),
            ),
        )

        # Nested sessions inherit unspecified settings from the active session.
        async with vercel.session(
            service_options=[
                # Replaces the inherited SandboxServiceOptions as a whole.
                # Option objects replace by concrete type; fields do not merge.
                SandboxServiceOptions(base_url="https://inner-proxy.example.com"),
            ],
        ):
            inner = await sandbox.create_sandbox(
                runtime="python3.13",
                name="inner-preview",
            )
            await inner.run_command("python", ["--version"])

        # `inner` was created by the nested SDK session.
        # When that session exited, the Python handle was invalidated.
        # The remote sandbox may still exist; reacquire a fresh handle.
        inner = await sandbox.get_sandbox(name="inner-preview")

    # `preview` was created by the outer scoped SDK session.
    # That session has exited, so this handle is invalid too.
    preview = await sandbox.get_sandbox(name="preview")

    # A context-managed sandbox is destroyed on exit.
    async with sandbox.create_sandbox(
        runtime="python3.13",
        name="scratch",
    ) as scratch:
        await scratch.run_command("python", ["--version"])

    # `scratch` has been destroyed and its handle is invalid.

    persistent = await sandbox.create_sandbox(
        runtime="python3.13",
        name="persistent",
    )

    # A context-managed sandbox runtime session is destroyed on exit.
    # The sandbox survives; the runtime session handle does not.
    async with persistent.session() as runtime_session:
        await runtime_session.run_command("python", ["--version"])

    # If you want a sandbox or runtime session to survive, do not use its
    # context manager.
    surviving_session = await persistent.session()
    await surviving_session.run_command("python", ["--version"])

    sandboxes = [
        item
        async for item in sandbox.query_sandboxes(
            page_size=20,
            tags=[TagFilter(key="env", value="preview")]
        )
    ]

    first_five = []
    async for item in sandbox.query_sandboxes(page_size=10):
        first_five.append(item)
        if len(first_five) == 5:
            break

    snapshot = await persistent.snapshot()
    restored = await sandbox.create_sandbox(
        runtime="python3.13",
        name="restored-from-snapshot",
        source=sandbox.SnapshotSource(snapshot_id=snapshot.id),
    )
    restored_snapshots = await restored.list_snapshots(page_size=10)
    project_snapshots = [item async for item in sandbox.query_snapshots(page_size=10)]
    fetched_snapshot = await sandbox.get_snapshot(snapshot_id=snapshot.id)
    await fetched_snapshot.delete()
```

## SDK Sessions And Transports

There are two session runtime types:

- `SdkSession` backs async service facades and owns one lazy async transport
  wrapping an `httpx.AsyncClient` pool.
- `SyncSdkSession` backs sync service facades and owns one lazy sync transport
  wrapping an `httpx.Client` pool.

The public construction syntax is shared, but the context-manager form selects
the mode:

```python
import httpx

from vercel import unstable as vercel


def async_client_factory() -> httpx.AsyncClient:
    return httpx.AsyncClient()


def sync_client_factory() -> httpx.Client:
    return httpx.Client()


async with vercel.session(httpx_client_factory=async_client_factory):
    ...

with vercel.session(httpx_client_factory=sync_client_factory):
    ...
```

The expected factory return type follows `with` versus `async with`. Type
overloads may catch a mismatch when the factory is explicitly annotated, but
runtime validation is still required for untyped factories and `Any`. When the
session first materializes its lazy transport, it checks that an async scope
received an `httpx.AsyncClient` and a sync scope received an `httpx.Client`,
then wraps that client in the appropriate session-owned transport. Mismatches
raise a session configuration error before a request is sent.

An explicit scope is mode-bound. Calling a sync facade inside an active async
scope, or an async facade inside an active sync scope, raises a session error.
Entering `with vercel.session(...)` below an active async scope, or entering
`async with vercel.session(...)` below an active sync scope, is rejected for
the same reason. Same-mode nesting inherits an omitted
`httpx_client_factory`; passing `httpx_client_factory=None` explicitly resets
the nested session to SDK-default HTTPX client construction.
Outside an explicit scope, async and sync facades use independent default
sessions.

Session transports and HTTP pools do not carry service origins. For example,
`SandboxServiceOptions.base_url` configures the Sandbox endpoint client, which
constructs absolute request URLs and uses the session's shared transport. This
permits one session to reuse its pool for multiple services, or for one
service to contact additional origins such as token, usage, or JWKS endpoints.

The transport is intentionally below service configuration. It preserves
common HTTP request mechanics, including generic bearer-header injection, and
can bridge sync execution through shared service logic, but it does not own
endpoint base URLs or select service credentials. Supplying
`httpx_client_factory` customizes the pool wrapped by the session transport;
it does not replace the transport contract.

## Service Options

Service endpoint configuration belongs to the SDK session. Transport and HTTP
pool construction are session-level concerns rather than service options.

```python
from vercel import unstable as vercel
from vercel.unstable.sandbox import SandboxServiceOptions


async with vercel.session(
    service_options=[
        SandboxServiceOptions(base_url="https://sandbox-proxy.example.com"),
    ],
):
    ...
```

Service option rules:

- every service option class inherits from a common marker base
- the session stores options in a map keyed by concrete type
- a single `service_options` list may contain at most one option per concrete
  type
- nested sessions of the same mode inherit options from the active session
- nested sessions replace option objects by concrete type
- option fields do not merge

```python
async with vercel.session(
    service_options=[
        SandboxServiceOptions(base_url="https://outer.example.com"),
    ],
):
    async with vercel.session(
        service_options=[
            # Replaces the whole inherited SandboxServiceOptions object.
            SandboxServiceOptions(base_url="https://inner.example.com"),

            # A second SandboxServiceOptions in this same list would be invalid.
            # SandboxServiceOptions(base_url="https://other.example.com"),
        ],
    ):
        ...
```

## Sandbox Lifecycle

`sandbox.create_sandbox(...)` always waits until the sandbox reaches a ready
state or a terminal state. Terminal states raise typed sandbox errors. Operation
time limits are composed by the caller.

```python
async with asyncio.timeout(90):
    sandbox_ = await sandbox.create_sandbox(runtime="python3.13")
```

Context manager syntax means scoped remote ownership:

```python
# Destroys the sandbox on exit.
async with sandbox.create_sandbox(runtime="python3.13") as sandbox_:
    ...

# Destroys the sandbox runtime session on exit.
sandbox_ = await sandbox.create_sandbox(runtime="python3.13")
async with sandbox_.session() as runtime_session:
    ...
```

Context manager exit awaits cleanup. Cleanup failures raise
`SandboxCleanupError`, whose `cause` points at the underlying failure. To keep a
sandbox or sandbox runtime session around, do not use its context manager.

Explicit cleanup uses the same ownership rules:

```python
sandbox_ = await sandbox.create_sandbox(runtime="python3.13")
runtime_session = await sandbox_.session()
await runtime_session.stop()
await sandbox_.destroy()
```

Sandbox identity methods such as `session()`, `run_command(...)`,
`start_command(...)`, `update(...)`, `list_sessions(...)`,
`extend_execution_time_limit(...)`, `update_network_policy(...)`, and
`destroy()` live on `Sandbox`. Session-scoped methods such as
`run_command(...)`, `start_command(...)`, `refresh()`,
`get_command(...)`, `query_commands(...)`, `mkdir(...)`, `read_file(...)`,
`read_text(...)`, `write_files(...)`, `snapshot(...)`,
`extend_execution_time_limit(...)`, `update_network_policy(...)`, and `stop()`
live on `SandboxRuntimeSession`. Command handles expose `wait()`, `kill()`,
`logs()`, `output()`, `stdout()`, and `stderr()`. Low-level endpoint
composition, response binding, and polling stay inside the internal Sandbox
service layer.

`Sandbox.update(...)` changes named sandbox defaults for future sessions, such
as runtime, resources, ports, tags, snapshot expiration, and persistence.
`SandboxRuntimeSession.update_network_policy(...)` and
`SandboxRuntimeSession.extend_execution_time_limit(...)` change the currently
running session. Use `sandbox.query_sessions(...)` for project-level session
listing and `Sandbox.list_sessions(...)` for sessions belonging to one named
sandbox.

`Sandbox.snapshot(...)` and `SandboxRuntimeSession.snapshot(...)` create a
filesystem snapshot from the current session. The returned `Snapshot` can be
used with `SnapshotSource(snapshot_id=...)` to create another sandbox.
`sandbox.query_snapshots(...)` lists project snapshots,
`Sandbox.list_snapshots(...)` filters by sandbox name, and
`sandbox.get_snapshot(...)` fetches one snapshot by ID. `Snapshot.delete()`
invalidates the deleted snapshot handle after the API confirms deletion.

## Handle Validity

Handles carry an internal alive marker.

A handle is valid only while:

- its owning SDK session is alive
- its own context-managed remote resource has not exited
- explicit cleanup such as `destroy()` or `stop()` has not succeeded

Handles created inside `vercel.session(...)` are invalidated when that session
context exits. The remote resource may still exist, but the old Python handle is
not usable. Reacquire a new handle through an active SDK session:

```python
from vercel import unstable as vercel
from vercel.unstable import sandbox


async with vercel.session(service_options=[...]):
    preview = await sandbox.create_sandbox(runtime="python3.13", name="preview")

# `preview` is invalid as a Python handle.
preview = await sandbox.get_sandbox(name="preview")
```

Snapshot creation may stop the current runtime session. When `snapshot()`
returns a non-running session from the API, the SDK invalidates the old
runtime-session handle. The sandbox identity handle remains usable for
sandbox-scoped operations such as `list_snapshots(...)`, `update(...)`, and
`destroy()`.

Invalid handle use raises `SandboxInvalidHandleError`.

## Sync Mirror

The async API is the primary API. Sync support mirrors the service shape inside
each domain package.

```python
from itertools import islice

from vercel.unstable.sandbox import WriteFile
from vercel.unstable.sandbox import sync as sandbox


with sandbox.create_sandbox(
    runtime="python3.13",
    name="sync-preview",
) as sandbox_:
    sandbox_.write_files(
        [
            WriteFile(
                path="hello.py",
                content="print('hello from sync sandbox')\n",
            )
        ]
    )
    command = sandbox_.start_command("python", ["hello.py"])
    for event in command.logs():
        print(event.data, end="")
    finished = command.wait()
    assert finished.exit_code == 0

    sessions = sandbox_.list_sessions(page_size=10)

    first_five = list(islice(sandbox.query_sandboxes(page_size=10), 5))
```

Scoped service options work the same way in sync code:

```python
from vercel import unstable as vercel
from vercel.unstable.sandbox import SandboxServiceOptions
from vercel.unstable.sandbox import sync as sandbox


with vercel.session(
    httpx_client_factory=sync_client_factory,
    service_options=[SandboxServiceOptions(base_url="https://sandbox-proxy.example.com")],
):
    sandbox.create_sandbox(
        runtime="python3.13",
        name="sync-preview",
    )
```

The sync mirror resolves `SyncSdkSession` and follows the same service option,
waiting, cleanup, and handle invalidation rules as the async API. Sync command
log streaming is exposed as a normal Python iterator.

## Error Model

All unstable SDK exceptions inherit from `vercel.unstable.VercelError`.

Session errors inherit from `VercelSessionError`. Domain-specific Sandbox
errors inherit from `SandboxError`.
Mode mismatches and wrong HTTPX client factory results use the existing
`VercelSessionError` type; they do not introduce an additional public error.

Sandbox terminal states raise `SandboxTerminalStateError`. Sandbox v2 API
failures raise `SandboxApiError`, which exposes `status_code`, `data`, and
`code` when the API returns an `{ "error": { "code": ... } }` envelope.
Malformed successful API responses raise `SandboxResponseError`. Cleanup
failures raise `SandboxCleanupError`, and invalid handle use raises
`SandboxInvalidHandleError`.
