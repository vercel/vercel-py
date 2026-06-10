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
- SDK sessions expose generic runtime capabilities and cache services requested
  by domain-owned constructors.
- Service configuration lives on the SDK session through
  `service_options=[...]`.
- Each service has a default options object, so simple calls work without a
  configured session; scoped options replace that default for the service.
- Endpoint calls use direct keyword arguments. Sandbox listing accepts a
  constrained query value because separate filter/order keywords would express
  unsupported backend query combinations.
- Service methods avoid complex retry, timeout, and polling policy. Compose
  those outside the call.
- Remote resource handles are stable mutable views bound to the SDK session
  that created them. Their state properties are read-only and expose the most
  recent state successfully observed through that handle.
- Receiver-targeting operations update and return the existing handle.
  Separately acquired handles are independent and may remain stale until they
  are refreshed or used in an operation that returns updated state.
- Handles can issue requests only while their originating SDK session is open.
- Async is primary. Sync support mirrors each domain under that domain package,
  for example `vercel.unstable.sandbox.sync`.
- Internally, Sandbox endpoint responses become immutable domain state before
  shared async orchestration runs. Runtime-bound async and sync clients turn
  that state into their matching public handles.

## Primary Async Shape

```python
from datetime import timedelta
import asyncio

from vercel import unstable as vercel
from vercel.unstable import sandbox
from vercel.unstable.sandbox import (
    GitSource,
    SandboxQueryByName,
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
        snapshot_expiration=timedelta(days=1),
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
        await ready_sandbox.run_process("python", ["--version"])

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
            snapshot_expiration=timedelta(days=1),
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
            await inner.run_process("python", ["--version"])

        # `inner` was created by the nested SDK session. That session is closed,
        # so later calls through `inner` raise VercelSessionClosedError.
        # Reacquire through an open session to make more requests.
        inner = await sandbox.get_sandbox(name="inner-preview")

    # `preview` is also bound to a closed scoped SDK session.
    preview = await sandbox.get_sandbox(name="preview")

    # A context-managed sandbox is destroyed on exit.
    async with sandbox.create_sandbox(
        runtime="python3.13",
        name="scratch",
    ) as scratch:
        await scratch.run_process("python", ["--version"])

    # Cleanup was requested remotely and its successful response was applied
    # to `scratch`. The retained handle remains request-capable while its
    # SDK session is open; later responses are determined by the API.

    persistent = await sandbox.create_sandbox(
        runtime="python3.13",
        name="persistent",
    )

    # Resolves the current session and stops it on exit.
    # The successful stop response updates `runtime_session`. It is still
    # request-capable while its SDK session remains open; the API decides
    # whether the resource can be used.
    async with persistent.session() as runtime_session:
        await runtime_session.run_process("python", ["--version"])

    # The sandbox is now stopped. Resolving its session again resumes it from
    # the latest snapshot and returns the replacement current session.
    resumed_session = await persistent.session()
    await resumed_session.run_process("python", ["--version"])

    sandboxes = [
        item
        async for item in sandbox.query_sandboxes(
            page_size=20,
            query=SandboxQueryByName(
                name_prefix="preview-",
                tag=TagFilter(key="env", value="preview"),
            ),
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

## Internal Service Composition

SDK sessions are domain-neutral runtime providers. They validate open state,
resolve service options, cache one instance per requested service
implementation type, lend the mode-specific shared transport, and provide an
async-shaped sleep operation. The sync sleep capability blocks directly so
async-first service logic remains compatible with `iter_coroutine()`.

Service packages own domain-specific assembly: options defaults, endpoint
origins, credentials, endpoint clients, and orchestration wiring. Public async
and sync domain facades resolve their active mode-specific session and pass it
to the domain-owned constructor.

Add another unstable service with this pattern:

1. Define `<Domain>ServiceOptions(ServiceOptions)` in the domain package.
2. Implement an async-first neutral `<Domain>Service`.
3. Add `get_<domain>_service(session)` beside that service.
4. Construct domain endpoint clients from generic session capabilities.
5. Cache the result through `session.get_or_create_service(...)`.
6. Resolve active async or sync sessions only in public domain facades.

A central registry or descriptor abstraction is unnecessary until multiple
services demonstrate additional repeated structure.

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

# Resolves the sandbox's current session and stops it on exit.
sandbox_ = await sandbox.create_sandbox(runtime="python3.13")
async with sandbox_.session() as runtime_session:
    ...
```

Context manager exit awaits cleanup. Cleanup failures raise
`SandboxCleanupError`, whose `cause` points at the underlying failure. Context
managers express ownership of a remote cleanup request, not reliable knowledge
of whether a resource can answer a later API request.

For a sandbox context manager, observe deletion by fetching its unique name
with `resume=False` and receiving a not-found API response. For a runtime
session context manager, the retained session handle reflects the successful
stop response as `SandboxStatus.STOPPED`.

Explicit cleanup requests termination or deletion through the API:

```python
sandbox_ = await sandbox.create_sandbox(runtime="python3.13")
runtime_session = await sandbox_.session()
await runtime_session.stop()
await sandbox_.destroy()
```

Sandbox identity methods such as `session()`, `run_process(...)`,
`create_process(...)`, `update(...)`, `list_sessions(...)`,
`extend_execution_time_limit(...)`, `update_network_policy(...)`, and
`destroy()` live on `Sandbox`. Session-scoped methods such as
`run_process(...)`, `create_process(...)`, `refresh()`,
`get_process(...)`, `query_processes(...)`, `snapshot(...)`,
`extend_execution_time_limit(...)`, `update_network_policy(...)`, and `stop()`
live on `SandboxRuntimeSession`. `run_process()` uses one streaming request,
forwards stdout and stderr to the current Python process by default, waits for
completion, and returns a frozen `CompletedProcess`. It accepts the familiar
`subprocess.PIPE`, `subprocess.DEVNULL`, and `stderr=subprocess.STDOUT` routing
sentinels, writable text streams, `capture_output=True`, and `check=True`.
Only streams routed to `PIPE` are populated on `CompletedProcess`; other output
fields are `None`. `check=True` raises `subprocess.CalledProcessError` with the
same captured values.

A sandbox has at most one active current runtime session. `Sandbox.session()`
returns that session while it remains usable. If it has stopped or otherwise
cannot accept commands, the backend resumes the sandbox from its latest
snapshot, records the new session as `currentSessionId`, and returns that
replacement. Older sessions remain available through session history APIs but
are not additional active sessions. Concurrent resume requests converge on the
same replacement session.

The session handle returned by `Sandbox.session()` is independent from the
`Sandbox` handle. Resuming can change the backend's current session without
refreshing state cached by an existing `Sandbox` handle. Fetch the sandbox
again to obtain a handle bound to the replacement current session.

`create_process()` instead returns a live `Process` handle for explicit
lifecycle and output consumption. `Process` exposes `stdout` and `stderr`
`TextReader` instances, plus `wait()`, `send_signal()`, `terminate()`,
`kill()`, `refresh()`, and `communicate()`. Low-level endpoint composition,
response binding, and polling stay inside the internal Sandbox service layer.

Workspace filesystem operations live on the `fs` capability of either handle:

```python
await sandbox_.fs.mkdir("workspace")
await sandbox_.fs.write_text("workspace/input.txt", "hello\n")
content = await sandbox_.fs.read_text("workspace/input.txt")
entries = await sandbox_.fs.listdir("workspace")
```

`Sandbox.fs` resolves the runtime session ID recorded by its owning `Sandbox`
handle on every operation. It follows a replacement current session only after
new sandbox state has been applied to that handle. `SandboxRuntimeSession.fs`
remains bound to that specific historical session identity. The async
`SandboxFilesystem` and sync `SyncSandboxFilesystem` expose `mkdir`,
`read_bytes`, `read_text`, `write_bytes`, `write_text`, `batch`, `exists`,
`is_file`, `is_dir`, `listdir`, `remove`, and `rename`. A batch stages files
synchronously inside its context and submits one tarball on clean exit.
`listdir()` returns sorted `DirectoryEntry(path=..., kind=...)` values, where
`kind` is `file`, `directory`, `symlink`, or `other`.

`create_process(...)` accepts the `subprocess.Popen` output sentinels.
`stdout` accepts `subprocess.PIPE` (default) or `subprocess.DEVNULL`; `stderr`
additionally accepts `subprocess.STDOUT`, which merges stderr output into the
`stdout` reader in arrival order. `Process.stdout` and `Process.stderr` are
`TextReader | None`: a stream routed to `DEVNULL` — or merged via
`stderr=subprocess.STDOUT` — has no reader and its attribute is `None`,
matching `Popen`. As in `Popen`, `stderr=subprocess.STDOUT` follows stdout's
destination, so combining it with `stdout=subprocess.DEVNULL` discards both.
`communicate()` returns `(stdout, stderr)` where each value is `None` when
that stream has no reader. When neither stream has a reader the combined-log
request is never issued.

`stdout` and `stderr` are one-shot readers backed by one shared lazy
combined-log request. Closing one preserves the other; transport failure
breaks both — including cancelling a pending read, so drain readers in a
dedicated task rather than wrapping reads in `asyncio.wait_for`. A structured
in-band stream failure raises `SandboxStreamError`, which exposes the server
`code` and uses the server message as its exception message. Direct reader
iteration and `receive()` yield logical lines while `read()` and `readline()`
share one cursor.

`run_process(...)` and `create_process(...)` accept `kill_after` as a numeric
duration in seconds or a `timedelta`. The sandbox enforces this per-command
limit from exec time and kills the process with `SIGKILL` when it expires,
including commands started with `create_process(...)`. This is distinct from
the sandbox session's `execution_time_limit` and from local waiting policy.

All unstable Sandbox duration inputs follow the same convention: numeric
values are seconds, while `timedelta` values are accepted directly. This
includes execution limits, snapshot expiration and retention, timeout
extension, snapshot creation expiration, and `kill_after`.

`Sandbox.update(...)` changes named sandbox defaults for future sessions, such
as runtime, resources, ports, tags, snapshot expiration, and persistence.
Snapshot expiration values accept `0` for no expiration or values from one day
through ten years inclusive. Pass `snapshot_retention=None` explicitly to
`Sandbox.update(...)` to clear an existing retention policy; omit the keyword
to preserve the policy.
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
requests remote deletion; later calls through a retained handle are still
sent while its SDK session is open.

## Handle Validity

Handles are permanently bound to their originating SDK session. Once that SDK
session closes, any later request through its sandbox, runtime-session,
command, snapshot, or captured service object raises
`VercelSessionClosedError`. Reacquire through an open SDK session:

```python
from vercel import unstable as vercel
from vercel.unstable import sandbox


async with vercel.session(service_options=[...]):
    preview = await sandbox.create_sandbox(runtime="python3.13", name="preview")

# `preview` is bound to the closed scoped SDK session.
preview = await sandbox.get_sandbox(name="preview")
```

Process output is never cached. Reading from a process reader after session
closure raises `VercelSessionClosedError`; already buffered reader text
remains available until that reader reaches data requiring another request
read.

Context-managed cleanup, explicit `destroy()` / `stop()` / `delete()`, and
snapshot responses reporting a stopped runtime do not locally revoke handles.
Remote existence and terminal state are server-authoritative: a retained
handle may make another request and receive the ordinary API success or error
response.

`SandboxInvalidHandleError` is reserved for unattached or mode-invalid handle
objects. Closed originating sessions raise `VercelSessionClosedError`.

## Sync Mirror

The async API is the primary API. Sync support mirrors the service shape inside
each domain package.

```python
from itertools import islice

from vercel.unstable.sandbox import sync as sandbox


with sandbox.create_sandbox(
    runtime="python3.13",
    name="sync-preview",
) as sandbox_:
    sandbox_.fs.write_text("hello.py", "print('hello from sync sandbox')\n")
    command = sandbox_.create_process("python", ["hello.py"])
    for line in command.stdout:
        print(line, end="")
    assert command.wait() == 0

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
waiting, cleanup, and session-bound handle rules as the async API. Sync command
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
failures raise `SandboxCleanupError`; unattached or mode-invalid handles raise
`SandboxInvalidHandleError`; valid error records received while streaming
command logs raise `SandboxStreamError`; and requests through closed SDK
sessions raise `VercelSessionClosedError`.

Filesystem capability errors inherit from `SandboxFilesystemError`. Native
filesystem requests raise `SandboxPathNotFoundError` only when structured API
error data proves a missing path; other endpoint failures remain
`SandboxApiError`. `exists`, `is_file`, and `is_dir` return `False` for normal
non-matches. Failed command-backed `listdir`, `remove`, and `rename` calls
raise `SandboxFilesystemCommandError`, which carries the operation, input
paths, exit code, stdout, and stderr.
