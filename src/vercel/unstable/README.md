# `vercel.unstable`

This package is a maintainer-facing workbench for experimental Vercel Python SDK
APIs. The surface may change without migration support.

The implemented unstable Sandbox surface is centered on creating V2 sandboxes.
It currently exposes:

- `vercel.unstable.Session` and `vercel.unstable.SyncSession`
- the default-bound async accessor `vercel.unstable.sandbox`
- session-bound `session.sandbox` accessors
- `Sandbox`, `SyncSandbox`, sandbox-domain `Session`, `SandboxRoute`, and
  `SandboxStatus`
- `SandboxCreateParams`, `SandboxOptions`, and Sandbox error types

Snapshot handles, lifecycle methods such as `stop()` or `refresh()`, command
execution, and generated management resources are not implemented here.

## Async Usage

Simple async usage can go through the default session:

```python
from datetime import timedelta

from vercel import unstable as vercel
from vercel.unstable.sandbox import SandboxCreateParams

sandbox = await vercel.sandbox.create(
    SandboxCreateParams(runtime="python3.13", name="preview-db"),
    wait=True,
    timeout=timedelta(seconds=90),
)
```

Configured applications can own an explicit session:

```python
from datetime import timedelta

from vercel.unstable import Session, SessionOptions
from vercel.unstable.auth import AccessTokenCredentials, StaticCredentialProvider
from vercel.unstable.sandbox import SandboxCreateParams, SandboxOptions

async with Session(options=SessionOptions(client_pool_size=10)) as session:
    sandbox = await session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_id",
                    team_id="team_id",
                )
            )
        )
    ).create(
        SandboxCreateParams(runtime="python3.13"),
        timeout=timedelta(seconds=90),
    )
```

The default async facade can be rebound for a context:

```python
from vercel import unstable as vercel
from vercel.unstable import Session
from vercel.unstable.sandbox import SandboxCreateParams

async with Session() as session:
    async with vercel.use_session(session):
        sandbox = await vercel.sandbox.create(SandboxCreateParams())
```

`vercel.sandbox.with_options(...)` returns another default-bound proxy without
initializing the default session. I/O methods resolve the effective session when
they run. `vercel.sandbox.with_session(session)` returns a concrete
session-bound accessor.

## Sync Usage

Sync usage is explicit:

```python
from datetime import timedelta

from vercel.unstable import SyncSession
from vercel.unstable.sandbox import SandboxCreateParams

with SyncSession() as session:
    sandbox = session.sandbox.create(
        SandboxCreateParams(runtime="python3.13"),
        wait=True,
        timeout=timedelta(seconds=90),
    )
```

There is no default sync facade.

## Create Params

Sandbox API payload data lives in `SandboxCreateParams`. Method keyword
arguments are SDK behavior:

```python
from datetime import timedelta

from vercel.unstable.sandbox import SandboxCreateParams

params = SandboxCreateParams(
    runtime="python3.13",
    name="preview-db",
    ports=[3000],
    timeout=timedelta(minutes=20),
    persistent=True,
    tags=["ci"],
)

sandbox = await session.sandbox.create(
    params,
    wait=True,
    timeout=timedelta(seconds=90),
)
```

In this example, `params.timeout` is the remote sandbox lifetime. The method
`timeout` bounds the SDK create operation, including polling when `wait=True`.

## Returned Data

`create(...)` returns a `Sandbox` or `SyncSandbox` handle populated from the V2
response:

```python
print(sandbox.name)
print(sandbox.persistent)
print(sandbox.current_snapshot_id)

if sandbox.current_session is not None:
    print(sandbox.current_session.id)
    print(sandbox.current_session.status)

for route in sandbox.routes:
    print(route.url, route.subdomain, route.port)
```

`sandbox.routes` is always a list. If the API returns no routes, it is `[]`.

## Waiting

Pass `wait=True` to poll until the current session reaches
`SandboxStatus.RUNNING`.

Terminal create states raise `SandboxTerminalStateError`:

- `FAILED`
- `ABORTED`
- `STOPPED`
- `STOPPING`

If the operation timeout expires, `SandboxOperationTimeoutError` is raised.

## Errors

All unstable SDK exceptions inherit from `vercel.unstable.VercelError`.
Sandbox-specific errors inherit from `SandboxError`:

```python
from vercel import unstable as vercel
from vercel.unstable.sandbox import SandboxAPIError, SandboxCreateParams

try:
    sandbox = await vercel.sandbox.create(SandboxCreateParams())
except SandboxAPIError as error:
    print(error.status_code)
    print(error.data)
except vercel.VercelError:
    raise
```

`SandboxAPIError.retry_after` is populated for numeric `retry-after` response
headers.
