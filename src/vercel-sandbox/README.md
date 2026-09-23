# Vercel Sandbox Python SDK

Create and manage Vercel Sandboxes with synchronous and asynchronous APIs.

```python
from vercel import sandbox
from vercel.api import session

async with session():
    async with sandbox.create_sandbox() as instance:
        process = await instance.run_process("echo", ["hello"], capture_output=True)
        print(process.stdout)
```

The package can be installed independently with `pip install vercel-sandbox`.

To attach a sandbox to a Secure Compute private network, pass its connect
network ID at creation:

```python
async with session():
    instance = await sandbox.create_sandbox(network_id="network_123")
    print(instance.network_id)
    await instance.update(network_id=None)  # Disconnect from Secure Compute
```

`network_id` is also accepted by `fork_sandbox()` and the synchronous APIs.

When no image is provided, the Sandbox API uses
`vercel/sandbox/universal:latest`.

The same promoted API is available synchronously:

```python
from vercel.api import session
from vercel.sandbox import sync as sandbox

with session():
    with sandbox.create_sandbox() as instance:
        process = instance.run_process("echo", ["hello"], capture_output=True)
        print(process.stdout)
```

## Standalone clients

Applications that use dependency injection can create a service-specific
client without configuring the ambient `vercel.api.session` context:

```python
from vercel.sandbox import SandboxClient, SandboxServiceOptions

client = SandboxClient.create(
    options=SandboxServiceOptions(region="iad1"),
)

try:
    async with client.create_sandbox() as instance:
        ...
finally:
    await client.aclose()
```

Client construction is synchronous and performs no I/O. The client can be
stored and shared by application components; the application that creates it
must call `aclose()` during shutdown. The synchronous API follows the same
ownership model:

```python
from vercel.sandbox.sync import SandboxServiceOptions, SyncSandboxClient

client = SyncSandboxClient.create(options=SandboxServiceOptions(region="iad1"))
try:
    with client.create_sandbox() as instance:
        ...
finally:
    client.close()
```

Standalone clients expose the same service operations as their module-level
counterparts. They do not read or modify the active SDK session.

## Custom images

Create a sandbox from a Vercel Container Registry (VCR) image with the
`image` keyword. The image reference is sent to the Sandbox API unchanged;
the backend validates access, resolves the image, and waits for it to be
ready.

```python
from vercel import sandbox
from vercel.api import session

async with session():
    async with sandbox.create_sandbox(image="my-repository:latest") as instance:
        result = await instance.run_process("my-command", capture_output=True)
        print(result.stdout)
        print(instance.image)  # The resolved digest-pinned image reference
```

The same option is available synchronously:

```python
from vercel.api import session
from vercel.sandbox import sync as sandbox

with session():
    with sandbox.create_sandbox(image="my-repository:latest") as instance:
        result = instance.run_process("my-command", capture_output=True)
        print(result.stdout)
        print(instance.image)  # The resolved digest-pinned image reference
```

Image references may be a bare repository (`my-repository`), a tagged image
(`my-repository:latest`), a digest-pinned image
(`my-repository@sha256:<digest>`), or a fully qualified VCR reference such as
`vcr.vercel.com/team-slug/project-slug/my-repository:latest`. The backend
resolves the selected image, and `Sandbox.image` contains the resolved image
reference.

Installing this package also provides the `vercel-sandbox` and `sandbox`
console commands. Both are aliases that delegate all arguments to `npx sandbox`;
they require Node.js with npm and `npx` installed. Node.js is not required when
using the Python API directly.

## Creating, forking, and restoring

Create a sandbox from a runtime, Git repository, tarball, or snapshot with
`create_sandbox(...)`. A snapshot source restores that snapshot's filesystem
into a new sandbox:

```python
from vercel import sandbox
from vercel.sandbox import SnapshotSource

restored = await sandbox.create_sandbox(
    name="restored-workspace",
    source=SnapshotSource(snapshot_id="snap_123"),
)
```

Use `fork_sandbox(...)` when the source is an existing named sandbox. The
server restores the fork from the source's current snapshot, or from its
runtime or image when no snapshot exists. It also copies the source's ports,
execution time limit, resources, image, persistence, network policy,
environment variables, tags, snapshot expiration, and snapshot retention.
Only pass values that should override the inherited configuration:

```python
forked = await sandbox.fork_sandbox(
    source_sandbox="production-agent",
    name="debug-agent",
    resources=sandbox.SandboxResources(vcpus=4, memory=8192),
    tags={"purpose": "debug"},
)
```

Both creation and fork operations can be used as async context managers for
automatic stop and destroy. The synchronous mirror uses the same arguments:

```python
from vercel.sandbox import sync as sandbox

with sandbox.fork_sandbox(source_sandbox="production-agent") as forked:
    result = forked.run_process("python", ["script.py"], capture_output=True)
```

## Sandbox Drives

Sandbox Drives are named persistent filesystems. A Drive belongs to one project
and one region. Create or retrieve a Drive, then mount it when you create a
sandbox:

```python
from pathlib import PurePosixPath

from vercel import sandbox
from vercel.sandbox import DriveMount

cache, _ = await sandbox.get_or_create_drive(
    name="cache",
    region="sfo1",
    max_size_bytes=10 * 1024**3,
)
async with sandbox.create_sandbox(
    region=cache.region,
    mounts={
        # Mount a read-write drive
        "/cache": cache,
        # You can mount an existing drive by name as a point-in-time read-only snapshot
        "/readonly": DriveMount("shared-source", mode="snapshot"),
        # You can also provide a PurePosixPath as the mount point
        PurePosixPath("/scratch"): "scratch",
    },
) as workspace:
    print(cache.id)
    print(workspace.mounts)

await cache.delete()
```

Drives must be unmounted from any running Sandbox before they are able to be
deleted. You can either call the `delete` method on the Drive handle, or use the
`delete_drive(name=...)` function to delete a drive by name if
you do not have a drive handle already.

By default, a `Drive` handle or Drive name mounts it read-write. Use
`drive.snapshot()` or `DriveMount(name, mode="snapshot")` for a point-in-time
read-only mount.

Forks cannot inherit the source sandbox's mounts. Pass a non-empty mapping to
attach Drives explicitly to the fork. A read-write Drive still attached to
another sandbox causes the API to return `409 drive_attached`.

Use `query_drives(...)` with `DriveQueryByCreatedAt`,
`DriveQueryByUpdatedAt`, or `DriveQueryByName` to list Drives in a project.
`get_or_create_sandbox(..., mounts=...)` uses `mounts` only when it creates or
recreates the sandbox. If the sandbox already exists, the call leaves its mounts
unchanged. Use `await box.update(mounts=...)` to replace the mount map for the
next session. Pass `None` to leave mounts unchanged or `{}` to remove all mounts.

The SDK canonicalizes repeated slashes, trailing slashes, and `.` components.
Paths must be absolute and non-overlapping. They cannot contain `..` or NUL
characters, target `/`, or exceed 256 characters after canonicalization. Each
Drive name may appear only once. Mounted sandboxes cannot use failover regions,
and each Drive must use the sandbox region. Drives default to `iad1` when
`region` is omitted.

## Session lifecycles

Sandbox-level process and filesystem operations resume a stopped sandbox
lazily. The original sandbox handle adopts the replacement current session:

```python
box = await sandbox.get_sandbox(name="workspace")
result = await box.run_process("python", ["script.py"])
```

Use `box.session()` when the session boundary should be explicit. Direct
acquisition leaves the acquired session running, while managed acquisition
stops exactly the session it yielded:

```python
active = await box.session()

async with box.session() as exact_session:
    await exact_session.run_process("python", ["script.py"])
```

The synchronous forms are `active = box.session()` and
`with box.session() as exact_session:`. Operations through an explicit session
remain pinned to its identity and never auto-resume. Operations through `box`
may adopt a replacement; that replacement is not stopped by an older managed
session scope.

Managed sandbox and session exit does not wait for concurrent operations.
Callers must join sandbox work before leaving a context when deterministic
cleanup is required.
