# Vercel Sandbox Python SDK

Create and manage Vercel Sandboxes with synchronous and asynchronous APIs.

```python
import subprocess

from vercel import sandbox
from vercel.api import session

async with session():
    async with sandbox.create_sandbox() as instance:
        process = await instance.run_process("echo", ["hello"], capture_output=True)
        print(process.stdout)

        # Pass input to run_process, or keep stdin open with stdin=subprocess.PIPE.
        result = await instance.run_process("sort", input="b\na\n", capture_output=True)
        server = await instance.create_process("cat", stdin=subprocess.PIPE)
        await server.stdin.send(b"ping\n")
        print(await server.stdout.readline())
        print(await server.communicate())  # Close stdin, read the rest, and wait.
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

Process APIs follow `subprocess`. Asynchronously, `process.stdin` is an
`anyio.abc.ByteSendStream`, so it works with `TextSendStream` under asyncio or
Trio. Synchronously it is a writable `io.RawIOBase`; wrap it in
`io.TextIOWrapper` for text. For a long-running stdio server, see
[`examples/sandbox_10_mcp_everything.py`](examples/sandbox_10_mcp_everything.py).

## Public data models

Sandbox value objects use Pydantic v2 `BaseModel`. This includes network
policies and their readback summaries, `SnapshotExpiration`, `DriveMount`,
`SandboxMount`, `DirectoryEntry`, `CompletedProcess`, `SandboxCredentials`,
service options, routes, and snapshot-retention state. Internal data records
also use Pydantic, including state attached to `SandboxResponseError.data`.
The sync API shares the same value types and has its own service-options and
user-handle models.

```python
from vercel.sandbox import DirectoryEntry, NetworkPolicy

entry = DirectoryEntry.model_validate({"path": "app.py", "kind": "file"})
print(entry.model_dump())  # {"path": "app.py", "kind": "file"}
print(entry.model_dump_json())

policy = NetworkPolicy.model_validate(
    {"mode": "custom", "allow": {"example.com": [{"response": {"status_code": 403}}]}}
)
```

These models validate their fields, reject unknown fields, and preserve
frozen-field behavior. Network-policy mappings are also defensively copied
and immutable. Invalid values and field assignments raise
`pydantic.ValidationError`. Existing helpers such as
`NetworkPolicy.allow_all()`, `NetworkPolicy.custom()`,
`NetworkPolicyMatcher.exact()`, and `CompletedProcess.check_returncode()`
remain available. `SnapshotExpiration(value)`, `DriveMount(drive, mode)`,
`NetworkPolicyMatcher(kind, value)`, and positional credentials construction
are retained; use keyword arguments for other model constructors.

Use `model_dump()` and `model_dump_json()` in place of `dataclasses.asdict()`.
For replacement values, use `model_validate()` to validate a modified
dictionary. A model's serialization describes its Python fields; SDK calls
still perform the API-specific conversions, including duration units and
network-policy field names.

`SandboxUser` and `SyncSandboxUser` remain handles obtained from `as_user()`
or `create_user()`. Their dumps contain `username` and the currently resolved
`home_dir`; filesystem and service bindings stay private. Dumps do not
recreate a live user handle. Service options retain their callable
`credentials_factory`; exclude that field when exporting options as JSON.

Internal state, pagination, deferred request parameters, upload metadata, and
terminal frames follow the same model convention. Recovery attempts and process
output targets use mutable models because they coordinate live operations.
Models containing streams, SDK handles, events, or exceptions preserve those
objects by reference; their dumps do not recreate live runtime resources.

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

Use `await sandbox.get_drive(name_or_id="cache")` to look up an existing Drive
by project-local name or Drive ID without creating it. A missing Drive raises
`SandboxApiError` with `status_code == 404`. The async and sync clients and
`vercel.sandbox.sync.get_drive(...)` expose the same lookup.

Fork a Drive's committed data into an independent Drive with a new name:

```python
cache = await sandbox.get_drive(name_or_id="cache")
forked = await cache.fork(name="cache-fork")
async with sandbox.create_sandbox(region=forked.region, mounts={"/cache": forked}) as box:
    print(forked.parent_drive_id, forked.root_drive_id)
await forked.delete()
```

If you already know the source name, fork it directly without a lookup:

```python
forked = await sandbox.fork_drive(source="cache", name="cache-fork")
```

`vercel.sandbox.sync.fork_drive(source=..., name=...)` is the synchronous
counterpart. Both standalone clients also expose `fork_drive(...)`.

Forks inherit the source Drive's data, region, and maximum size without changing
its handle. `parent_drive_id` identifies the immediate source and `root_drive_id`
identifies the original Drive; both are `None` when absent from the API response.
`SyncDrive.fork(name=...)` provides the same operation synchronously. Async calls
use Python's normal task cancellation.

`SandboxApiError` preserves the API's status, code, and message:
`404 not_found` means the source is missing, `409 conflict` means the new name
already exists, and `409 drive_not_initialized` means the source has no committed
data. Mount the source read-write and stop its session to commit data before
forking. See [the Drive fork example](examples/sandbox_10_drive_fork.py).

All Drive operations use the project resolved by the configured Sandbox credentials.
They do not accept a `project_id` override.

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

## Interactive PTY sessions

`open_interactive(...)` starts a process attached to a pseudo-terminal and
scopes the connection to a context manager. Terminal I/O is a standard byte
stream on `session.stream`, so it can be handed to anything that consumes one;
the session itself carries the controls that are not part of the stream:

```python
import asyncio

from vercel import sandbox


async def main() -> None:
    async with sandbox.create_sandbox() as box:
        async with box.open_interactive("/bin/bash", cols=100, rows=30) as pty:
            await pty.stream.send(b"tty\n")
            async for chunk in pty.stream:
                print(chunk.decode(errors="replace"), end="")
                break

            await pty.resize(120, 40)
            await pty.stream.send(b"exit 0\n")
            print(await pty.wait())


asyncio.run(main())
```

The asynchronous `stream` is an `anyio.abc.ByteStream`; the synchronous one is
an `io.RawIOBase`, so each matches the conventions of its own ecosystem:

```python
from vercel.sandbox import sync as sandbox

with sandbox.create_sandbox() as box:
    with box.open_interactive("/bin/bash") as pty:
        pty.stream.write(b"tty\n")
        print(pty.stream.read(1024))
        pty.stream.write(b"exit 0\n")
        print(pty.wait(timeout=30))
```

`TERM` defaults to `xterm-256color` so full-screen programs render correctly;
override it through `env`. The stream ends when the process exits, and
`wait()` returns its exit code. If the connection ends before the process
reports its exit, even cleanly, reading the stream and `wait()` both raise
(`anyio.BrokenResourceError` asynchronously, `ConnectionError`
synchronously) rather than looking like a normal end. Output that arrives while waiting is kept for the stream, up
to a bounded buffer; with more pending than that, `wait()` blocks until the
stream is read, so sequential code should read to the end of the stream
before waiting.

The session is bound to one connection: it cannot be reattached after the
context exits, and the stream carries raw terminal bytes including escape
sequences. Programs that repaint the screen need a terminal emulator on the
consuming side to interpret those sequences.

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

## Isolated users and shared capabilities

Create a Linux account with a private home directory, or select an existing
account without creating it:

```python
alice = await instance.create_user("alice")
bob = instance.as_user("bob")  # Synchronous selection, including on the async API

await alice.fs.write_text("secret.txt", "private", mode=0o600)
result = await alice.run_process("whoami", capture_output=True, check=True)
assert result.stdout == "alice\n"
assert alice.home_dir == "/home/alice"
```

`SandboxUser` and `SyncSandboxUser` are immutable handles exposing `username`,
`home_dir`, `run_process()`, `create_process()`, and `.fs`. Their command and
filesystem options match the session APIs. Usernames must match
`[a-z_][a-z0-9_-]*` and contain at most 32 characters; invalid names fail
immediately. `create_user()` raises `SandboxUserAlreadyExistsError` if the account
already exists. Catch it and pass its `username` to `as_user()` to select that
account. Other provisioning failures, including occupied home paths, raise
`SandboxError`. Successful creation provisions
`/bin/bash` and `/home/<username>` owned by the user and the sandbox default
group with mode `0770`, and returns a handle.
It rejects existing home paths, including symlinks, before creating an account.
`as_user()` performs no I/O. It resolves the actual existing account's home
on first use, so `home_dir` is initially `None`, except for `root` (`/root`).
A missing account fails on first use. Selecting an account never creates it.

Commands preserve arguments without shell interpolation, initialize the
account's UID, primary GID and supplementary groups, and set `HOME`, `USER`,
and `LOGNAME`. Commands and relative filesystem paths default to the user's
home; explicit relative `cwd` values also resolve against that home. Caller
`env` entries override the defaults. `sudo=True` runs one command as root,
with root's environment defaults, while retaining the handle's default cwd.
Use `as_user("root")` for a persistent root identity.

Commands, reads, listing, renaming, and removal run as the selected account
and honor Linux permissions. Another user cannot read a private home.
Uploads (`write_bytes`, `write_text`, batches, and writable `fs.open` handles)
and `mkdir` are trusted SDK orchestration: they use the native HTTP filesystem
API as the sandbox default account, then run privileged ownership commands.
Upload contents never appear in command arguments. Successful uploads belong
to the selected user and its actual primary group. Directories created by
`mkdir`, and ancestors below the selected user's home, belong to that user
and the sandbox default group with mode `0770` so later HTTP uploads can reach
them. File access modes come from the upload request.

These SDK write operations can access paths that the selected user's commands
cannot, including another user's home when the HTTP account has access. Do not
expose them as a filesystem permission boundary to untrusted callers. The HTTP
account still needs access to the destination; existing accounts selected with
`as_user()` retain their home ownership and permissions, so uploads may require
explicit preparation. Failed uploads or ownership changes can leave partial
files, and ownership is assigned only after the HTTP upload succeeds.

User handles obtained from a sandbox follow its lazy resume; handles obtained
from a runtime session stay pinned to that session. No lifecycle, configuration,
or process lookup/listing methods are exposed by user handles. Group management
is not included. Custom images need Bash with coprocess support, getent,
useradd, setpriv with `--pdeathsig` support (util-linux), GNU find, and coreutils
(including env's `--default-signal`), with setpriv, env, and sleep available
under `/usr/bin`. User file reads use base64 command output to preserve binary
contents; uploads stream through the native file endpoint. Command time limits
and `KILL` clean up the command's process group inside the selected account so
child processes release their output streams promptly. Catchable signals such
as `INT`, `TERM`, and `USR1` allow command handlers to finish and report their
actual exit status. As an intentional exception to the mirrored process API,
user processes (including `sudo=True`) reject `STOP`, `TSTP`, `TTIN`, and `TTOU`
with `NotImplementedError` before sending an API request. The launcher cannot
safely pause the actual command's group; rejection leaves the command running
and its process metadata intact. `CONT` remains accepted.

The public `SandboxExecution` and `SandboxFilesystemOperations` Protocols
support generic consumers of sandboxes, runtime sessions, and user handles:

```python
from vercel.sandbox import SandboxExecution

async def run_agent(executor: SandboxExecution) -> str:
    await executor.fs.write_text("input.txt", "hello")
    result = await executor.run_process("cat", ["input.txt"], capture_output=True, check=True)
    return result.stdout or ""

await run_agent(instance)
await run_agent(await instance.session())
await run_agent(alice)
```

`SyncSandboxExecution` and `SyncSandboxFilesystemOperations` provide the same
contracts for synchronous code and are exported from both `vercel.sandbox`
and `vercel.sandbox.sync`. These structural Protocols cover command execution
and filesystem operations only. See the executable
[`sandbox_10_users.py`](examples/sandbox_10_users.py) example for generic
consumers, command execution, and isolation using both APIs. The design follows
Vercel's [multi-agent sandbox guide](https://vercel.com/docs/sandbox/concepts/multi-agent).
