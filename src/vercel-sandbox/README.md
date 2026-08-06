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

The same promoted API is available synchronously:

```python
from vercel.api import session
from vercel.sandbox import sync as sandbox

with session():
    with sandbox.create_sandbox() as instance:
        process = instance.run_process("echo", ["hello"], capture_output=True)
        print(process.stdout)
```

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
`vcr.vercel.com/team-slug/project-slug/my-repository:latest`. Use `image`
instead of `runtime`; the two options are mutually exclusive. For an
image-backed sandbox, `Sandbox.image` contains the returned image metadata and
`Sandbox.runtime` remains `None`.

Installing this package also provides the `vercel-sandbox` and `sandbox`
console commands. Both are aliases that delegate all arguments to `npx sandbox`;
they require Node.js with npm and `npx` installed. Node.js is not required when
using the Python API directly.

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
