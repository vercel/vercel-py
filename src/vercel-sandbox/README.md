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
