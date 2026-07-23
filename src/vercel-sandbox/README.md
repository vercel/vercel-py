# Vercel Sandbox Python SDK

Create and manage Vercel Sandboxes with synchronous and asynchronous APIs.

```python
from vercel import sandbox, session

async with session():
    async with sandbox.create_sandbox() as instance:
        process = await instance.run_process("echo", ["hello"], capture_output=True)
        print(process.stdout)
```

The package can be installed independently with `pip install vercel-sandbox`.

The same promoted API is available synchronously:

```python
from vercel import session
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
