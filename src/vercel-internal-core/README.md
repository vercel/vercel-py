# Internal Core

`vercel._internal.core` provides the shared runtime used by Vercel Python service
packages. It is installed as their dependency and is not intended for direct
installation or direct end-user imports.

The distribution contributes service-neutral namespace portions. Import the
public session context from `vercel.api` and shared exceptions from
`vercel.errors`:

```python
from vercel.api import session
from vercel.errors import VercelError
```

## Legacy HTTPX client migration

HTTPX2 is the default HTTP implementation and the only HTTPX family installed
by the SDK. As a temporary runtime-only migration path, applications that
explicitly install legacy `httpx` may return a complete `httpx.Client` or
`httpx.AsyncClient` from `session(httpx_client_factory=...)`. Static annotations
remain HTTPX2-based, so type-checked applications need a targeted `cast(Any, ...)`.

Legacy transport objects must remain inside a legacy client; an
`httpx.HTTPTransport` cannot be mounted in an `httpx2.Client`.
