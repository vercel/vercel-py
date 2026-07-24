# Internal Core

`vercel.internal.core` provides the shared runtime used by Vercel Python service
packages. It is installed as their dependency and is not intended for direct
installation or direct end-user imports.

The distribution contributes service-neutral namespace portions. Import the
public session context from `vercel.api` and shared exceptions from
`vercel.errors`:

```python
from vercel.api import session
from vercel.errors import VercelError
```
