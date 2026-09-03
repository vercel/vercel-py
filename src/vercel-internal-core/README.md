# Internal Core

`vercel._internal.core` provides the shared runtime used by Vercel Python service
packages. It is installed as their dependency and is not intended for direct
installation or direct end-user imports.

The distribution contributes service-neutral namespace portions. Import the
public session context from `vercel.api`, shared exceptions from
`vercel.errors`, and Function invocation context from
`vercel.functions.context`:

```python
from vercel.api import session
from vercel.errors import VercelError
from vercel.functions.context import get_wait_until
```
