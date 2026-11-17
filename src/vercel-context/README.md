# Vercel Context

Python helpers for code running inside a Vercel Function: request context,
lifecycle hooks, and Vercel system environment variables.

```sh
pip install vercel-context
```

## Modules

- `vercel.functions` - convenience exports for function code, including
  `wait_until()` and `get_deadline()`
- `vercel.env` - Vercel system environment variable parsing

## Usage

```python
from vercel.env import get_env
from vercel.functions import get_deadline

env = get_env()
deadline = get_deadline()
print(env.VERCEL_REGION, deadline)
```
