# vercel-sdk

Python SDK for Vercel.


## Installation

```bash
pip install vercel-sdk
```

## Requirements

- Python 3.9+

## Usage

### Headers and request context

```python
from vercel.headers import geolocation, ip_address

# In a framework handler, pass request.headers
city_info = geolocation(request)
client_ip = ip_address(request.headers)
```

### Runtime Cache

The SDK talks to Vercel’s Runtime Cache when the following env vars are present; otherwise it falls back to an in-memory cache.

- `RUNTIME_CACHE_ENDPOINT`: base URL of the runtime cache API (e.g. https://cache.vercel.com/...)  
- `RUNTIME_CACHE_HEADERS`: JSON object of headers to send (e.g. '{"authorization": "Bearer <token>"}')
- Optional: `SUSPENSE_CACHE_DEBUG=true` to log fallback behavior

```python
import asyncio
from vercel.cache import get_cache

async def main():
    cache = get_cache(namespace="demo")

    await cache.delete("greeting")
    await cache.set("greeting", {"hello": "world"}, {"ttl": 60, "tags": ["demo"]})
    value = await cache.get("greeting")  # dict or None
    await cache.expire_tag("demo")        # invalidate by tag

asyncio.run(main())
```

### OIDC (Vercel project tokens)

If the `VERCEL_OIDC_TOKEN` header is not present, the SDK will try to refresh a token using the local Vercel CLI session and your project configuration.

```python
import asyncio
from vercel.oidc import get_vercel_oidc_token, decode_oidc_payload

async def main():
    token = await get_vercel_oidc_token()
    payload = decode_oidc_payload(token)
    sub = payload.get("sub")

asyncio.run(main())
```

Notes:
- Requires a valid Vercel CLI login on the machine running the code for refresh.
- Project info is resolved from `.vercel/project.json`.

## Examples

See `examples/` for runnable scripts:
- `runtime_cache_basic.py`: set/get with fallback to in-memory
- `cache_tags.py`: tag-based invalidation
- `build_cache_env.py`: shows behavior when cache env vars are set
- `fastapi_oidc_plus_cache/`: small FastAPI demo wiring headers/oidc

## Development

- Lint/typecheck/tests:
```bash
uv pip install -e .[dev]
uv run ruff format --check && uv run ruff check . && uv run mypy src && uv run pytest -v
```
- CI runs lint, typecheck, examples as smoke tests, and builds wheels.
- Publishing: push a tag (`vX.Y.Z`) that matches `project.version` to publish via PyPI Trusted Publishing.

## License

MIT


