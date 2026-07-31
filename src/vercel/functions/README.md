# Functions

`vercel.functions` collects convenience exports commonly used inside Vercel
Functions.

```python
from vercel.functions import (
    AsyncRuntimeCache,
    geolocation,
    get_env,
    ip_address,
    set_headers,
    wait_until,
)


async def handler(request):
    set_headers(request.headers)
    wait_until(record_request_analytics(request))

    env = get_env()
    cache = AsyncRuntimeCache(namespace="api")
    await cache.set("last_region", env.VERCEL_REGION, {"ttl": 60})

    return {
        "ip": ip_address(request),
        "geo": geolocation(request),
        "region": env.VERCEL_REGION,
    }
```

Exports include environment helpers from `vercel.env`, header and geolocation
helpers from `vercel.headers`, cache clients from `vercel.cache`, and
`wait_until()` for work that should finish after the response is sent.

`wait_until()` is not a durable task queue. Its work must finish within the
Function's configured maximum duration, and it is not retried if the
invocation terminates. It accepts awaitables only; run synchronous work with
`wait_until(asyncio.to_thread(func))`.
