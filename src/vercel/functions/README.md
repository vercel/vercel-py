# Functions

`vercel.functions` collects convenience exports commonly used inside Vercel
Functions.

```python
from vercel.functions import AsyncRuntimeCache, geolocation, get_env, ip_address, set_headers


async def handler(request):
    set_headers(request.headers)

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
helpers from `vercel.headers`, and cache clients from `vercel.cache`.
