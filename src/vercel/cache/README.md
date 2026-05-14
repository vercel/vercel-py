# Runtime Cache

`vercel.cache` provides Runtime Cache clients and invalidation helpers for
Vercel Functions.

## Async Client

```python
from vercel.cache import AsyncRuntimeCache


async def main() -> None:
    cache = AsyncRuntimeCache(namespace="products")
    await cache.set("featured", ["sku_1", "sku_2"], {"ttl": 300, "tags": ["catalog"]})
    featured = await cache.get("featured")
    await cache.expire_tag("catalog")
```

Use `RuntimeCache` or `vercel.cache.get_cache()` for synchronous code.
