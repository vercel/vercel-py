from __future__ import annotations

import asyncio
import time

from vercel.cache import get_cache


async def main() -> None:
    cache = get_cache()

    key = "example:user:42"
    await cache.delete(key)

    v = await cache.get(key)
    assert v is None

    await cache.set(key, {"name": "Ada"}, {"ttl": 2, "tags": ["user"]})
    got = await cache.get(key)
    assert isinstance(got, dict) and got.get("name") == "Ada"
    print("Cache set/get ok")

    # TTL expiry check
    time.sleep(3)
    expired = await cache.get(key)
    assert expired is None
    print("TTL expiry ok")


if __name__ == "__main__":
    asyncio.run(main())
