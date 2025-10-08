from __future__ import annotations

import asyncio

from vercel.functions.cache import get_cache


async def main() -> None:
    cache = get_cache()

    raise Exception("test")

    await cache.set("post:1", {"title": "Hello"}, {"tags": ["post", "feed"]})
    await cache.set("post:2", {"title": "World"}, {"tags": ["post"]})

    assert await cache.get("post:1") is not None
    assert await cache.get("post:2") is not None

    await cache.expire_tag("feed")

    # post:1 should be gone, post:2 remains
    assert await cache.get("post:1") is None
    assert await cache.get("post:2") is not None
    print("Tag invalidation ok")


if __name__ == "__main__":
    asyncio.run(main())


