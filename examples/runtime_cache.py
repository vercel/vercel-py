from __future__ import annotations

import asyncio
import time

from vercel.cache import (
    get_cache,
    get_async_cache,
    RuntimeCache,
    AsyncRuntimeCache,
)


def sync_demo() -> None:
    # Helper-based sync client
    cache = get_cache(namespace="sync-demo")

    key = "example:user:42"
    cache.delete(key)
    assert cache.get(key) is None

    cache.set(key, {"name": "Ada"}, {"ttl": 2, "tags": ["user"]})
    got = cache.get(key)
    assert isinstance(got, dict) and got.get("name") == "Ada"
    print("[sync:get_cache] set/get ok")

    # TTL expiry check
    time.sleep(3)
    assert cache.get(key) is None
    print("[sync:get_cache] TTL expiry ok")

    # Tag invalidation
    cache.set("post:1", {"title": "Hello"}, {"tags": ["post", "feed"]})
    cache.set("post:2", {"title": "World"}, {"tags": ["post"]})
    assert cache.get("post:1") is not None
    assert cache.get("post:2") is not None
    cache.expire_tag("feed")
    assert cache.get("post:1") is None
    assert cache.get("post:2") is not None
    print("[sync:get_cache] tag invalidation ok")

    # Direct class-based sync client
    client = RuntimeCache(namespace="sync-client")
    client.set("k", 1, {"tags": ["t"]})
    assert client.get("k") == 1
    client.expire_tag("t")
    assert client.get("k") is None
    print("[sync:RuntimeCache] set/get + tag invalidation ok")


async def async_demo() -> None:
    # Helper-based async client
    cache = get_async_cache(namespace="async-demo")

    key = "example:user:99"
    await cache.delete(key)
    assert await cache.get(key) is None

    await cache.set(key, {"name": "Bob"}, {"ttl": 1, "tags": ["user"]})
    got = await cache.get(key)
    assert isinstance(got, dict) and got.get("name") == "Bob"
    print("[async:get_async_cache] set/get ok")

    # TTL expiry check
    await asyncio.sleep(2)
    assert await cache.get(key) is None
    print("[async:get_async_cache] TTL expiry ok")

    # Tag invalidation
    await cache.set("post:1", {"title": "Hello"}, {"tags": ["post", "feed"]})
    await cache.set("post:2", {"title": "World"}, {"tags": ["post"]})
    assert await cache.get("post:1") is not None
    assert await cache.get("post:2") is not None
    await cache.expire_tag("feed")
    assert await cache.get("post:1") is None
    assert await cache.get("post:2") is not None
    print("[async:get_async_cache] tag invalidation ok")

    # Direct class-based async client
    client = AsyncRuntimeCache(namespace="async-client")
    await client.set("k", 1, {"tags": ["t"]})
    assert await client.get("k") == 1
    await client.expire_tag("t")
    assert await client.get("k") is None
    print("[async:AsyncRuntimeCache] set/get + tag invalidation ok")


if __name__ == "__main__":
    asyncio.run(async_demo())
    sync_demo()
