from __future__ import annotations

import asyncio
import time

import vercel
from vercel.stable.options import CacheSetOptions


def sync_demo() -> None:
    vc = vercel.create_sync_client()
    cache = vc.get_cache(namespace="sync-demo")

    try:
        key = "example:user:42"
        cache.delete(key)
        assert cache.get(key) is None

        cache.set(key, {"name": "Ada"}, CacheSetOptions(ttl=2, tags=("user",)))
        got = cache.get(key)
        assert isinstance(got, dict) and got.get("name") == "Ada"
        print("[sync:get_cache] set/get ok")

        time.sleep(3)
        assert cache.get(key) is None
        print("[sync:get_cache] TTL expiry ok")

        cache.set("post:1", {"title": "Hello"}, CacheSetOptions(tags=("post", "feed")))
        cache.set("post:2", {"title": "World"}, CacheSetOptions(tags=("post",)))
        assert cache.get("post:1") is not None
        assert cache.get("post:2") is not None
        cache.expire_tag("feed")
        assert cache.get("post:1") is None
        assert cache.get("post:2") is not None
        print("[sync:get_cache] tag invalidation ok")
    finally:
        vc.close()


async def async_demo() -> None:
    vc = vercel.create_async_client()
    cache = vc.get_cache(namespace="async-demo")

    try:
        key = "example:user:99"
        await cache.delete(key)
        assert await cache.get(key) is None

        await cache.set(key, {"name": "Bob"}, CacheSetOptions(ttl=1, tags=("user",)))
        got = await cache.get(key)
        assert isinstance(got, dict) and got.get("name") == "Bob"
        print("[async:get_cache] set/get ok")

        await asyncio.sleep(2)
        assert await cache.get(key) is None
        print("[async:get_cache] TTL expiry ok")

        await cache.set("post:1", {"title": "Hello"}, CacheSetOptions(tags=("post", "feed")))
        await cache.set("post:2", {"title": "World"}, CacheSetOptions(tags=("post",)))
        assert await cache.get("post:1") is not None
        assert await cache.get("post:2") is not None
        await cache.expire_tag("feed")
        assert await cache.get("post:1") is None
        assert await cache.get("post:2") is not None
        print("[async:get_cache] tag invalidation ok")
    finally:
        await vc.aclose()


if __name__ == "__main__":
    asyncio.run(async_demo())
    sync_demo()
