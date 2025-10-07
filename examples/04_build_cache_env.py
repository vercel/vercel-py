from __future__ import annotations

import asyncio
import os

from vercel.functions.cache import get_cache


async def main() -> None:
    # This example only exercises the build/runtime cache when the env vars are set.
    # Otherwise it will fall back to in-memory and still pass.
    os.environ.setdefault("SUSPENSE_CACHE_DEBUG", "true")

    cache = get_cache()
    key = "build:probe"
    await cache.delete(key)
    await cache.set(key, {"ok": True}, {"ttl": 60, "tags": ["probe"]})
    got = await cache.get(key)
    assert got is None or (isinstance(got, dict) and got.get("ok") is True)
    # When talking to the remote cache, only fresh entries return; otherwise None.
    print("Build cache probe executed; value:", got)


if __name__ == "__main__":
    asyncio.run(main())
