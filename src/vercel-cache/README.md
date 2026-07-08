# Cache

`vercel.cache` exposes runtime cache helpers for Vercel Python applications.

Use `get_cache()` for synchronous code and `vercel.cache.aio.get_cache()` for
async code. When runtime cache environment variables are unavailable, cache
operations fall back to an in-memory cache.
