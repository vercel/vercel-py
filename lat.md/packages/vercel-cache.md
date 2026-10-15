# vercel-cache

Runtime Cache clients with namespaced keys and tag invalidation, falling back to an in-memory cache wherever Runtime Cache is unavailable.

Lives in [[src/vercel-cache]] and is exposed as [[src/vercel-cache/vercel/cache/__init__.py|vercel.cache]], with an async entry point in [[src/vercel-cache/vercel/cache/aio.py|vercel.cache.aio]]. [[vercel-celery]], [[vercel-dramatiq]], [[vercel-django-tasks]], and [[vercel-apscheduler]] build on it.

## Backend resolution

Every operation resolves its backend lazily, so one cache object behaves correctly inside a request, in background work, and locally.

[[src/vercel-cache/vercel/cache/runtime_cache.py#resolve_cache]] picks, in order:

1. A cache injected into the current request context by the runtime.
2. A build cache client created from `RUNTIME_CACHE_ENDPOINT` and `RUNTIME_CACHE_HEADERS`.
3. The last live client seen in this process, so worker threads outside a request still reach Runtime Cache.
4. A process-wide in-memory cache, with a one-time warning.

`RUNTIME_CACHE_DISABLE_BUILD_CACHE=true` forces the in-memory cache. Strict mode raises `RuntimeCacheError` instead of falling back. The framework integrations construct their caches with `strict=True`, because silently storing results in process memory would lose them between invocations. [[src/vercel-cache/vercel/cache/runtime_cache.py#prime_runtime_cache]] records the request's client early so later background work can reuse it.

## Keys and namespaces

Keys are hashed and optionally namespaced before they reach any backend, using the same scheme as the TypeScript SDK.

[[src/vercel-cache/vercel/cache/runtime_cache.py#get_cache]] accepts a custom `key_hash_function`, a `namespace`, and a `namespace_separator`. The default hash in [[src/vercel-cache/vercel/cache/utils.py#default_key_hash_function]] mirrors the TypeScript djb2 variant, and namespaced keys take the form `namespace$hash`.

## Tag invalidation

Entries carry tags, and purges operate on tags through the purge API the runtime provides for the current request.

[[src/vercel-cache/vercel/cache/purge.py#invalidate_by_tag]] marks tagged entries stale. [[src/vercel-cache/vercel/cache/purge.py#dangerously_delete_by_tag]] deletes them, optionally with a revalidation deadline. Both are no-ops when no purge API is registered, so the same code runs locally.

## Request context

The runtime hands per-request services to the SDK through context variables rather than globals.

[[src/vercel-cache/vercel/cache/context.py#set_context]] stores the request's cache clients, purge API, `waitUntil` callback, and headers. The headers go to the shared [[vercel-headers]] store. [[vercel#Function helpers]] reads `wait_until` from the same context.
