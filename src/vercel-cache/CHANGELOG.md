# Changelog

## 0.7.1 - 2026-07-16

### Bug Fixes

- Remember the last live Runtime Cache client process-wide and fall back to it on threads that have no request context, and make `strict=True` raise `RuntimeCacheError` when no cache is available instead of silently degrading to the in-memory fallback. On Vercel the cache configuration arrives with the request context, so background worker threads (such as embedded task-queue workers storing results) previously wrote to per-instance memory that readers could never see. `prime_runtime_cache()` lets integrations capture the request's cache while its context is still visible. (#178)

## 0.7.0 - 2026-07-13

### Features

- Split runtime cache helpers into the standalone `vercel-cache` package. (#160) (#172)
- Add strict mode support for runtime cache operations. (#161) (#172)
