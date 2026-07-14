Remember the last live Runtime Cache client process-wide and fall back to it
on threads that have no request context, and make `strict=True` raise
`RuntimeCacheError` when no cache is available instead of silently degrading
to the in-memory fallback. On Vercel the cache configuration arrives with the
request context, so background worker threads (such as embedded task-queue
workers storing results) previously wrote to per-instance memory that readers
could never see. `prime_runtime_cache()` lets integrations capture the
request's cache while its context is still visible.
