Add a Vercel Runtime Cache backend and use it by default when Redis is not
configured, so schedulers run with zero infrastructure. Jobs stay defined in
code; the cache document only coordinates the chain (generation, start and
wake bookkeeping, lifecycle flags). Because cache entries are evictable and
per-region, the queue messages remain the authority: an evicted document is
rebuilt from the arriving wake, idempotency keys still fence duplicate
starts, and pause/resume flags additionally ride the start topic. Scheduler
identity comes from the builder-assigned subscriber id, with a
declared-subscriber lookup for web processes. Under `vercel dev` the backend
falls back to a per-process in-memory cache and activates on the first
request like production, using a stable deployment id derived from the
project directory.
