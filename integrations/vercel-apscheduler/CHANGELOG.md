# Changelog

## 0.1.0 - 2026-08-07

### Features

- Add the durable Redis driver for running APScheduler schedules through delayed Vercel Queue messages. (#242)
- Add Redis-backed APScheduler subscribers for Vercel Queues. The integration patches `scheduler.start()`, `scheduler.pause()`, and `scheduler.resume()` with durable, deployment-scoped lifecycle transitions and atomic single-chain fencing. Paused occurrences are skipped on resume, and interrupted successor publication is repaired on retry. Production schedules activate on the first request, and opted-in previews stop after a durable idle timeout. Jobs that do not choose a `misfire_grace_time` run their occurrences whenever the wake arrives: the stock one-second grace assumes in-process wakeup precision that queue delivery cannot meet. (#238)
- Add a Vercel Runtime Cache backend and use it by default when Redis is not configured, so schedulers run with zero infrastructure. Jobs stay defined in code; the cache document only coordinates the chain (generation, start and wake bookkeeping, lifecycle flags). Because cache entries are evictable and per-region, the queue messages remain the authority: an evicted document is rebuilt from the arriving wake, idempotency keys still fence duplicate starts, and pause/resume flags additionally ride the start topic. Scheduler identity comes from the builder-assigned subscriber id, with a declared-subscriber lookup for web processes. Under `vercel dev` the backend falls back to a per-process in-memory cache and activates on the first request like production, using a stable deployment id derived from the project directory. (#245)

### Bug Fixes

- Fix the Runtime Cache backend to work under `vc dev`. (#253)
