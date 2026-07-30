# Changelog

## 0.7.1 - 2026-07-16

- No changes.

## 0.7.0 - 2026-07-13

### Features

- Initial release. `vercel-celery` integration with Kombu transports for Vercel Queue auto, poll, and (#172)
- push delivery modes; Celery result backend backed by Vercel Runtime Cache. (#159) (#172)

### Bug Fixes

- Settle push deliveries within the delivering request: drain the embedded (#175)
- worker's deferred ACKs/rejects before responding, and wait in-request for the (#175)
- handoff lock, consumer readiness, and prefetch capacity (new (#175)
- `push_handoff_wait_seconds` transport option) instead of bouncing deliveries (#175)
- with `RetryAfter`. On Vercel the worker loop thread is suspended between (#175)
- requests, so deferred ACKs never ran, leases silently expired, and every (#175)
- message was redelivered and re-executed on a five-minute cycle. (#175)
