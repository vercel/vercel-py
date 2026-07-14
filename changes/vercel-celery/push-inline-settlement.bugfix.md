Settle push deliveries within the delivering request: drain the embedded
worker's deferred ACKs/rejects before responding, and wait in-request for the
handoff lock, consumer readiness, and prefetch capacity (new
`push_handoff_wait_seconds` transport option) instead of bouncing deliveries
with `RetryAfter`. On Vercel the worker loop thread is suspended between
requests, so deferred ACKs never ran, leases silently expired, and every
message was redelivered and re-executed on a five-minute cycle.
