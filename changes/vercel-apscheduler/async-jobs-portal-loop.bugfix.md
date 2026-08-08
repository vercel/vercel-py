Run coroutine jobs on a persistent portal event loop so they execute from
the queue worker instead of being rejected, and so loop-bound resources
(async clients, locks) stay valid across runs.
