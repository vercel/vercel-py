# vercel-queue

Client for Vercel Queues: publish to topics, consume through push-triggered Functions or polling, and emulate the service locally.

Lives in [[src/vercel-queue]] and is exposed as [[src/vercel-queue/vercel/queue/__init__.py|vercel.queue]] (async) and [[src/vercel-queue/vercel/queue/sync.py|vercel.queue.sync]]. [[vercel-workflow]] and every framework integration build on it.

## Delivery model

Delivery is at least once, and a message stays invisible to other consumers only while its processing lease is alive.

A message is sent to a topic and delivered once per consumer group, so separate groups fan out while consumers in the same group compete. A received message is leased: until the lease expires or is extended, nobody else can claim it. It is then acknowledged, made visible again after a delay, or left to expire and be redelivered. Handlers must therefore be idempotent. `send` accepts an `idempotency_key` to deduplicate publishes, as well as `delay` and `retention`.

Messages can only be received in the region they were sent to. Push delivery handles this automatically; polling code must pin one region for both sending and receiving.

## Client and HTTP runtime

The queue client carries its own HTTP layer rather than the shared [[vercel-internal-core]] transports and sessions.

[[src/vercel-queue/vercel/queue/_internal/client.py#BaseQueueClient]] holds the business logic, and the async [[src/vercel-queue/vercel/queue/_internal/client.py#QueueClient]] and [[src/vercel-queue/vercel/queue/_internal/client_sync.py#QueueClient|sync QueueClient]] specialize it. The sync client runs the shared coroutines through the package's own [[src/vercel-queue/vercel/queue/_internal/asynctools.py#iter_coroutine]]. Requests go through [[src/vercel-queue/vercel/queue/_internal/http.py#BaseQueueRuntime]], which accepts any HTTPX-compatible client factory. It parses multipart receive responses and streams bodies itself, and borrows only the legacy `httpx` client detection from [[vercel-internal-core]].

Configuration resolves from arguments first, then the environment:

- [[src/vercel-queue/vercel/queue/_internal/config.py#resolve_region]] uses `VERCEL_REGION`.
- [[src/vercel-queue/vercel/queue/_internal/config.py#resolve_base_url]] defaults to `https://<region>.vercel-queue.com/`, and accepts a fixed URL, a `{region}` template, or a callable.
- [[src/vercel-queue/vercel/queue/_internal/config.py#resolve_token]] takes `VERCEL_QUEUE_TOKEN`, then the [[vercel-oidc]] token.
- [[src/vercel-queue/vercel/queue/_internal/config.py#resolve_deployment]] pins the current deployment from `VERCEL_DEPLOYMENT_ID`, unless the caller passes `ALL_DEPLOYMENTS` or an explicit ID.

## Subscriptions

`@subscribe` declares handlers in a process-wide registry, which both push delivery and the Vercel build read.

[[src/vercel-queue/vercel/queue/_internal/subscribers.py#subscribe]] records a [[src/vercel-queue/vercel/queue/_internal/subscribers.py#Subscription]] for a topic and consumer group, along with delivery tuning: `retry_after`, `initial_delay`, `max_concurrency`, and `max_attempts`. Topic patterns ending in `*` match prefixes, and a bare `*` is a catch-all. Overlapping registrations raise `DuplicateSubscriptionError`. Handlers are held by weak reference, so registering does not keep a function alive.

A project declares a subscriber entrypoint under `[[tool.vercel.subscribers]]` in `pyproject.toml`. At build time, Vercel imports that module, reads its registered subscriptions, and compiles them into a queue-triggered Function, so no `vercel.json` trigger configuration is needed. The framework integrations register their own queues in this same registry.

## Payload transports

The handler's type annotation, or a typed topic, selects how payloads are serialized, so typed messages need no extra configuration.

[[src/vercel-queue/vercel/queue/_internal/transports.py#receive_transport_for_annotation]] maps annotations to transports:

- Plain JSON values use raw JSON.
- Pydantic models and other structured types use JSON with validation on receive (the `typed` extra).
- `bytes` and `str` are buffered.
- `Iterable`/`AsyncIterable` of `bytes` or `str` are streamed, for large payloads.

[[src/vercel-queue/vercel/queue/_internal/types.py#Topic]] carries a payload type or an explicit transport, so senders and handlers can agree on custom serialization. A handler may instead take a [[src/vercel-queue/vercel/queue/_internal/types.py#Message]] to receive the payload together with its [[src/vercel-queue/vercel/queue/_internal/types.py#MessageMetadata]], such as the delivery count.

Topic names must match `[A-Za-z0-9_-]+`. [[src/vercel-queue/vercel/queue/_internal/names.py#sanitize_name]] reversibly escapes arbitrary names into a [[src/vercel-queue/vercel/queue/_internal/names.py#SanitizedName]]; consumer groups are always sanitized.

## Push delivery

On Vercel, the queue service calls the subscriber Function, and the SDK owns the delivery lifecycle around the user's handlers.

[[src/vercel-queue/vercel/queue/_internal/api_async.py#accept_and_handle]] parses the callback ([[src/vercel-queue/vercel/queue/_internal/push.py#parse_push_delivery]]), starts lease renewal, and dispatches every subscription matching the topic. When they all return, it acknowledges the message. If no subscription matches, it raises `UnhandledMessageError`.

Handlers steer the outcome with [[src/vercel-queue/vercel/queue/_internal/types.py#QueueDirective]] exceptions:

- Any other exception reports failure; the message is redelivered after the subscription's `retry_after`.
- [[src/vercel-queue/vercel/queue/_internal/types.py#RetryAfter]] stops dispatch and makes the message visible again after a chosen delay. The SDK performs that update before reporting success.
- [[src/vercel-queue/vercel/queue/_internal/types.py#Handoff]] stops dispatch and leaves the lease open for an external system to finish. [[vercel-celery]] and [[vercel-dramatiq]] use it after passing a delivery to their frameworks' own acknowledgement machinery.

## Lease renewal

Leases are extended in the background while handlers run, through one process-wide worker shared by sync and async callers.

[[src/vercel-queue/vercel/queue/_internal/lease.py#LeaseRenewal]] registers a message with a daemon thread running an AnyIO worker, which runs one renewal task per message. Starting a renewal waits for the worker to schedule it, but not for the first extension, so handler startup never waits on renewal network I/O.

Acknowledgement stops renewal without waiting and cancels any in-flight extension, because a late renewal after acknowledgement is harmless. The `RetryAfter` and `Handoff` paths wait for the stop instead, and a waited stop lets an in-flight extension finish rather than cancelling it: a request the service has already received cannot be retracted, so only its completion guarantees a late renewal cannot overwrite the explicit visibility change. An extension that outlasts the stop timeout is cancelled. Each renewal handle is single-use, and at most one extension per message is in flight.

## Polling

Off Vercel, or for self-hosted workers, consumers poll the service instead of being invoked.

[[src/vercel-queue/vercel/queue/_internal/api_async.py#poll]] makes one request for up to `limit` messages and yields deliveries; it never blocks waiting for new messages. Each [[src/vercel-queue/vercel/queue/_internal/client.py#Delivery]] is a context manager: entering starts lease renewal, a clean exit acknowledges, and an exception leaves the message unacknowledged for redelivery. [[src/vercel-queue/vercel/queue/_internal/api_async.py#poll_and_handle]] runs a loop that dispatches to a subscriber. The sync variant runs on a thread ([[src/vercel-queue/vercel/queue/_internal/polling.py#start_sync_polling_thread]]). Wildcard subscriptions cannot be polled without explicit concrete topics.

## Local emulation

An in-process emulation of the queue service exercises the same client, push, lease, and acknowledgement paths without a deployment.

[[src/vercel-queue/vercel/queue/_internal/embedded.py#embedded_queue_service]] runs [[src/vercel-queue/vercel/queue/_internal/embedded.py#EmbeddedQueueServer]] with a dispatcher that invokes subscribers through `accept_and_handle`, using synthetic push headers. Failures retry through lease expiry or visibility changes, just as they do in production. [[vercel-workflow]]'s local world is built on it.

The [[src/vercel-queue/vercel/queue/testing/pytest.py#embedded_queue_server]] fixture shares one server per test session and resets it between tests. The `devserver` extra serves the same emulation over HTTP (`python -m vercel.queue.devserver`) for cross-process and cross-language development.
