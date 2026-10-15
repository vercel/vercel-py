# vercel-celery

Celery on Vercel: a Kombu broker transport backed by Vercel Queues and a result backend backed by Runtime Cache.

Lives in [[integrations/vercel-celery]] and is exposed as [[integrations/vercel-celery/vercel/integrations/celery/__init__.py|vercel.integrations.celery]]. It builds on [[vercel-queue]] and [[vercel-cache]].

## Installation and defaults

One installer wires Celery's defaults to Vercel, without overriding anything the application already configured.

[[integrations/vercel-celery/vercel/integrations/celery/__init__.py#install_vercel_celery_integration]] sets `broker_url` to `vercel://` and `result_backend` to `vercel-runtime-cache://`, but only where Celery has no default yet; each can be opted out. It also registers the app's queues as [[vercel-queue#Subscriptions]], so the Vercel build can compile a `[[tool.vercel.subscribers]]` entry pointing at the Celery app into a queue-triggered Function. [[integrations/vercel-celery/vercel/integrations/celery/_broker.py#register_celery_app_queues]] does this manually when automatic registration is off.

## Topics and consumer groups

Celery queues map onto Vercel Queue topics, and consumer groups decide between competing workers and fan-out.

Named apps use app-prefixed topics: `Celery("my_app")` maps queue `emails` to the sanitized topic `celery-my__app-emails`. Unnamed apps use unprefixed topics, and `queue_name_prefix` overrides the prefix. Workers share the consumer group `celery` by default, so they compete for tasks; giving workers different groups produces fan-out copies.

## Push and poll delivery

The `vercel://` transport delivers by push on Vercel and by polling elsewhere; explicit schemes force either mode.

[[integrations/vercel-celery/vercel/integrations/celery/__init__.py#VercelQueueTransport]] picks push when `VERCEL` is truthy and polling otherwise. `vercel-push://` ([[integrations/vercel-celery/vercel/integrations/celery/__init__.py#VercelQueuePushTransport]]) and `vercel-poll://` ([[integrations/vercel-celery/vercel/integrations/celery/__init__.py#VercelQueuePollTransport]]) force one mode.

In push mode, a delivery is handed into Kombu, and the handler raises [[vercel-queue#Push delivery|Handoff]] so that Celery, not the queue SDK, owns acknowledgement. Two details matter on Vercel:

- If the embedded worker has no prefetch capacity yet, the handler waits in-request, up to a budget (30 seconds by default), before bouncing the delivery with `RetryAfter`. Server-paced redelivery can take up to the remaining visibility timeout, so waiting is usually cheaper.
- Celery normally drains acknowledgements from its consumer loop, but that thread is suspended between requests. The handler therefore settles the acknowledgement or reject before the push invocation returns; otherwise the lease would expire and the task would run again.

## Result backend

Task results live in Runtime Cache, which makes them expiring rather than durable.

[[integrations/vercel-celery/vercel/integrations/celery/_result_backend.py#VercelRuntimeCacheBackend]] stores results under an app-scoped namespace (`celery-results-<app>`). Their TTL defaults to Celery's `result_expires`. Binary serializers such as pickle are wrapped in base64, because the cache accepts only JSON-compatible values. Missing entries read as pending. There are no native bulk fetches and no increment-backed chord counters, because Runtime Cache is a single-key API.
