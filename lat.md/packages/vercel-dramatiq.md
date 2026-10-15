# vercel-dramatiq

Dramatiq on Vercel: a broker backed by Vercel Queues and a results backend backed by Runtime Cache.

Lives in [[integrations/vercel-dramatiq]] and is exposed as [[integrations/vercel-dramatiq/vercel/integrations/dramatiq/__init__.py|vercel.integrations.dramatiq]]. It builds on [[vercel-queue]] and [[vercel-cache]].

## Broker

The broker forwards client options to a Vercel Queue client and maps each Dramatiq queue, including its delay queue, to a topic.

[[integrations/vercel-dramatiq/vercel/integrations/dramatiq/_broker.py#VercelQueueBroker]] accepts queue client options (token, region, base URL, deployment, timeout, headers) and broker options (consumer group, retention, lease duration, requeue delay, middleware). Workers share the consumer group `dramatiq` by default.

Each declared queue also declares Dramatiq's delay queue: `default` maps to the topics `default` and the sanitized `default_DDQ`. Retries and delayed messages travel through the delay topic, so subscriber registration has to cover both. [[integrations/vercel-dramatiq/vercel/integrations/dramatiq/_broker.py#register_dramatiq_queues]] registers every declared queue as a [[vercel-queue#Subscriptions|subscription]], and a `[[tool.vercel.subscribers]]` entry points at the broker.

Constructing the broker explicitly is the recommended setup. [[integrations/vercel-dramatiq/vercel/integrations/dramatiq/_broker.py#install_vercel_dramatiq_integration]] installs a default broker only when no global broker exists yet. Vercel Functions install the integration before user code runs, so options passed to a later installer call have no effect there.

## Push delivery

Push deliveries are handed to Dramatiq's own consumers and settled before the invocation returns, as in [[vercel-celery#Push and poll delivery]].

`handle_push_message` waits in-request for a consumer to accept the delivery, within a handoff budget, and falls back to `RetryAfter` if none does. It then waits for the message to settle and raises `Handoff`, leaving acknowledgement to Dramatiq. Push is used when `VERCEL` is truthy and polling otherwise; `poll=True` or `poll=False` forces a mode. `VERCEL_DRAMATIQ_DEBUG=1` enables debug logging for the integration and Dramatiq's worker loggers.

## Results

Actor results can be stored in Runtime Cache through Dramatiq's results middleware.

[[integrations/vercel-dramatiq/vercel/integrations/dramatiq/_result_backend.py#VercelRuntimeCacheBackend]] plugs into `dramatiq.results.Results`. Like every Runtime Cache-backed store, results expire and are not durable.
