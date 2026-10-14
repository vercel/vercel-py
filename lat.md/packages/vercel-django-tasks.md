# vercel-django-tasks

A Django tasks backend that enqueues through Vercel Queues and keeps task results in Runtime Cache.

Lives in [[integrations/vercel-django-tasks]] and is exposed as [[integrations/vercel-django-tasks/vercel/integrations/django/__init__.py|vercel.integrations.django]]. It requires Django 6 (and therefore Python 3.12 or later) and builds on [[vercel-queue]] and [[vercel-cache]].

## Installation

The installer makes the Vercel backend Django's default when the project has not configured `TASKS`, and registers its queue subscribers.

[[integrations/vercel-django-tasks/vercel/integrations/django/_backend.py#install_vercel_django_task_integration]] fills in the default backend only when `TASKS` is not overridden, replacing any already-constructed non-Vercel default connection. It then registers each configured queue as a [[vercel-queue#Subscriptions|subscription]]. A `[[tool.vercel.subscribers]]` entry points at the module that loads the Django application, such as `my_project.wsgi`.

## Enqueueing and execution

Tasks travel as versioned JSON envelopes on per-queue topics, and failures retry with exponential backoff.

[[integrations/vercel-django-tasks/vercel/integrations/django/_backend.py#VercelQueuesBackend]] supports deferred tasks (`run_after` becomes a queue `delay`), async tasks, and result lookup; it does not support priorities. Workers consume under the consumer group `django-tasks`. A failed task is retried by raising `RetryAfter`, with delays growing from 5 seconds and doubling up to an hour. After three attempts, the result is marked failed.

## Results

Task results are written to Runtime Cache under a namespace with a TTL, so they expire and are not durable storage.

Results default to the `django-task-results` namespace with a 24-hour TTL. Both are set through `OPTIONS` (`result_namespace`, `result_ttl_seconds`). The cache is created in strict mode, so a missing Runtime Cache raises instead of silently storing results in process memory.
