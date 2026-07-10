# vercel-celery

Celery integration for Vercel Queue Service and Vercel Runtime Cache.

This package provides a Celery broker transport backed by Vercel Queue Service
and a Celery result backend backed by Vercel Runtime Cache.

```python
from vercel.integrations.celery import install_vercel_celery_integration

install_vercel_celery_integration()
```

By default, named Celery apps publish to app-prefixed Vercel Queue topics. For
example, `Celery("my_app")` maps Celery queue `emails` to the Vercel Queue topic
`celery-my__app-emails` after Vercel Queue name sanitization. Apps with no name
use unprefixed topics. To override this, set
`broker_transport_options = {"queue_name_prefix": "jobs-"}`. Set
`queue_name_prefix` to `""` to use Celery queue names as Vercel Queue topics.

The broker's Vercel Queue consumer group defaults to `celery`. Workers sharing
the same Vercel Queue topic and consumer group compete for tasks. Workers using
the same topic with different consumer groups receive fan-out copies. To
override it, set `broker_transport_options = {"consumer_group": "workers"}`.

The installer sets Celery's default `broker_url` to `vercel://` when Celery has
no broker default configured. Pass `set_default_broker=False` to opt out.
It also sets Celery's default `result_backend` to `vercel-runtime-cache://`
when Celery has no result backend configured. Pass
`set_default_result_backend=False` to opt out.

`vercel://` uses push delivery when `VERCEL` is truthy in the environment and
poll delivery otherwise. Set `broker_url = "vercel-push://"` to force push
delivery, or `broker_url = "vercel-poll://"` for forced polling workers, local
workers, and background workers.

Pass `register_queues=False` to skip automatic Vercel Queue trigger
registration. When using that mode, manually register each Celery app whose
queues should receive Vercel Queue push deliveries:

```python
from vercel.integrations.celery import register_celery_app_queues

register_celery_app_queues(app)
```

To customize Celery task result storage in Vercel Runtime Cache, configure
Celery's result backend transport options:

```python
result_backend_transport_options = {
    "namespace": "celery-results",
    "ttl": 3600,
}
```

By default, named Celery apps use an app-scoped Runtime Cache namespace. For
example, `Celery("my_app")` stores results under `celery-results-my_app`. Apps
with no name use `celery-results`. If producers and workers use different
Celery app names, configure a shared explicit `namespace`.

The Runtime Cache backend uses `vercel.cache`, including its normal key hashing,
namespacing, and local in-memory fallback when Runtime Cache is unavailable.
Celery result backends are not required to retain results indefinitely; results
may expire according to Celery's result expiration settings or the backend's own
retention behavior. This backend is therefore an expiring, cache-backed result
backend rather than durable result storage. Celery treats missing result entries
as pending results. By default, stored results use Celery's `result_expires`
value as their Runtime Cache TTL; set `result_backend_transport_options["ttl"]`
to override that retention period.

JSON result serialization works normally. Binary serializers, including
`pickle`, are stored through an internal base64 wrapper so Runtime Cache only
receives JSON-compatible values.

Runtime Cache is a single-key cache API, so this backend does not provide native
bulk result fetches or increment-backed chord counters. Result availability is
limited by Runtime Cache retention and eviction behavior.
