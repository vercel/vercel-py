# vercel-dramatiq

Dramatiq broker backed by Vercel Queue Service.

```python
# tasks.py
import dramatiq

from vercel.integrations.dramatiq import VercelQueueBroker

broker = VercelQueueBroker()
dramatiq.set_broker(broker)


@dramatiq.actor
def send_email(user_id: str) -> None: ...
```

Constructing the broker explicitly is the recommended pattern: it always takes
effect and the broker options are visible at the construction site. To store
actor results in Vercel Runtime Cache, add the results middleware:

```python
from dramatiq.results import Results

from vercel.integrations.dramatiq import VercelRuntimeCacheBackend

broker.add_middleware(Results(backend=VercelRuntimeCacheBackend()))
```

Alternatively, `install_vercel_dramatiq_integration()` sets Dramatiq's global
broker to a default `VercelQueueBroker` with the results middleware when no
broker has been configured yet. Note that this is a no-op once a global broker
exists, so broker options passed to the installer do not apply in environments
that install the integration first (Vercel functions do).

The broker uses push delivery when `VERCEL` is truthy in the environment and
poll delivery otherwise. Pass `poll=False` or `poll=True` to
`VercelQueueBroker(...)` to force a mode.

For Vercel push delivery, declare a module that exposes the broker as a queue
subscriber in `pyproject.toml`:

```python
# worker.py
# Importing tasks declares the actors' queues on the broker.
from tasks import broker

__all__ = ["broker"]
```

```toml
[[tool.vercel.subscribers]]
entrypoint = "worker:broker"
```

The Vercel build introspects every queue declared on the broker and compiles
the subscriber into a queue-triggered function. That includes each queue's
Dramatiq delay queue: the `default` queue maps to both the `default` topic and
the sanitized `default_DDQ` delay topic, and retries and delayed messages are
delivered through the delay queue topic.

Set `VERCEL_DRAMATIQ_DEBUG=1` to enable debug logging for the integration and
Dramatiq worker loggers.

The package is standalone and depends on `vercel-queue` and Dramatiq.
