# vercel-dramatiq

Dramatiq broker backed by Vercel Queue Service.

```python
import dramatiq

from vercel.integrations.dramatiq import install_vercel_dramatiq_integration

install_vercel_dramatiq_integration()


@dramatiq.actor
def send_email(user_id: str) -> None: ...
```

The installer sets Dramatiq's global broker to `VercelQueueBroker` when no
broker has been configured yet. Pass `set_default_broker=False` to opt out, or
call `dramatiq.set_broker(VercelQueueBroker(...))` directly when you need to
construct the broker yourself.

The broker uses push delivery when `VERCEL` is truthy in the environment and
poll delivery otherwise. Pass `poll=False` or `poll=True` to
`install_vercel_dramatiq_integration(...)` to force a mode, or pass the same
option to `VercelQueueBroker(...)` when constructing the broker yourself.

For Vercel push delivery, declare a module that exposes the broker as a queue
subscriber in `pyproject.toml`:

```python
# worker.py
import dramatiq
import tasks  # noqa: F401  (importing tasks declares the actors' queues)

broker = dramatiq.get_broker()

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
