from __future__ import annotations

import dramatiq
from dramatiq.results import Results

from vercel.integrations.dramatiq import VercelQueueBroker, VercelRuntimeCacheBackend

# Construct the broker explicitly: dramatiq.set_broker() always takes effect,
# and the broker options are visible right here.
broker = VercelQueueBroker(queue_name_prefix="dramatiq-vercel-dramatiq-example-chunks-")
broker.add_middleware(Results(backend=VercelRuntimeCacheBackend()))
dramatiq.set_broker(broker)


@dramatiq.actor(store_results=True)
def add(left: int, right: int) -> int:
    result = left + right
    print(f"{left} + {right} = {result}")
    return result
