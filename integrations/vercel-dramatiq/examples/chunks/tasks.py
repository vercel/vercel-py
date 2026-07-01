from __future__ import annotations

import dramatiq

from vercel.integrations.dramatiq import install_vercel_dramatiq_integration

install_vercel_dramatiq_integration(
    consumer_group="api/dramatiq_worker.py",
    queue_name_prefix="dramatiq-vercel-dramatiq-example-chunks-",
)


@dramatiq.actor(store_results=True)
def add(left: int, right: int) -> int:
    result = left + right
    print(f"{left} + {right} = {result}")
    return result
