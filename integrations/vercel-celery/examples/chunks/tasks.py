from __future__ import annotations

import os

from celery import Celery

# On Vercel, the runtime installs the vercel-celery integration before this
# module is imported, registering the "vercel://" broker transport and the
# "vercel-runtime-cache://" result backend.
celery_app = Celery(
    "vercel_celery_example_chunks",
    broker=os.getenv("CELERY_BROKER_URL", "vercel://"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "vercel-runtime-cache://"),
)


@celery_app.task(name="chunks.add")
def add(left: int, right: int) -> int:
    result = left + right
    print(f"{left} + {right} = {result}")
    return result
