from __future__ import annotations

from celery import Celery

from vercel.integrations.celery import install_vercel_celery_integration

install_vercel_celery_integration()

celery_app = Celery("vercel_celery_example_chunks")


@celery_app.task(name="chunks.add")
def add(left: int, right: int) -> int:
    result = left + right
    print(f"{left} + {right} = {result}")
    return result
