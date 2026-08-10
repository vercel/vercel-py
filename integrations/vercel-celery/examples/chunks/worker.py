# Subscriber entrypoint ("worker:celery_app"). Importing tasks registers the
# Celery tasks on the app so the queue subscriber can execute them.
from tasks import celery_app

__all__ = ["celery_app"]
