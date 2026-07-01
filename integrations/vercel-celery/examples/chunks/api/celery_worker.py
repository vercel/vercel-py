from tasks import celery_app

from vercel.integrations.celery import register_celery_app_queues
from vercel.queue import asgi_app

celery_app.conf.broker_transport_options = {"consumer_group": "api/celery_worker.py"}
register_celery_app_queues(celery_app)

app = asgi_app()
