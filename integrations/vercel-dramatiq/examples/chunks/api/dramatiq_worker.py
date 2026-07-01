import tasks  # noqa: F401

import vercel.queue
from vercel.integrations.dramatiq import register_dramatiq_queues

register_dramatiq_queues()

app = vercel.queue.asgi_app()
