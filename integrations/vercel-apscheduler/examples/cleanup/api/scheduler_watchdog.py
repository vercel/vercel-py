from __future__ import annotations

from api.scheduler import OPTIONS, scheduler

from vercel.integrations.apscheduler import get_watchdog_asgi_app

app = get_watchdog_asgi_app(scheduler, options=OPTIONS)
