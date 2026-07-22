from __future__ import annotations

from typing import Any

import hmac
import logging
import os
from functools import partial

import anyio

from ._adapter import SchedulerAdapter, adopt_scheduler
from ._imports import BaseScheduler
from ._options import VercelAPSchedulerOptions

LOGGER = logging.getLogger("vercel.integrations.apscheduler")

__all__ = ["get_watchdog_asgi_app"]


class APSchedulerWatchdogAsgiApp:
    """Cron endpoint that idempotently restores a scheduler wake chain."""

    def __init__(self, adapter: SchedulerAdapter) -> None:
        self.adapter = adapter

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        del receive
        if scope.get("type") != "http" or scope.get("method") != "GET":
            await self._send_status(send, 405)
            return
        if not self._authorized(scope):
            await self._send_status(send, 401)
            return

        try:
            await anyio.to_thread.run_sync(partial(self.adapter.seed, kind="watchdog"))
        except Exception:
            LOGGER.exception("APScheduler watchdog failed to seed a wakeup")
            await self._send_status(send, 500)
            return
        finally:
            self.adapter.shutdown(wait=True)

        await self._send_status(send, 204)

    @staticmethod
    def _authorized(scope: Any) -> bool:
        secret = os.environ.get("CRON_SECRET")
        if not secret:
            return True
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        actual = headers.get(b"authorization", b"")
        expected = f"Bearer {secret}".encode()
        return hmac.compare_digest(actual, expected)

    @staticmethod
    async def _send_status(send: Any, status: int) -> None:
        await send({"type": "http.response.start", "status": status, "headers": []})
        await send({"type": "http.response.body", "body": b""})


def get_watchdog_asgi_app(
    scheduler: BaseScheduler,
    *,
    options: VercelAPSchedulerOptions | dict[str, Any] | None = None,
) -> APSchedulerWatchdogAsgiApp:
    return APSchedulerWatchdogAsgiApp(adopt_scheduler(scheduler, options))
