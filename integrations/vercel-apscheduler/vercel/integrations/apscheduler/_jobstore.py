"""Redis job store with Vercel environment defaults."""

from __future__ import annotations

from typing import Any

from os import environ

from redis import Redis

from ._imports import RedisJobStore

DEFAULT_REDIS_URL_ENV = "REDIS_URL"

__all__ = ["VercelRedisJobStore"]


class VercelRedisJobStore(RedisJobStore):
    """APScheduler RedisJobStore that defaults to ``REDIS_URL``."""

    def __init__(
        self,
        url: str | None = None,
        **kwargs: Any,
    ) -> None:
        resolved_url = url or environ.get(DEFAULT_REDIS_URL_ENV)
        super().__init__(**kwargs)
        if resolved_url:
            self.redis = Redis.from_url(resolved_url)
