"""Session skeletons for the unstable SDK surface."""

from __future__ import annotations

from dataclasses import dataclass

from vercel._internal.unstable.errors import SessionClosedError
from vercel._internal.unstable.session.settings import (
    SessionSettings,
    default_session_setting_sources,
    load_session_settings,
)


@dataclass(frozen=True, slots=True)
class SessionOptions:
    """Session-level runtime options."""

    client_pool_size: int | None = None


class Session:
    """Async runtime and lifecycle container."""

    def __init__(self, *, options: SessionOptions | None = None) -> None:
        self.options = options or SessionOptions()
        self._settings: SessionSettings | None = None
        self._initialized = False
        self._closed = False

    async def initialize(self) -> None:
        self._ensure_open()
        if self._settings is None:
            self._settings = load_session_settings(default_session_setting_sources(self.options))
        self._initialized = True

    async def aclose(self) -> None:
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise SessionClosedError("session is closed")

    async def __aenter__(self) -> Session:
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.aclose()


class SyncSession:
    """Sync runtime and lifecycle container."""

    def __init__(self, *, options: SessionOptions | None = None) -> None:
        self.options = options or SessionOptions()
        self._settings: SessionSettings | None = None
        self._initialized = False
        self._closed = False

    def initialize(self) -> None:
        self._ensure_open()
        if self._settings is None:
            self._settings = load_session_settings(default_session_setting_sources(self.options))
        self._initialized = True

    def close(self) -> None:
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise SessionClosedError("session is closed")

    def __enter__(self) -> SyncSession:
        self.initialize()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()


__all__ = ["Session", "SessionOptions", "SessionSettings", "SyncSession"]
