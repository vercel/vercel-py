"""Session skeletons for the unstable SDK surface."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from vercel._internal.unstable.errors import SessionClosedError
from vercel._internal.unstable.session.settings import (
    SessionSettings,
    default_session_setting_sources,
    load_session_settings,
)

if TYPE_CHECKING:
    from vercel._internal.http.transport import BaseTransport
    from vercel._internal.unstable.sandbox.accessor import (
        SandboxAccessor,
        SyncSandboxAccessor,
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
        self._sandbox_accessor: SandboxAccessor | None = None
        self._sandbox_transport: BaseTransport | None = None
        self._close_hooks: list[Callable[[], Awaitable[None]]] = []
        self._initialized = False
        self._closed = False

    @property
    def sandbox(self) -> SandboxAccessor:
        if self._sandbox_accessor is None:
            from vercel._internal.unstable.sandbox.accessor import SandboxAccessor

            self._sandbox_accessor = SandboxAccessor(self)
        return self._sandbox_accessor

    async def initialize(self) -> None:
        self._ensure_open()
        if self._settings is None:
            self._settings = load_session_settings(default_session_setting_sources(self.options))
        self._initialized = True

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        for close_hook in self._close_hooks:
            await close_hook()
        self._close_hooks.clear()

    def _ensure_open(self) -> None:
        if self._closed:
            raise SessionClosedError("session is closed")

    def _add_close_hook(self, close_hook: Callable[[], Awaitable[None]]) -> None:
        self._close_hooks.append(close_hook)

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
        self._sandbox_accessor: SyncSandboxAccessor | None = None
        self._sandbox_transport: BaseTransport | None = None
        self._close_hooks: list[Callable[[], None]] = []
        self._initialized = False
        self._closed = False

    @property
    def sandbox(self) -> SyncSandboxAccessor:
        if self._sandbox_accessor is None:
            from vercel._internal.unstable.sandbox.accessor import SyncSandboxAccessor

            self._sandbox_accessor = SyncSandboxAccessor(self)
        return self._sandbox_accessor

    def initialize(self) -> None:
        self._ensure_open()
        if self._settings is None:
            self._settings = load_session_settings(default_session_setting_sources(self.options))
        self._initialized = True

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for close_hook in self._close_hooks:
            close_hook()
        self._close_hooks.clear()

    def _ensure_open(self) -> None:
        if self._closed:
            raise SessionClosedError("session is closed")

    def _add_close_hook(self, close_hook: Callable[[], None]) -> None:
        self._close_hooks.append(close_hook)

    def __enter__(self) -> SyncSession:
        self.initialize()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()


__all__ = ["Session", "SessionOptions", "SessionSettings", "SyncSession"]
