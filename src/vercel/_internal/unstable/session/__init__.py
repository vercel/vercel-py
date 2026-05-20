"""Session skeletons for the unstable SDK surface."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from vercel._internal.http import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT, TransportOptions
from vercel._internal.unstable.errors import SessionClosedError
from vercel._internal.unstable.session.settings import (
    SessionSettings,
    default_session_setting_sources,
    load_session_settings,
)

if TYPE_CHECKING:
    from vercel._internal.http.transport import AsyncTransport, SyncTransport
    from vercel._internal.unstable.sandbox.accessor import (
        SandboxAccessor,
        SyncSandboxAccessor,
    )


@dataclass(frozen=True, slots=True)
class SessionOptions:
    """Session-level runtime options."""

    client_pool_size: int | None = None
    http2: bool | None = None


class Session:
    """Async runtime and lifecycle container."""

    def __init__(self, *, options: SessionOptions | None = None) -> None:
        self.options = options or SessionOptions()
        self._settings: SessionSettings | None = None
        self._sandbox_accessor: SandboxAccessor | None = None
        self._transport: AsyncTransport | None = None
        self._close_hooks: list[Callable[[], Awaitable[None]]] = []
        self._closed = False
        self._owns_transport = False

    @property
    def sandbox(self) -> SandboxAccessor:
        if self._sandbox_accessor is None:
            from vercel._internal.unstable.sandbox.accessor import SandboxAccessor

            self._sandbox_accessor = SandboxAccessor(self)
        return self._sandbox_accessor

    @property
    def _is_initialized(self) -> bool:
        return self._settings is not None and self._transport is not None

    async def initialize(self) -> None:
        self._ensure_open()
        if self._is_initialized:
            return
        if self._settings is None:
            self._settings = load_session_settings(default_session_setting_sources(self.options))
        if self._transport is None:
            from vercel._internal.http import create_base_async_client
            from vercel._internal.http.transport import AsyncTransport

            transport_options = TransportOptions(
                timeout=DEFAULT_TIMEOUT,
                base_url=DEFAULT_API_BASE_URL,
                max_connections=self._settings.client_pool_size,
                enable_http2=self._settings.http2,
            )
            self._transport = AsyncTransport(create_base_async_client(transport_options))
            self._owns_transport = True

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        for close_hook in self._close_hooks:
            await close_hook()
        self._close_hooks.clear()
        if self._owns_transport and self._transport is not None:
            await self._transport.aclose()
            self._transport = None
            self._owns_transport = False

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
        self._transport: SyncTransport | None = None
        self._close_hooks: list[Callable[[], None]] = []
        self._closed = False
        self._owns_transport = False

    @property
    def sandbox(self) -> SyncSandboxAccessor:
        if self._sandbox_accessor is None:
            from vercel._internal.unstable.sandbox.accessor import SyncSandboxAccessor

            self._sandbox_accessor = SyncSandboxAccessor(self)
        return self._sandbox_accessor

    @property
    def _is_initialized(self) -> bool:
        return self._settings is not None and self._transport is not None

    def initialize(self) -> None:
        self._ensure_open()
        if self._is_initialized:
            return
        if self._settings is None:
            self._settings = load_session_settings(default_session_setting_sources(self.options))
        if self._transport is None:
            from vercel._internal.http import create_base_client
            from vercel._internal.http.transport import SyncTransport

            transport_options = TransportOptions(
                timeout=DEFAULT_TIMEOUT,
                base_url=DEFAULT_API_BASE_URL,
                max_connections=self._settings.client_pool_size,
                enable_http2=self._settings.http2,
            )
            self._transport = SyncTransport(create_base_client(transport_options))
            self._owns_transport = True

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for close_hook in self._close_hooks:
            close_hook()
        self._close_hooks.clear()
        if self._owns_transport and self._transport is not None:
            self._transport.close()
            self._transport = None
            self._owns_transport = False

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
