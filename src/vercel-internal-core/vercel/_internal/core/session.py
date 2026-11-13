"""Mode-specific sessions shared by Vercel Python services."""

import threading
import time
from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar, Token
from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, Self, TypeVar, cast, overload

import anyio
import anyio.lowlevel
import httpx2 as httpx

from vercel._internal.core.errors import (
    VercelServiceOptionsError,
    VercelSessionClosedError,
    VercelSessionError,
)
from vercel._internal.core.http._compat import (
    is_async_http_client,
    is_sync_http_client,
)
from vercel._internal.core.options import ServiceOptions, merge_service_options

if TYPE_CHECKING:
    from vercel._internal.core.byte_stream import (
        AsyncByteStreamRuntime,
        StagingFileRuntime,
        SyncByteStreamRuntime,
    )
    from vercel._internal.core.http import AsyncTransport, BaseTransport, SyncTransport

ServiceOptionsT = TypeVar("ServiceOptionsT", bound=ServiceOptions)
ServiceT = TypeVar("ServiceT")
HttpxClientFactory = Callable[[], httpx.AsyncClient] | Callable[[], httpx.Client]
_UNSET = object()


def _current_event_loop() -> object | None:
    """Return the native AnyIO token of the running event loop, or `None` outside one.

    The token is the loop itself on asyncio and the run's `TrioToken` on Trio. It
    stays the same object for the whole run, so it can key per-loop state.
    """
    try:
        return anyio.lowlevel.current_token().native_token
    except RuntimeError:
        # Outside an event loop AnyIO raises `NoEventLoopError`, or sniffio's
        # `AsyncLibraryNotFoundError` on older releases; both are `RuntimeError`s.
        return None


def _ignore() -> None:
    pass


def _event_loop_has_finished(token: object) -> bool:
    """Tell whether the event loop behind a native AnyIO token can no longer run.

    AnyIO does not expose this, so each backend's own token is asked directly.
    """
    is_closed = getattr(token, "is_closed", None)
    if callable(is_closed):
        # asyncio: the token is the event loop.
        return bool(is_closed())
    run_sync_soon = getattr(token, "run_sync_soon", None)
    if callable(run_sync_soon):
        # Trio: a token whose run has ended raises `RunFinishedError`, a
        # `RuntimeError`, instead of queueing the callback. For a live run the
        # no-op is deduplicated and is harmless.
        try:
            run_sync_soon(_ignore, idempotent=True)
        except RuntimeError:
            return True
    return False


class _BaseSdkSession:
    """Common scoped state shared by one runtime-mode session."""

    def __init__(
        self,
        *,
        service_options: Mapping[type[ServiceOptions], ServiceOptions] | None = None,
        httpx_client_factory: HttpxClientFactory | None = None,
    ) -> None:
        self._service_options = dict(service_options or {})
        self._httpx_client_factory = httpx_client_factory
        self._closed = False
        self._service_cache: dict[type[object], object] = {}

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def service_options(self) -> Mapping[type[ServiceOptions], ServiceOptions]:
        self.check_open()
        return dict(self._service_options)

    def check_open(self) -> None:
        if self._closed:
            raise VercelSessionClosedError("SDK session is closed")

    def get_service_option(self, option_type: type[ServiceOptionsT]) -> ServiceOptionsT | None:
        self.check_open()
        option = self._service_options.get(option_type.service_options_key())
        if option is None:
            return None
        if not isinstance(option, option_type):
            raise VercelServiceOptionsError(
                f"{type(option).__name__} cannot configure this session mode; "
                f"use {option_type.__name__}"
            )
        return option

    def get_or_create_service(
        self, service_type: type[ServiceT], factory: Callable[[], ServiceT]
    ) -> ServiceT:
        self.check_open()
        cached = self._service_cache.get(service_type)
        if cached is None:
            cached = factory()
            self._service_cache[service_type] = cached
        return cast(ServiceT, cached)

    def get_transport(self) -> "BaseTransport":
        raise NotImplementedError()

    def get_staging_file_runtime(self) -> "StagingFileRuntime":
        raise NotImplementedError()

    async def sleep(self, seconds: float) -> None:
        raise NotImplementedError()

    def _clear_services(self) -> None:
        self._service_cache.clear()


class SdkSession(_BaseSdkSession):
    """Async runtime object for Vercel SDK services."""

    _default: ClassVar["SdkSession | None"] = None

    def __init__(
        self,
        *,
        service_options: Mapping[type[ServiceOptions], ServiceOptions] | None = None,
        httpx_client_factory: HttpxClientFactory | None = None,
    ) -> None:
        super().__init__(
            service_options=service_options,
            httpx_client_factory=httpx_client_factory,
        )
        self._transport: AsyncTransport | None = None
        self._staging_file_runtime: AsyncByteStreamRuntime | None = None
        # Keyed by the event loop's native AnyIO token, with `None` for a client
        # built outside one. The default session is process-wide, so it outlives
        # any loop its sockets belong to.
        self._clients: dict[object | None, httpx.AsyncClient] = {}
        # One session can be reached from threads running loops of their own.
        self._clients_lock = threading.Lock()

    @classmethod
    def default(cls) -> Self:
        if cls._default is None:
            cls._default = cls()
        return cast(Self, cls._default)

    @classmethod
    def scoped(
        cls,
        *,
        parent: Self,
        service_options: Sequence[ServiceOptions] | None,
        httpx_client_factory: HttpxClientFactory | None | object = _UNSET,
    ) -> Self:
        parent.check_open()
        factory = (
            parent._httpx_client_factory
            if httpx_client_factory is _UNSET
            else cast(HttpxClientFactory | None, httpx_client_factory)
        )
        return cast(
            Self,
            cls(
                service_options=merge_service_options(parent._service_options, service_options),
                httpx_client_factory=factory,
            ),
        )

    def _client_for_running_loop(self) -> httpx.AsyncClient:
        """Return the client owned by the loop making this request.

        Reusing a keep-alive socket from a closed loop does not fail early: the
        request is sent and the response is read, and only then does closing the
        connection call into the dead loop and raise `RuntimeError: Event loop is
        closed`. The caller sees an error for work the server already did, so the
        client is chosen per loop up front rather than repaired afterwards.
        """
        loop = _current_event_loop()
        with self._clients_lock:
            # Under the lock, so a request holding this transport cannot build a
            # client that outlives the session it belongs to.
            self.check_open()
            client = self._clients.get(loop)
            if client is not None:
                return client
            for known in [
                known
                for known in self._clients
                if known is not None and _event_loop_has_finished(known)
            ]:
                # Dropped, not closed: `aclose` needs the loop that owns the
                # sockets, and that loop is gone.
                del self._clients[known]
            client = self._create_client()
            self._clients[loop] = client
            return client

    def _create_client(self) -> httpx.AsyncClient:
        from vercel._internal.core.http import (
            DEFAULT_TIMEOUT,
            TransportOptions,
            create_base_async_client,
        )

        if self._httpx_client_factory is None:
            client = create_base_async_client(
                TransportOptions(
                    timeout=DEFAULT_TIMEOUT,
                    base_url=None,
                    max_connections=100,
                    enable_http2=False,
                )
            )
        else:
            candidate = cast(Any, self._httpx_client_factory())
            if not is_async_http_client(candidate):
                if is_sync_http_client(candidate):
                    try:
                        candidate.close()
                    except Exception:
                        pass
                raise VercelSessionError(
                    "Async SDK sessions require httpx_client_factory to return "
                    "httpx2.AsyncClient or httpx.AsyncClient"
                )
            client = cast(httpx.AsyncClient, candidate)
        return client

    def get_transport(self) -> "AsyncTransport":
        from vercel._internal.core.http import AsyncTransport

        self.check_open()
        if self._transport is None:
            self._transport = AsyncTransport(self._client_for_running_loop)
        return self._transport

    def get_staging_file_runtime(self) -> "AsyncByteStreamRuntime":
        from vercel._internal.core.byte_stream import AsyncByteStreamRuntime

        self.check_open()
        if self._staging_file_runtime is None:
            self._staging_file_runtime = AsyncByteStreamRuntime()
        return self._staging_file_runtime

    async def sleep(self, seconds: float) -> None:
        await anyio.sleep(seconds)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._clear_services()
        self._transport = None
        loop = _current_event_loop()
        with self._clients_lock:
            # Closing and the flag that stops new clients have to be one step, or
            # a resolver already past the check registers a client nothing closes.
            self._closed = True
            clients = list(self._clients.items())
            self._clients.clear()
        for owner, client in clients:
            # Another loop's client is dropped: closing it here would fail the
            # same way reusing it does.
            if owner is None or owner is loop:
                await client.aclose()


class SyncSdkSession(_BaseSdkSession):
    """Synchronous runtime object for Vercel SDK services."""

    _default: ClassVar["SyncSdkSession | None"] = None

    def __init__(
        self,
        *,
        service_options: Mapping[type[ServiceOptions], ServiceOptions] | None = None,
        httpx_client_factory: HttpxClientFactory | None = None,
    ) -> None:
        super().__init__(
            service_options=service_options,
            httpx_client_factory=httpx_client_factory,
        )
        self._transport: SyncTransport | None = None
        self._staging_file_runtime: SyncByteStreamRuntime | None = None

    @classmethod
    def default(cls) -> Self:
        if cls._default is None:
            cls._default = cls()
        return cast(Self, cls._default)

    @classmethod
    def scoped(
        cls,
        *,
        parent: Self,
        service_options: Sequence[ServiceOptions] | None,
        httpx_client_factory: HttpxClientFactory | None | object = _UNSET,
    ) -> Self:
        parent.check_open()
        factory = (
            parent._httpx_client_factory
            if httpx_client_factory is _UNSET
            else cast(HttpxClientFactory | None, httpx_client_factory)
        )
        return cast(
            Self,
            cls(
                service_options=merge_service_options(parent._service_options, service_options),
                httpx_client_factory=factory,
            ),
        )

    def _close_wrong_async_client(self, client: Any) -> None:
        """Close, best effort, an async client a factory returned to this sync session.

        The client was just built and has sent nothing, so no event loop owns its
        resources and any loop can close it. A fresh one runs in a worker thread,
        because this sync path may itself be called from inside a running loop.
        """

        def close() -> None:
            try:
                anyio.run(client.aclose)
            except Exception:
                pass

        worker = threading.Thread(target=close, name="vercel-close-async-client", daemon=True)
        worker.start()
        worker.join()

    def get_transport(self) -> "SyncTransport":
        from vercel._internal.core.http import (
            DEFAULT_TIMEOUT,
            SyncTransport,
            TransportOptions,
            create_base_client,
        )

        self.check_open()
        if self._transport is not None:
            return self._transport

        if self._httpx_client_factory is None:
            client = create_base_client(
                TransportOptions(
                    timeout=DEFAULT_TIMEOUT,
                    base_url=None,
                    max_connections=100,
                    enable_http2=False,
                )
            )
        else:
            candidate = cast(Any, self._httpx_client_factory())
            if not is_sync_http_client(candidate):
                if is_async_http_client(candidate):
                    self._close_wrong_async_client(candidate)
                raise VercelSessionError(
                    "Sync SDK sessions require httpx_client_factory to return "
                    "httpx2.Client or httpx.Client"
                )
            client = cast(httpx.Client, candidate)
        self._transport = SyncTransport(client)
        return self._transport

    def get_staging_file_runtime(self) -> "SyncByteStreamRuntime":
        from vercel._internal.core.byte_stream import SyncByteStreamRuntime

        self.check_open()
        if self._staging_file_runtime is None:
            self._staging_file_runtime = SyncByteStreamRuntime()
        return self._staging_file_runtime

    async def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._clear_services()
        if self._transport is not None:
            self._transport.close()
            self._transport = None


_active_session: ContextVar[SdkSession | SyncSdkSession | None] = ContextVar(
    "vercel_active_session",
    default=None,
)


def get_active_session() -> SdkSession:
    active = _active_session.get()
    if active is None:
        return SdkSession.default()
    if isinstance(active, SyncSdkSession):
        raise VercelSessionError("Async Vercel APIs cannot be used in a sync SDK session")
    return active


def get_active_sync_session() -> SyncSdkSession:
    active = _active_session.get()
    if active is None:
        return SyncSdkSession.default()
    if isinstance(active, SdkSession):
        raise VercelSessionError("Sync Vercel APIs cannot be used in an async SDK session")
    return active


def bind_active_session(
    active: SdkSession | SyncSdkSession,
) -> Token[SdkSession | SyncSdkSession | None]:
    return _active_session.set(active)


def reset_active_session(token: Token[SdkSession | SyncSdkSession | None]) -> None:
    _active_session.reset(token)


class SessionContext:
    """Context object returned by `vercel.api.session(...)`."""

    def __init__(
        self,
        *,
        service_options: Sequence[ServiceOptions] | None = None,
        httpx_client_factory: HttpxClientFactory | None | object = _UNSET,
    ) -> None:
        self._service_options = tuple(service_options) if service_options is not None else None
        self._httpx_client_factory = httpx_client_factory
        self._token: Token[SdkSession | SyncSdkSession | None] | None = None
        self._session: SdkSession | SyncSdkSession | None = None

    def __enter__(self) -> Self:
        if self._token is not None:
            raise RuntimeError("vercel.api.session(...) contexts cannot be re-entered")

        session = SyncSdkSession.scoped(
            parent=get_active_sync_session(),
            service_options=self._service_options,
            httpx_client_factory=self._httpx_client_factory,
        )
        self._token = bind_active_session(session)
        self._session = session
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._token is None or not isinstance(self._session, SyncSdkSession):
            return None

        token = self._token
        session = self._session
        self._token = None
        self._session = None
        try:
            session.close()
        finally:
            reset_active_session(token)

    async def __aenter__(self) -> Self:
        if self._token is not None:
            raise RuntimeError("vercel.api.session(...) contexts cannot be re-entered")

        session = SdkSession.scoped(
            parent=get_active_session(),
            service_options=self._service_options,
            httpx_client_factory=self._httpx_client_factory,
        )
        self._token = bind_active_session(session)
        self._session = session
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._token is None or not isinstance(self._session, SdkSession):
            return None

        token = self._token
        session = self._session
        self._token = None
        self._session = None
        try:
            await session.aclose()
        finally:
            reset_active_session(token)


@overload
def session(*, service_options: Sequence[ServiceOptions] | None = None) -> "SessionContext": ...


@overload
def session(
    *,
    service_options: Sequence[ServiceOptions] | None = None,
    httpx_client_factory: HttpxClientFactory | None,
) -> "SessionContext": ...


def session(
    *,
    service_options: Sequence[ServiceOptions] | None = None,
    httpx_client_factory: HttpxClientFactory | None | object = _UNSET,
) -> "SessionContext":
    return SessionContext(
        service_options=service_options,
        httpx_client_factory=httpx_client_factory,
    )
