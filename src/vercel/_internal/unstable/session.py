"""Mode-specific SDK sessions for the experimental public API."""

import asyncio
import time
from collections.abc import Callable, Mapping, Sequence
from contextvars import Token
from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast, overload

import httpx

from vercel._internal.unstable.errors import VercelSessionClosedError, VercelSessionError
from vercel._internal.unstable.options import ServiceOptions, merge_service_options

if TYPE_CHECKING:
    from vercel._internal.http import AsyncTransport, SyncTransport
    from vercel._internal.unstable.sandbox.client import AsyncSandboxClient, SyncSandboxClient

ServiceOptionsT = TypeVar("ServiceOptionsT", bound=ServiceOptions)
HttpxClientFactory = Callable[[], httpx.AsyncClient] | Callable[[], httpx.Client]
_UNSET = object()


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

    @property
    def settings(self) -> Mapping[str, Any]:
        self.check_open()
        return {"httpx_client_factory": self._httpx_client_factory}

    def check_open(self) -> None:
        if self._closed:
            raise VercelSessionClosedError("SDK session is closed")

    def get_service_option(self, option_type: type[ServiceOptionsT]) -> ServiceOptionsT | None:
        self.check_open()
        option = self._service_options.get(option_type)
        if option is None:
            return None
        return cast(ServiceOptionsT, option)

    def get_setting(self, name: str, default: Any = None) -> Any:
        self.check_open()
        if name == "httpx_client_factory":
            return self._httpx_client_factory
        return default

    def _clear_services(self) -> None:
        self._service_cache.clear()


class SdkSession(_BaseSdkSession):
    """Async runtime object for unstable SDK services."""

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

    @classmethod
    def default(cls) -> "SdkSession":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    @classmethod
    def scoped(
        cls,
        *,
        parent: "SdkSession",
        service_options: Sequence[ServiceOptions] | None,
        httpx_client_factory: HttpxClientFactory | None | object = _UNSET,
    ) -> "SdkSession":
        parent.check_open()
        factory = (
            parent._httpx_client_factory
            if httpx_client_factory is _UNSET
            else cast(HttpxClientFactory | None, httpx_client_factory)
        )
        return cls(
            service_options=merge_service_options(parent._service_options, service_options),
            httpx_client_factory=factory,
        )

    def _get_transport(self) -> "AsyncTransport":
        self.check_open()
        if self._transport is not None:
            return self._transport

        from vercel._internal.http import (
            DEFAULT_TIMEOUT,
            AsyncTransport,
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
            candidate = self._httpx_client_factory()
            if not isinstance(candidate, httpx.AsyncClient):
                if isinstance(candidate, httpx.Client):
                    try:
                        candidate.close()
                    except Exception:
                        pass
                raise VercelSessionError(
                    "Async SDK sessions require httpx_client_factory to return httpx.AsyncClient"
                )
            client = candidate
        self._transport = AsyncTransport(client)
        return self._transport

    def sandbox_service(self) -> "AsyncSandboxClient":
        self.check_open()

        from vercel._internal.unstable.sandbox.api_client import SandboxApiClient
        from vercel._internal.unstable.sandbox.client import AsyncSandboxClient
        from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
        from vercel._internal.unstable.sandbox.service import SandboxService

        cached = self._service_cache.get(AsyncSandboxClient)
        if cached is not None:
            return cast(AsyncSandboxClient, cached)
        options = self.get_service_option(SandboxServiceOptions) or SandboxServiceOptions()
        service = SandboxService(
            api_client=SandboxApiClient(
                base_url=options.base_url,
                credentials_factory=options.credentials_factory,
                transport=self._get_transport(),
            ),
            options=options,
            ensure_open=self.check_open,
        )
        client = AsyncSandboxClient(service)
        self._service_cache[AsyncSandboxClient] = client
        return client

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._clear_services()
        if self._transport is not None:
            await self._transport.aclose()
            self._transport = None


class SyncSdkSession(_BaseSdkSession):
    """Synchronous runtime object for unstable SDK services."""

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

    @classmethod
    def default(cls) -> "SyncSdkSession":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    @classmethod
    def scoped(
        cls,
        *,
        parent: "SyncSdkSession",
        service_options: Sequence[ServiceOptions] | None,
        httpx_client_factory: HttpxClientFactory | None | object = _UNSET,
    ) -> "SyncSdkSession":
        parent.check_open()
        factory = (
            parent._httpx_client_factory
            if httpx_client_factory is _UNSET
            else cast(HttpxClientFactory | None, httpx_client_factory)
        )
        return cls(
            service_options=merge_service_options(parent._service_options, service_options),
            httpx_client_factory=factory,
        )

    def _close_wrong_async_client(self, client: httpx.AsyncClient) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                asyncio.run(client.aclose())
            except Exception:
                pass
        else:
            loop.create_task(client.aclose())

    def _get_transport(self) -> "SyncTransport":
        self.check_open()
        if self._transport is not None:
            return self._transport

        from vercel._internal.http import (
            DEFAULT_TIMEOUT,
            SyncTransport,
            TransportOptions,
            create_base_client,
        )

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
            candidate = self._httpx_client_factory()
            if not isinstance(candidate, httpx.Client):
                if isinstance(candidate, httpx.AsyncClient):
                    self._close_wrong_async_client(candidate)
                raise VercelSessionError(
                    "Sync SDK sessions require httpx_client_factory to return httpx.Client"
                )
            client = candidate
        self._transport = SyncTransport(client)
        return self._transport

    def sandbox_service(self) -> "SyncSandboxClient":
        self.check_open()

        from vercel._internal.unstable.sandbox.api_client import SandboxApiClient
        from vercel._internal.unstable.sandbox.client import SyncSandboxClient
        from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
        from vercel._internal.unstable.sandbox.service import SandboxService

        cached = self._service_cache.get(SyncSandboxClient)
        if cached is not None:
            return cast(SyncSandboxClient, cached)
        options = self.get_service_option(SandboxServiceOptions) or SandboxServiceOptions()

        async def sync_sleep(seconds: float) -> None:
            time.sleep(seconds)

        service = SandboxService(
            api_client=SandboxApiClient(
                base_url=options.base_url,
                credentials_factory=options.credentials_factory,
                transport=self._get_transport(),
            ),
            options=options,
            ensure_open=self.check_open,
            sleep=sync_sleep,
        )
        client = SyncSandboxClient(service)
        self._service_cache[SyncSandboxClient] = client
        return client

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._clear_services()
        if self._transport is not None:
            self._transport.close()
            self._transport = None


class SessionContext:
    """Public `vercel.session(...)` context object."""

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

    def __enter__(self) -> "SessionContext":
        if self._token is not None:
            raise RuntimeError("vercel.session(...) contexts cannot be re-entered")

        from vercel._internal.unstable.context import bind_active_session, get_active_sync_session

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

        from vercel._internal.unstable.context import reset_active_session

        token = self._token
        session = self._session
        self._token = None
        self._session = None
        try:
            session.close()
        finally:
            reset_active_session(token)

    async def __aenter__(self) -> "SessionContext":
        if self._token is not None:
            raise RuntimeError("vercel.session(...) contexts cannot be re-entered")

        from vercel._internal.unstable.context import bind_active_session, get_active_session

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

        from vercel._internal.unstable.context import reset_active_session

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
