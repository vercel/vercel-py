"""SDK session skeleton for the experimental public API."""

import inspect
from collections.abc import Awaitable, Mapping, Sequence
from contextvars import Token
from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast

from vercel._internal.unstable.errors import VercelSessionClosedError
from vercel._internal.unstable.options import ServiceOptions, merge_service_options

if TYPE_CHECKING:
    from vercel._internal.unstable.sandbox.service import SandboxService

ServiceOptionsT = TypeVar("ServiceOptionsT", bound=ServiceOptions)


class AliveToken:
    """Shared invalidation marker for session-owned runtime objects."""

    def __init__(self) -> None:
        self._is_alive = True

    @property
    def is_alive(self) -> bool:
        return self._is_alive

    def invalidate(self) -> None:
        self._is_alive = False

    def raise_if_invalid(self, message: str = "SDK session is closed") -> None:
        if not self._is_alive:
            raise VercelSessionClosedError(message)


class SdkSession:
    """Internal runtime object for active unstable SDK settings."""

    _default: ClassVar["SdkSession | None"] = None

    def __init__(
        self,
        *,
        service_options: Mapping[type[ServiceOptions], ServiceOptions] | None = None,
        settings: Mapping[str, Any] | None = None,
        alive_token: AliveToken | None = None,
    ) -> None:
        self._service_options = dict(service_options or {})
        self._settings = dict(settings or {})
        self._alive_token = alive_token or AliveToken()
        self._service_cache: dict[type[object], object] = {}

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
        settings: Mapping[str, Any],
    ) -> "SdkSession":
        parent.check_alive()
        return cls(
            service_options=merge_service_options(parent._service_options, service_options),
            settings={**parent._settings, **settings},
        )

    @property
    def alive_token(self) -> AliveToken:
        return self._alive_token

    @property
    def is_alive(self) -> bool:
        return self._alive_token.is_alive

    @property
    def service_options(self) -> Mapping[type[ServiceOptions], ServiceOptions]:
        return dict(self._service_options)

    @property
    def settings(self) -> Mapping[str, Any]:
        return dict(self._settings)

    def check_alive(self) -> None:
        self._alive_token.raise_if_invalid()

    def get_service_option(self, option_type: type[ServiceOptionsT]) -> ServiceOptionsT | None:
        self.check_alive()
        option = self._service_options.get(option_type)
        if option is None:
            return None
        return cast(ServiceOptionsT, option)

    def get_setting(self, name: str, default: Any = None) -> Any:
        self.check_alive()
        return self._settings.get(name, default)

    def sandbox_service(self) -> "SandboxService":
        self.check_alive()

        from vercel._internal.http import (
            DEFAULT_TIMEOUT,
            AsyncTransport,
            TransportOptions,
            create_base_async_client,
        )
        from vercel._internal.unstable.sandbox.api_client import SandboxApiClient
        from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
        from vercel._internal.unstable.sandbox.service import SandboxService

        cached = self._service_cache.get(SandboxService)
        if cached is not None:
            return cast(SandboxService, cached)

        options = self.get_service_option(SandboxServiceOptions)
        if options is None:
            options = SandboxServiceOptions()

        transport_options = TransportOptions(
            timeout=DEFAULT_TIMEOUT,
            base_url=options.base_url,
            max_connections=100,
            enable_http2=False,
        )
        api_client = SandboxApiClient(
            base_url=options.base_url,
            credentials_factory=options.credentials_factory,
            transport=AsyncTransport(create_base_async_client(transport_options)),
        )
        service = SandboxService(
            api_client=api_client,
            alive_token=self._alive_token,
            options=options,
            sdk_session=self,
        )
        self._service_cache[SandboxService] = service
        return service

    def close(self) -> None:
        if not self._alive_token.is_alive:
            return

        self._alive_token.invalidate()
        try:
            for service in list(self._service_cache.values()):
                close = getattr(service, "close", None)
                if callable(close):
                    close()
        finally:
            self._service_cache.clear()

    async def aclose(self) -> None:
        if not self._alive_token.is_alive:
            return

        self._alive_token.invalidate()
        try:
            for service in list(self._service_cache.values()):
                aclose = getattr(service, "aclose", None)
                if callable(aclose):
                    result = aclose()
                    if inspect.isawaitable(result):
                        await cast(Awaitable[None], result)
                    continue

                close = getattr(service, "close", None)
                if callable(close):
                    close()
        finally:
            self._service_cache.clear()


class SessionContext:
    """Public `vercel.session(...)` context object."""

    def __init__(
        self,
        *,
        service_options: Sequence[ServiceOptions] | None = None,
        **kwargs: Any,
    ) -> None:
        self._service_options = tuple(service_options) if service_options is not None else None
        self._settings = dict(kwargs)
        self._token: Token[SdkSession | None] | None = None
        self._session: SdkSession | None = None

    def _enter(self) -> "SessionContext":
        if self._token is not None:
            raise RuntimeError("vercel.session(...) contexts cannot be re-entered")

        from vercel._internal.unstable.context import bind_active_session, get_active_session

        session = SdkSession.scoped(
            parent=get_active_session(),
            service_options=self._service_options,
            settings=self._settings,
        )
        self._token = bind_active_session(session)
        self._session = session
        return self

    def __enter__(self) -> "SessionContext":
        return self._enter()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._token is None or self._session is None:
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
        return self._enter()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._token is None or self._session is None:
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


def session(
    *,
    service_options: Sequence[ServiceOptions] | None = None,
    **kwargs: Any,
) -> "SessionContext":
    return SessionContext(service_options=service_options, **kwargs)
