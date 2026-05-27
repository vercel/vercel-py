"""Awaitable/context-manager operation skeletons for Sandbox flows."""

from types import TracebackType
from typing import Any

from vercel._internal.unstable.context import get_active_session
from vercel._internal.unstable.sandbox.models import Sandbox
from vercel._internal.unstable.session import SdkSession


class CreateSandboxOperation:
    def __init__(self, *, session: SdkSession, kwargs: dict[str, Any]) -> None:
        self._session = session
        self._kwargs = kwargs

    async def _run(self) -> Sandbox:
        return await self._session.sandbox_service().create_sandbox(**self._kwargs)

    def __await__(self) -> Any:
        return self._run().__await__()

    async def __aenter__(self) -> Sandbox:
        return await self._run()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


def create_sandbox_operation(**kwargs: Any) -> CreateSandboxOperation:
    return CreateSandboxOperation(session=get_active_session(), kwargs=kwargs)
