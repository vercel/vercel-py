"""Sandbox v2 API client skeleton."""

from typing import Any

from vercel._internal.unstable.sandbox.options import SandboxServiceOptions


class SandboxApiClient:
    def __init__(self, *, options: SandboxServiceOptions) -> None:
        self._options = options

    @property
    def options(self) -> SandboxServiceOptions:
        return self._options

    async def create_sandbox(self, **kwargs: Any) -> Any:
        raise NotImplementedError("Sandbox v2 API client is not implemented yet")

    async def get_sandbox(self, **kwargs: Any) -> Any:
        raise NotImplementedError("Sandbox v2 API client is not implemented yet")

    async def query_sandboxes(self, **kwargs: Any) -> Any:
        raise NotImplementedError("Sandbox v2 API client is not implemented yet")

    async def destroy_sandbox(self, **kwargs: Any) -> Any:
        raise NotImplementedError("Sandbox v2 API client is not implemented yet")

    async def create_runtime_session(self, **kwargs: Any) -> Any:
        raise NotImplementedError("Sandbox v2 API client is not implemented yet")

    async def destroy_runtime_session(self, **kwargs: Any) -> Any:
        raise NotImplementedError("Sandbox v2 API client is not implemented yet")
