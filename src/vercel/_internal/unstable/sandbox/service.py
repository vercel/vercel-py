"""Sandbox service skeleton."""

from typing import Any

from vercel._internal.unstable.sandbox.api_client import SandboxApiClient
from vercel._internal.unstable.sandbox.models import Sandbox
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
from vercel._internal.unstable.session import AliveToken


class SandboxService:
    def __init__(
        self,
        *,
        api_client: SandboxApiClient,
        alive_token: AliveToken,
        options: SandboxServiceOptions,
    ) -> None:
        self._api_client = api_client
        self._alive_token = alive_token
        self._options = options

    @property
    def api_client(self) -> SandboxApiClient:
        return self._api_client

    @property
    def alive_token(self) -> AliveToken:
        return self._alive_token

    @property
    def options(self) -> SandboxServiceOptions:
        return self._options

    async def create_sandbox(self, **kwargs: Any) -> Sandbox:
        self._alive_token.raise_if_invalid()
        raise NotImplementedError("Sandbox service is not implemented yet")

    async def get_sandbox(self, **kwargs: Any) -> Sandbox:
        self._alive_token.raise_if_invalid()
        raise NotImplementedError("Sandbox service is not implemented yet")

    async def query_sandboxes(self, **kwargs: Any) -> list[Sandbox]:
        self._alive_token.raise_if_invalid()
        raise NotImplementedError("Sandbox service is not implemented yet")
