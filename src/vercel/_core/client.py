"""Vercel API clients with namespaced sub-clients."""

from __future__ import annotations

from typing import Any

from .config import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT, ClientConfig
from .deployments import AsyncDeploymentsClient, DeploymentsClient
from .iter_coroutine import iter_coroutine
from .projects import AsyncProjectsClient, ProjectsClient
from .transport import AsyncTransport, BlockingTransport


class VercelClient:
    """Synchronous Vercel SDK client."""

    def __init__(
        self,
        *,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
        default_team_id: str | None = None,
        default_slug: str | None = None,
    ):
        self._config = ClientConfig(
            access_token=access_token,
            base_url=base_url or DEFAULT_API_BASE_URL,
            timeout=timeout or DEFAULT_TIMEOUT,
            default_team_id=default_team_id,
            default_slug=default_slug,
        )
        self._transport = BlockingTransport(self._config)
        self.projects = ProjectsClient(self._transport, self._config)
        self.deployments = DeploymentsClient(self._transport, self._config)

    def close(self) -> None:
        iter_coroutine(self._transport.close())

    def __enter__(self) -> VercelClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class AsyncVercelClient:
    """Asynchronous Vercel SDK client."""

    def __init__(
        self,
        *,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
        default_team_id: str | None = None,
        default_slug: str | None = None,
    ):
        self._config = ClientConfig(
            access_token=access_token,
            base_url=base_url or DEFAULT_API_BASE_URL,
            timeout=timeout or DEFAULT_TIMEOUT,
            default_team_id=default_team_id,
            default_slug=default_slug,
        )
        self._transport = AsyncTransport(self._config)
        self.projects = AsyncProjectsClient(self._transport, self._config)
        self.deployments = AsyncDeploymentsClient(self._transport, self._config)

    async def close(self) -> None:
        await self._transport.close()

    async def __aenter__(self) -> AsyncVercelClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
