from __future__ import annotations

from .projects.client import ProjectsClient, AsyncProjectsClient
from .deployments.client import DeploymentsClient, AsyncDeploymentsClient


class Vercel:
    def __init__(
        self,
        *,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        self._access_token = access_token
        self._base_url = base_url
        self._timeout = timeout or 30.0

        self.deployments = DeploymentsClient(
            access_token=access_token, base_url=base_url, timeout=timeout
        )
        self.projects = ProjectsClient(
            access_token=access_token, base_url=base_url, timeout=timeout
        )


class AsyncVercel:
    def __init__(
        self,
        *,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        self._access_token = access_token
        self._base_url = base_url
        self._timeout = timeout or 30.0

        self.deployments = AsyncDeploymentsClient(
            access_token=access_token, base_url=base_url, timeout=timeout
        )
        self.projects = AsyncProjectsClient(
            access_token=access_token, base_url=base_url, timeout=timeout
        )


__all__ = [
    "Vercel",
    "AsyncVercel",
]
