"""Core business logic for Vercel OIDC API."""

from __future__ import annotations

from .._http import (
    AsyncTransport,
    BaseTransport,
    BlockingTransport,
    HTTPConfig,
)
from .types import VercelTokenResponse

BASE_URL = "https://api.vercel.com"
DEFAULT_TIMEOUT = 30.0


class _BaseOidcClient:
    """
    Base class containing shared business logic for OIDC operations.

    All methods are async and use the abstract _transport property for HTTP requests.
    Subclasses must provide a concrete transport implementation.
    """

    _transport: BaseTransport

    async def _fetch_vercel_oidc_token(
        self,
        auth_token: str,
        project_id: str,
        team_id: str | None,
    ) -> VercelTokenResponse | None:
        """Fetch OIDC token from Vercel API.

        Args:
            auth_token: The authentication token.
            project_id: The project ID.
            team_id: Optional team ID.

        Returns:
            VercelTokenResponse with the token, or None if failed.

        Raises:
            RuntimeError: If the API returns an error status.
            TypeError: If the response doesn't contain a valid token.
        """
        params = {"source": "vercel-oidc-refresh"}
        if team_id:
            params["teamId"] = team_id

        resp = await self._transport.send(
            "POST",
            f"/v1/projects/{project_id}/token",
            params=params,
            headers={"authorization": f"Bearer {auth_token}"},
        )

        if not (200 <= resp.status_code < 300):
            raise RuntimeError(
                f"Failed to refresh OIDC token: {resp.status_code} {resp.reason_phrase}"
            )

        data = resp.json()
        if not isinstance(data, dict) or not isinstance(data.get("token"), str):
            raise TypeError("Expected a string-valued token property")

        return VercelTokenResponse(token=data["token"])


class SyncOidcClient(_BaseOidcClient):
    """Sync client for OIDC operations."""

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        config = HTTPConfig(
            base_url=BASE_URL,
            timeout=timeout,
            token=None,  # Token is passed per-request for OIDC
        )
        self._transport = BlockingTransport(config)


class AsyncOidcClient(_BaseOidcClient):
    """Async client for OIDC operations."""

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        config = HTTPConfig(
            base_url=BASE_URL,
            timeout=timeout,
            token=None,  # Token is passed per-request for OIDC
        )
        self._transport = AsyncTransport(config)


__all__ = [
    "SyncOidcClient",
    "AsyncOidcClient",
]
