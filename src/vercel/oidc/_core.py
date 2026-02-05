"""Core business logic for Vercel OIDC API."""

from __future__ import annotations

from .._http import (
    AsyncTransport,
    BaseTransport,
    BlockingTransport,
    create_base_async_client,
    create_base_client,
)
from .types import VercelTokenResponse

BASE_URL = "https://api.vercel.com"
DEFAULT_TIMEOUT = 30.0


class _BaseOidcClient:
    """Base class for OIDC with shared async implementation."""

    _transport: BaseTransport

    async def _fetch_vercel_oidc_token(
        self,
        auth_token: str,
        project_id: str,
        team_id: str | None,
    ) -> VercelTokenResponse | None:
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
    def __init__(self, timeout: float = DEFAULT_TIMEOUT) -> None:
        client = create_base_client(timeout=timeout, base_url=BASE_URL)
        self._transport = BlockingTransport(client)

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> SyncOidcClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class AsyncOidcClient(_BaseOidcClient):
    def __init__(self, timeout: float = DEFAULT_TIMEOUT) -> None:
        client = create_base_async_client(timeout=timeout, base_url=BASE_URL)
        self._transport = AsyncTransport(client)

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> AsyncOidcClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()


__all__ = [
    "SyncOidcClient",
    "AsyncOidcClient",
]
