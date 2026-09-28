"""Shared helpers for Sandbox tests."""

import httpx2 as httpx
import sniffio

import vendor.respx as respx
from vercel._internal.core.options import ServiceOptions
from vercel.sandbox import SandboxCredentials, SandboxServiceOptions, sync as sandbox_sync


def sandbox_api_response(
    method: str, path: str, payload: object, *, status: int = 200
) -> respx.Route:
    """Stub one JSON response, keeping its request history available to assertions."""
    return respx.request(method, f"https://sandbox.test{path}").mock(
        return_value=httpx.Response(status, json=payload)
    )


def sandbox_service_options(
    *,
    base_url: str = "https://sandbox.test",
    token: str = "token",
    team_id: str = "team_123",
    project_id: str = "prj_123",
    region: str | None = None,
    sync: bool | None = None,
) -> list[ServiceOptions]:
    """Build Sandbox options for the test's current session mode."""
    credentials = SandboxCredentials(token=token, team_id=team_id, project_id=project_id)

    if sync is None:
        try:
            sniffio.current_async_library()
        except sniffio.AsyncLibraryNotFoundError:
            sync = True
        else:
            sync = False

    if sync:

        def sync_credentials_factory() -> SandboxCredentials:
            return credentials

        return [
            sandbox_sync.SandboxServiceOptions(
                base_url=base_url,
                credentials_factory=sync_credentials_factory,
                region=region,
            )
        ]

    async def async_credentials_factory() -> SandboxCredentials:
        return credentials

    return [
        SandboxServiceOptions(
            base_url=base_url,
            credentials_factory=async_credentials_factory,
            region=region,
        )
    ]
