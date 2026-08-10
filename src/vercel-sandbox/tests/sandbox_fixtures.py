"""Shared helpers for Sandbox tests."""

import sniffio

from vercel._internal.core.options import ServiceOptions
from vercel.sandbox import SandboxCredentials, SandboxServiceOptions, sync as sandbox_sync


def sandbox_service_options(
    *,
    base_url: str = "https://sandbox.test",
    token: str = "token",
    team_id: str = "team_123",
    project_id: str = "prj_123",
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
            )
        ]

    async def async_credentials_factory() -> SandboxCredentials:
        return credentials

    return [
        SandboxServiceOptions(
            base_url=base_url,
            credentials_factory=async_credentials_factory,
        )
    ]
