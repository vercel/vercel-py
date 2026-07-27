"""Shared helpers for Sandbox tests."""

from vercel._internal.core.options import ServiceOptions
from vercel.sandbox import SandboxCredentials, SandboxServiceOptions, sync as sandbox_sync


def sandbox_service_options(
    *,
    base_url: str = "https://sandbox.test",
    credential_value: str = "token",
    team_id: str = "team_123",
    project_id: str = "prj_123",
) -> list[ServiceOptions]:
    """Build matching async and sync Sandbox options for one test session."""
    credentials = SandboxCredentials(credential_value, team_id, project_id)

    async def credentials_factory() -> SandboxCredentials:
        return credentials

    def sync_credentials_factory() -> SandboxCredentials:
        return credentials

    return [
        SandboxServiceOptions(
            base_url=base_url,
            credentials_factory=credentials_factory,
        ),
        sandbox_sync.SandboxServiceOptions(
            base_url=base_url,
            credentials_factory=sync_credentials_factory,
        ),
    ]
