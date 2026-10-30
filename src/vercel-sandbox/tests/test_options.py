from datetime import timedelta
from types import SimpleNamespace

import httpx2 as httpx
import pytest

import vendor.respx as respx
from vercel import sandbox
from vercel.sandbox._internal.options import (
    SandboxCredentials,
    SandboxServiceOptions,
    SyncSandboxServiceOptions,
)


@pytest.mark.parametrize(
    "options_type", [SandboxServiceOptions, sandbox.sync.SandboxServiceOptions]
)
@pytest.mark.parametrize("env_region", [None, "", "iad1"])
def test_options_region_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
    options_type: type[SandboxServiceOptions] | type[sandbox.sync.SandboxServiceOptions],
    env_region: str | None,
) -> None:
    if env_region is None:
        monkeypatch.delenv("VERCEL_REGION", raising=False)
    else:
        monkeypatch.setenv("VERCEL_REGION", env_region)
    assert options_type().region is None
    assert options_type(region=None).region is None
    assert options_type(region="sfo1").region == "sfo1"
    assert options_type(region=sandbox.CURRENT_REGION).region is sandbox.CURRENT_REGION


def test_current_region_is_public_and_distinct_from_strings() -> None:
    assert sandbox.sync.CURRENT_REGION is sandbox.CURRENT_REGION
    assert sandbox.sync.CurrentRegion is sandbox.CurrentRegion
    assert sandbox.sync.RegionInput is sandbox.RegionInput
    assert isinstance(sandbox.CURRENT_REGION, sandbox.CurrentRegion)
    assert not isinstance(sandbox.CURRENT_REGION, str)


@pytest.mark.parametrize("options_cls", [SandboxServiceOptions, SyncSandboxServiceOptions])
@pytest.mark.parametrize("region_env", [None, "iad1", ""])
@pytest.mark.parametrize("explicit_region", [None, "", "sfo1", sandbox.CURRENT_REGION])
def test_options_constructor_and_model_validate_share_defaults(
    monkeypatch: pytest.MonkeyPatch,
    options_cls: type[SandboxServiceOptions] | type[SyncSandboxServiceOptions],
    region_env: str | None,
    explicit_region: sandbox.RegionInput,
) -> None:
    if region_env is None:
        monkeypatch.delenv("VERCEL_REGION", raising=False)
    else:
        monkeypatch.setenv("VERCEL_REGION", region_env)

    defaults = options_cls.model_validate({})
    assert options_cls() == defaults
    assert (
        options_cls(
            base_url=None,
            credentials_factory=None,
            file_transfer_timeout=None,
            region=None,
        )
        == defaults
    )
    assert defaults.region is None

    configured = options_cls.model_validate({"region": explicit_region})
    assert configured == options_cls(region=explicit_region)
    assert configured.region == explicit_region


def test_options_equality_preserves_custom_credential_factory_identity() -> None:
    assert SandboxServiceOptions() == SandboxServiceOptions()
    assert SandboxServiceOptions(
        base_url="https://example.com",
        file_transfer_timeout=timedelta(seconds=30),
    ) == SandboxServiceOptions(
        base_url="https://example.com",
        file_transfer_timeout=timedelta(seconds=30),
    )

    async def first_factory() -> SandboxCredentials:
        return SandboxCredentials("token", "team", "project")

    async def second_factory() -> SandboxCredentials:
        return SandboxCredentials("token", "team", "project")

    assert SandboxServiceOptions(credentials_factory=first_factory) != SandboxServiceOptions(
        credentials_factory=second_factory
    )


@respx.mock
@pytest.mark.asyncio
async def test_public_request_uses_default_oidc_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vercel.oidc import credentials

    monkeypatch.setattr(
        credentials,
        "get_credentials",
        lambda: SimpleNamespace(token="token", team_id="team_123", project_id="prj_123"),
    )
    route = respx.get("https://vercel.com/api/v2/sandboxes/snapshots/snap_123").mock(
        return_value=httpx.Response(
            200,
            json={
                "snapshot": {
                    "id": "snap_123",
                    "sourceSessionId": "sbx_123",
                    "region": "iad1",
                    "status": "created",
                    "sizeBytes": 1024,
                    "createdAt": 1,
                    "updatedAt": 2,
                }
            },
        )
    )

    snapshot = await sandbox.get_snapshot(snapshot_id="snap_123")

    assert snapshot.id == "snap_123"
    request = route.calls.last.request
    assert request.headers["authorization"] == "Bearer token"
    assert request.url.params["teamId"] == "team_123"
