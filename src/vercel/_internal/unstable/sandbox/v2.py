"""V2 Sandbox API request and response adapters."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from vercel._internal.sandbox.models import ApiNetworkPolicy
from vercel._internal.time import to_ms_int
from vercel._internal.unstable.sandbox.errors import SandboxAPIError
from vercel._internal.unstable.sandbox.models import (
    Sandbox,
    SandboxRoute,
    SandboxStatus,
    Session as SandboxSession,
)
from vercel._internal.unstable.sandbox.params import SandboxCreateParams


class _V2BaseModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True, serialize_by_alias=True)


class _V2CreateSandboxRequest(_V2BaseModel):
    projectId: str
    name: str | None = None
    ports: list[int] | None = None
    source: Any | None = None
    timeout: int | None = None
    resources: Any | None = None
    runtime: str | None = None
    networkPolicy: ApiNetworkPolicy | None = None
    interactive: bool | None = Field(default=None, serialization_alias="__interactive")
    env: dict[str, str] | None = None
    persistent: bool | None = None
    snapshotExpiration: int | None = None
    tags: list[str] | None = None


class _V2SandboxSessionResponse(BaseModel):
    id: str
    status: SandboxStatus | None = None
    memory: int | None = None
    vcpus: int | None = None
    region: str | None = None
    runtime: str | None = None
    timeout: int | None = None
    requestedAt: int | None = None
    startedAt: int | None = None
    cwd: str | None = None
    projectId: str | None = None
    sourceSandboxName: str | None = None
    sourceSnapshotId: str | None = None
    activeCpuDurationMs: int | None = None
    networkTransfer: int | None = None


class _V2SandboxMetadataResponse(BaseModel):
    name: str | None = None
    persistent: bool | None = None
    currentSnapshotId: str | None = None


class _V2SandboxRoute(BaseModel):
    url: str
    subdomain: str
    port: int


class _V2SandboxResponse(BaseModel):
    sandbox: _V2SandboxMetadataResponse
    session: _V2SandboxSessionResponse
    routes: list[_V2SandboxRoute] = Field(default_factory=list)


def build_v2_create_sandbox_body(
    params: SandboxCreateParams,
    *,
    project_id: str,
) -> dict[str, Any]:
    api_network_policy: ApiNetworkPolicy | None = None
    if params.network_policy is not None:
        api_network_policy = ApiNetworkPolicy.from_network_policy(params.network_policy)

    return _V2CreateSandboxRequest(
        projectId=project_id,
        name=params.name,
        ports=params.ports,
        source=params.source,
        timeout=to_ms_int(params.timeout) if params.timeout is not None else None,
        resources=params.resources,
        runtime=params.runtime,
        networkPolicy=api_network_policy,
        interactive=params.interactive,
        env=params.env,
        persistent=params.persistent,
        snapshotExpiration=to_ms_int(params.snapshot_expiration)
        if params.snapshot_expiration is not None
        else None,
        tags=params.tags,
    ).model_dump(by_alias=True, exclude_none=True)


def parse_v2_sandbox_response(data: object) -> Sandbox:
    raw = data if isinstance(data, dict) else None
    try:
        validated = _V2SandboxResponse.model_validate(data)
    except Exception as exc:
        raise SandboxAPIError(
            f"v2 response validation failed: {exc}",
            response=data,
            status_code=200,
            data=data,
        ) from None

    session_data = validated.session
    sandbox_data = validated.sandbox

    current_session = SandboxSession(
        id=session_data.id,
        status=session_data.status,
        memory=session_data.memory,
        vcpus=session_data.vcpus,
        region=session_data.region,
        runtime=session_data.runtime,
        timeout=session_data.timeout,
        requested_at=session_data.requestedAt,
        started_at=session_data.startedAt,
        cwd=session_data.cwd,
        project_id=session_data.projectId,
        source_sandbox_name=session_data.sourceSandboxName,
        source_snapshot_id=session_data.sourceSnapshotId,
        active_cpu_duration_ms=session_data.activeCpuDurationMs,
        network_transfer=session_data.networkTransfer,
    )

    return Sandbox(
        name=sandbox_data.name or session_data.id,
        persistent=sandbox_data.persistent,
        current_snapshot_id=sandbox_data.currentSnapshotId,
        current_session=current_session,
        routes=[
            SandboxRoute(url=route.url, subdomain=route.subdomain, port=route.port)
            for route in validated.routes
        ],
        _raw=raw,
    )


__all__ = ["build_v2_create_sandbox_body", "parse_v2_sandbox_response"]
