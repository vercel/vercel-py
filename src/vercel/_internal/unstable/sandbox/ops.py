"""Sandbox ops layer for unstable APIs."""

from __future__ import annotations

from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field

from vercel._internal.http import JSONBody
from vercel._internal.http.transport import BaseTransport
from vercel._internal.sandbox.models import ApiNetworkPolicy
from vercel._internal.sandbox.time import to_ms_int
from vercel._internal.unstable.sandbox.auth import (
    SandboxCredentialProvider,
    SandboxCredentials,
    SyncSandboxCredentialProvider,
    resolve_sandbox_credentials,
    resolve_sync_sandbox_credentials,
)
from vercel._internal.unstable.sandbox.request_client import (
    _USER_AGENT,
    SyncUnstableSandboxRequestClient,
    UnstableSandboxRequestClient,
)
from vercel._internal.unstable.sandbox.types import (
    Sandbox,
    SandboxAPIError,
    SandboxCreateParams,
    SandboxOptions,
    SandboxStatus,
    Session as SandboxSession,
)


class _V2BaseModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True, serialize_by_alias=True)


class V2CreateSandboxRequest(_V2BaseModel):
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


class V2SandboxSessionResponse(BaseModel):
    id: str
    status: str | None = None
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


class V2SandboxMetadataResponse(BaseModel):
    name: str | None = None
    persistent: bool | None = None
    currentSnapshotId: str | None = None


class V2SandboxRoute(BaseModel):
    url: str
    subdomain: str
    port: int


class V2SandboxResponse(BaseModel):
    sandbox: V2SandboxMetadataResponse
    session: V2SandboxSessionResponse
    routes: list[V2SandboxRoute] = []


def _parse_v2_sandbox_response(data: dict[str, Any]) -> Sandbox:
    try:
        validated = V2SandboxResponse.model_validate(data)
    except Exception as exc:
        raise SandboxAPIError(
            f"v2 response validation failed: {exc}",
            response=data,
            status_code=200,
            data=data,
        ) from None

    session_data = validated.session
    sandbox_data = validated.sandbox
    routes = validated.routes

    status = SandboxStatus(session_data.status) if session_data.status else None

    current_session = SandboxSession(
        id=session_data.id,
        status=status,
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
        routes=[r.model_dump() for r in routes] if routes else None,
        _raw=data,
    )


class BaseUnstableSandboxOpsClient:
    """Shared Sandbox ops client for unstable APIs."""

    def __init__(
        self,
        *,
        options: SandboxOptions | None = None,
    ) -> None:
        self._options = options or SandboxOptions()

    async def _resolve_credentials(self) -> SandboxCredentials:
        raise NotImplementedError

    def _get_request_client(self) -> Any:
        raise NotImplementedError

    async def create(self, params: SandboxCreateParams) -> Sandbox:
        credentials = await self._resolve_credentials()

        api_network_policy: ApiNetworkPolicy | None = None
        if params.network_policy is not None:
            api_network_policy = ApiNetworkPolicy.from_network_policy(params.network_policy)

        body = V2CreateSandboxRequest(
            projectId=credentials.project_id,
            name=params.name,
            ports=params.ports,
            source=params.source,
            timeout=to_ms_int(params.timeout) if params.timeout is not None else None,
            resources=params.resources,
            runtime=params.runtime,
            networkPolicy=api_network_policy,
            interactive=params.interactive,
            env=params.env,
        ).model_dump(by_alias=True, exclude_none=True)

        if params.persistent is not None:
            body["persistent"] = params.persistent
        if params.snapshot_expiration is not None:
            body["snapshotExpiration"] = to_ms_int(params.snapshot_expiration)
        if params.tags is not None:
            body["tags"] = params.tags

        data = await self._get_request_client().request_json(
            "POST",
            "/v2/sandboxes",
            headers={
                "user-agent": _USER_AGENT,
                "authorization": f"Bearer {credentials.token}",
                "content-type": "application/json",
            },
            query={"teamId": credentials.team_id},
            body=JSONBody(body),
        )

        return _parse_v2_sandbox_response(data)

    async def get_sandbox(self, name: str) -> Sandbox:
        credentials = await self._resolve_credentials()
        data = await self._get_request_client().request_json(
            "GET",
            f"/v2/sandboxes/{name}",
            headers={
                "user-agent": _USER_AGENT,
                "authorization": f"Bearer {credentials.token}",
            },
            query={"teamId": credentials.team_id},
            body=JSONBody({}),
        )
        return _parse_v2_sandbox_response(data)


class UnstableSandboxOpsClient(BaseUnstableSandboxOpsClient):
    """Async Sandbox ops client for unstable APIs."""

    def __init__(
        self,
        *,
        options: SandboxOptions | None = None,
        transport: BaseTransport | None = None,
    ) -> None:
        super().__init__(options=options)
        self._request_client = UnstableSandboxRequestClient(options=options, transport=transport)

    def _get_request_client(self) -> UnstableSandboxRequestClient:
        return self._request_client

    async def _resolve_credentials(self) -> SandboxCredentials:
        return await resolve_sandbox_credentials(
            credential_provider=cast(
                SandboxCredentialProvider | None,
                self._options.credential_provider,
            ),
            project_id=self._options.project_id,
            team_id=self._options.team_id,
        )

    async def aclose(self) -> None:
        await self._request_client.aclose()


class SyncUnstableSandboxOpsClient(BaseUnstableSandboxOpsClient):
    """Sync Sandbox ops client with async-shaped methods for iter_coroutine()."""

    def __init__(
        self,
        *,
        options: SandboxOptions | None = None,
        transport: BaseTransport | None = None,
    ) -> None:
        super().__init__(options=options)
        self._request_client = SyncUnstableSandboxRequestClient(
            options=options, transport=transport
        )

    def _get_request_client(self) -> SyncUnstableSandboxRequestClient:
        return self._request_client

    async def _resolve_credentials(self) -> SandboxCredentials:
        return resolve_sync_sandbox_credentials(
            credential_provider=cast(
                SyncSandboxCredentialProvider | None,
                self._options.credential_provider,
            ),
            project_id=self._options.project_id,
            team_id=self._options.team_id,
        )

    def close(self) -> None:
        self._request_client.close()
