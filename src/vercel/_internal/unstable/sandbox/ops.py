"""Sandbox ops layer for unstable APIs."""

from __future__ import annotations

from typing import Any, cast

from vercel._internal.http import JSONBody
from vercel._internal.http.transport import BaseTransport
from vercel._internal.sandbox.models import CreateSandboxRequest, SandboxStatus
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
    Session as SandboxSession,
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
        body = CreateSandboxRequest(
            project_id=credentials.project_id,
            name=params.name,
            ports=params.ports,
            source=params.source,
            timeout=params.timeout,
            resources=params.resources,
            runtime=params.runtime,
            network_policy=params.network_policy,
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

        sandbox_data = data.get("sandbox") or {}
        session_data = data.get("session") or {}
        routes = data.get("routes")

        if not sandbox_data or not session_data:
            raise SandboxAPIError(
                "v2 create response missing required sandbox or session data",
                response=data,
                status_code=200,
                data=data,
            )

        status_str = session_data.get("status")
        status = SandboxStatus(status_str) if status_str else None

        current_session = SandboxSession(
            id=session_data.get("id", ""),
            status=status,
            memory=session_data.get("memory"),
            vcpus=session_data.get("vcpus"),
            region=session_data.get("region"),
            runtime=session_data.get("runtime"),
            timeout=session_data.get("timeout"),
            requested_at=session_data.get("requestedAt"),
            started_at=session_data.get("startedAt"),
            cwd=session_data.get("cwd"),
            project_id=session_data.get("projectId"),
            source_sandbox_name=session_data.get("sourceSandboxName"),
            source_snapshot_id=session_data.get("sourceSnapshotId"),
            active_cpu_duration_ms=session_data.get("activeCpuDurationMs"),
            network_transfer=session_data.get("networkTransfer"),
        )

        return Sandbox(
            name=sandbox_data.get("name", session_data.get("id", "")),
            persistent=sandbox_data.get("persistent"),
            current_snapshot_id=sandbox_data.get("currentSnapshotId"),
            current_session=current_session,
            routes=routes,
            _raw=data,
        )


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
