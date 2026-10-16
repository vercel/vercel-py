"""Sandbox API response payloads.

The shapes follow the API's own DTO mappers (`map-named-sandbox-to-dto`,
`map-session-to-dto`, and friends): fields the API leaves undefined are
omitted rather than sent as ``null``. The fake builds every response from
these, and tests that hand-craft a response build it from them too, so there
is one place to update when the wire format moves.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

DEFAULT_IMAGE = "vercel/sandbox/universal:latest"

JSON = dict[str, Any]


def _defined(**fields: Any) -> JSON:
    return {key: value for key, value in fields.items() if value is not None}


def sandbox_payload(
    *,
    name: str = "preview",
    current_session_id: str = "sbx_1",
    status: str = "running",
    persistent: bool = True,
    image: str | None = DEFAULT_IMAGE,
    region: str | None = "iad1",
    failover_regions: Sequence[str] | None = (),
    vcpus: int | None = 2,
    memory: int | None = 4096,
    timeout: int | None = 300_000,
    cwd: str | None = "/vercel/sandbox",
    created_at: int = 1,
    updated_at: int = 1,
    status_updated_at: int | None = None,
    current_snapshot_id: str | None = None,
    snapshot_expiration: int | None = None,
    keep_last_snapshots: Mapping[str, Any] | None = None,
    network_policy: Mapping[str, Any] | None = None,
    network_id: str | None = None,
    tags: Mapping[str, str] | None = None,
    mounts: Mapping[str, Mapping[str, str]] | None = None,
    expires_at: int | None = None,
) -> JSON:
    return _defined(
        name=name,
        currentSnapshotId=current_snapshot_id,
        currentSessionId=current_session_id,
        status=status,
        statusUpdatedAt=created_at if status_updated_at is None else status_updated_at,
        persistent=persistent,
        region=region,
        failoverRegions=None if failover_regions is None else list(failover_regions),
        vcpus=vcpus,
        memory=memory,
        image=image,
        timeout=timeout,
        snapshotExpiration=snapshot_expiration,
        keepLastSnapshots=None if keep_last_snapshots is None else dict(keep_last_snapshots),
        networkPolicy=None if network_policy is None else dict(network_policy),
        networkId=network_id,
        cwd=cwd,
        tags=dict(tags) if tags else None,
        mounts=None if mounts is None else {path: dict(m) for path, m in mounts.items()},
        createdAt=created_at,
        updatedAt=updated_at,
        expiresAt=expires_at,
    )


def session_payload(
    *,
    id: str = "sbx_1",
    sandbox_name: str = "preview",
    project_id: str = "prj_123",
    status: str = "running",
    cwd: str = "/vercel/sandbox",
    region: str = "iad1",
    memory: int = 4096,
    vcpus: int = 2,
    timeout: int = 300_000,
    requested_at: int = 1,
    started_at: int | None = None,
    stopped_at: int | None = None,
    created_at: int = 1,
    updated_at: int = 1,
    network_policy: Mapping[str, Any] | None = None,
    network_id: str | None = None,
    source_snapshot_id: str | None = None,
) -> JSON:
    return _defined(
        id=id,
        memory=memory,
        vcpus=vcpus,
        region=region,
        timeout=timeout,
        status=status,
        requestedAt=requested_at,
        startedAt=started_at,
        stoppedAt=stopped_at,
        cwd=cwd,
        sourceSnapshotId=source_snapshot_id,
        createdAt=created_at,
        updatedAt=updated_at,
        networkPolicy=None if network_policy is None else dict(network_policy),
        networkId=network_id,
        sourceSandboxName=sandbox_name,
        projectId=project_id,
    )


def route_payload(*, port: int, subdomain: str, system: bool = False) -> JSON:
    route: JSON = {"url": f"https://{subdomain}.vercel.run", "subdomain": subdomain, "port": port}
    if system:
        route["system"] = True
    return route


def sandbox_response(
    *,
    sandbox: JSON | None = None,
    session: JSON | None = None,
    routes: Sequence[JSON] | None = (),
    resumed: bool | None = None,
) -> JSON:
    """The `{sandbox, session, routes}` envelope create, get, and fork return."""
    return _defined(
        sandbox=sandbox_payload() if sandbox is None else sandbox,
        session=session_payload() if session is None else session,
        routes=None if routes is None else list(routes),
        resumed=resumed,
    )


def command_payload(
    *,
    id: str = "cmd_1",
    name: str = "true",
    args: Sequence[str] = (),
    cwd: str = "/vercel/sandbox",
    session_id: str = "sbx_1",
    exit_code: int | None = None,
    started_at: int = 1,
    duration_ms: int | None = None,
) -> JSON:
    return _defined(
        id=id,
        name=name,
        args=list(args),
        cwd=cwd,
        sessionId=session_id,
        startedAt=started_at,
        durationMs=duration_ms,
    ) | {"exitCode": exit_code}


def snapshot_payload(
    *,
    id: str = "snap_1",
    source_session_id: str = "sbx_1",
    region: str = "iad1",
    regions: Sequence[str] | None = None,
    status: str = "created",
    size_bytes: int = 0,
    expires_at: int | None = None,
    created_at: int = 1,
    updated_at: int = 1,
    last_used_at: int | None = None,
    creation_method: str | None = "manual",
    parent_id: str | None = None,
) -> JSON:
    return _defined(
        id=id,
        sourceSessionId=source_session_id,
        region=region,
        regions=[region] if regions is None else list(regions),
        status=status,
        sizeBytes=size_bytes,
        expiresAt=expires_at,
        createdAt=created_at,
        updatedAt=updated_at,
        lastUsedAt=created_at if last_used_at is None else last_used_at,
        creationMethod=creation_method,
        parentId=parent_id,
    )


def drive_payload(
    *,
    id: str = "drive_1",
    name: str = "cache",
    project_id: str = "prj_123",
    region: str = "iad1",
    max_size_bytes: int = 1024**3,
    current_session_id: str | None = None,
    current_sandbox_name: str | None = None,
    created_at: int = 1,
    updated_at: int = 1,
) -> JSON:
    return _defined(
        id=id,
        name=name,
        projectId=project_id,
        region=region,
        maxSizeBytes=max_size_bytes,
        currentSessionId=current_session_id,
        currentSandboxName=current_sandbox_name,
        createdAt=created_at,
        updatedAt=updated_at,
    )


def error_payload(code: str, message: str) -> JSON:
    return {"error": {"code": code, "message": message}}


def log_record(stream: str, data: str) -> JSON:
    return {"stream": stream, "data": data}


__all__ = [
    "DEFAULT_IMAGE",
    "command_payload",
    "drive_payload",
    "error_payload",
    "log_record",
    "route_payload",
    "sandbox_payload",
    "sandbox_response",
    "session_payload",
    "snapshot_payload",
]
