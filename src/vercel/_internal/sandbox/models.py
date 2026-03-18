from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from vercel.sandbox.types import NetworkPolicy


_REDACTED_HEADER_VALUE = "<redacted>"

# Source types for Sandbox.create()


class _GitSourceRequired(TypedDict):
    """Required fields for GitSource."""

    type: Literal["git"]
    url: str


class GitSource(_GitSourceRequired, total=False):
    """Git repository source for creating a sandbox."""

    depth: int
    revision: str
    username: str
    password: str


class TarballSource(TypedDict):
    """Tarball URL source for creating a sandbox."""

    type: Literal["tarball"]
    url: str


class SnapshotSource(TypedDict):
    """Snapshot source for creating a sandbox."""

    type: Literal["snapshot"]
    snapshot_id: str


Source = GitSource | TarballSource | SnapshotSource


class Sandbox(BaseModel):
    """Sandbox metadata from the API."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    memory: int
    vcpus: int
    region: str
    runtime: str
    timeout: int
    status: Literal["pending", "running", "stopping", "stopped", "failed", "snapshotting"]
    requested_at: int = Field(alias="requestedAt")
    started_at: int | None = Field(default=None, alias="startedAt")
    requested_stop_at: int | None = Field(default=None, alias="requestedStopAt")
    stopped_at: int | None = Field(default=None, alias="stoppedAt")
    duration: int | None = None
    source_snapshot_id: str | None = Field(default=None, alias="sourceSnapshotId")
    snapshotted_at: int | None = Field(default=None, alias="snapshottedAt")
    created_at: int = Field(alias="createdAt")
    cwd: str
    updated_at: int = Field(alias="updatedAt")
    interactive_port: int | None = Field(default=None, alias="interactivePort")
    network_policy_data: dict[str, Any] | None = Field(default=None, alias="networkPolicy")

    @property
    def network_policy(self) -> NetworkPolicy | None:
        if self.network_policy_data is None:
            return None
        return _from_api_network_policy(self.network_policy_data)


class SandboxRoute(BaseModel):
    """Route mapping for a sandbox port."""

    url: str
    subdomain: str
    port: int


class Pagination(BaseModel):
    """Pagination metadata for list responses."""

    count: int
    next: int | None = None
    prev: int | None = None


class Command(BaseModel):
    """Command metadata from the API."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    name: str
    args: list[str]
    cwd: str
    sandbox_id: str = Field(alias="sandboxId")
    exit_code: int | None = Field(default=None, alias="exitCode")
    started_at: int = Field(alias="startedAt")


class CommandFinished(Command):
    """Completed command with exit code."""

    exit_code: int = Field(alias="exitCode")


class SandboxResponse(BaseModel):
    """API response containing a sandbox."""

    sandbox: Sandbox


class SandboxAndRoutesResponse(SandboxResponse):
    """API response containing a sandbox and its routes."""

    routes: list[SandboxRoute]


class CommandResponse(BaseModel):
    """API response containing a command."""

    command: Command


class CommandFinishedResponse(BaseModel):
    """API response containing a finished command."""

    command: CommandFinished


class EmptyResponse(BaseModel):
    """Empty API response."""

    pass


class LogLine(BaseModel):
    """Log line from command output."""

    stream: Literal["stdout", "stderr"]
    data: str


class SandboxesResponse(BaseModel):
    """API response containing a list of sandboxes."""

    sandboxes: list[Sandbox]
    pagination: Pagination


class _WriteFileRequired(TypedDict):
    """File to write to the sandbox."""

    path: str
    content: bytes


class WriteFile(_WriteFileRequired, total=False):
    """File to write to the sandbox."""

    mode: int


class Snapshot(BaseModel):
    """Snapshot metadata from the API."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    source_sandbox_id: str = Field(alias="sourceSandboxId")
    region: str
    status: Literal["created", "deleted", "failed"]
    size_bytes: int = Field(alias="sizeBytes")
    expires_at: int = Field(alias="expiresAt")
    created_at: int = Field(alias="createdAt")
    updated_at: int = Field(alias="updatedAt")


class SnapshotResponse(BaseModel):
    """API response containing a snapshot."""

    snapshot: Snapshot


class CreateSnapshotResponse(BaseModel):
    """API response containing a snapshot and the stopped sandbox."""

    snapshot: Snapshot
    sandbox: Sandbox


def _subnets_from_api(network_policy: Mapping[str, Any]) -> dict[str, list[str]]:
    allowed = network_policy.get("allowedCIDRs") if "allowedCIDRs" in network_policy else None
    denied = network_policy.get("deniedCIDRs") if "deniedCIDRs" in network_policy else None

    subnet_payload: dict[str, list[str]] = {}
    if allowed is not None:
        subnet_payload["allow"] = list(allowed)
    if denied is not None:
        subnet_payload["deny"] = list(denied)
    return subnet_payload


def _from_api_network_policy(network_policy: Mapping[str, Any]) -> NetworkPolicy:
    mode = network_policy.get("mode")
    if mode in ("allow-all", "deny-all"):
        return cast("NetworkPolicy", mode)

    allowed_domains = list(network_policy.get("allowedDomains") or [])
    injection_rules = list(network_policy.get("injectionRules") or [])
    subnets = _subnets_from_api(network_policy)

    if not injection_rules:
        list_policy_result: dict[str, Any] = {"allow": allowed_domains}
        if subnets:
            list_policy_result["subnets"] = subnets
        return cast("NetworkPolicy", list_policy_result)

    allow: dict[str, list[dict[str, list[dict[str, dict[str, str]]]]]] = {
        domain: [] for domain in allowed_domains
    }
    for rule in injection_rules:
        domain = rule.get("domain")
        if not isinstance(domain, str):
            continue

        allow.setdefault(domain, [])
        header_names = rule.get("headerNames")
        if header_names is None and isinstance(rule.get("headers"), Mapping):
            header_names = list(rule["headers"].keys())
        header_names = header_names or []
        headers = {name: _REDACTED_HEADER_VALUE for name in header_names if isinstance(name, str)}
        if not headers:
            continue
        allow[domain].append({"transform": [{"headers": headers}]})

    record_policy_result: dict[str, Any] = {"allow": allow}
    if subnets:
        record_policy_result["subnets"] = subnets
    return cast("NetworkPolicy", record_policy_result)
