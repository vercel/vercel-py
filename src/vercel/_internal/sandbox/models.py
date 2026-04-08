from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal, TypeAlias, TypedDict

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator

from vercel._internal.sandbox.errors import SandboxError
from vercel._internal.sandbox.network_policy import ApiNetworkPolicy, NetworkPolicy

# Source types for Sandbox.create()


class SandboxStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    ABORTED = "aborted"
    SNAPSHOTTING = "snapshotting"


@dataclass(frozen=True, slots=True)
class SandboxValidationIssue:
    """One local validation issue for sandbox create inputs."""

    path: str
    message: str


class SandboxValidationError(SandboxError):
    """Local sandbox input validation failed before the API request was sent."""

    def __init__(self, issues: list[SandboxValidationIssue]) -> None:
        self.issues = tuple(issues)
        message = "; ".join(f"{issue.path}: {issue.message}" for issue in self.issues)
        super().__init__(message or "Sandbox validation failed")


@dataclass(frozen=True, slots=True)
class GitSource:
    """Git repository source for creating a sandbox."""

    url: str
    depth: int | None = None
    revision: str | None = None
    username: str | None = None
    password: str | None = None
    type: Literal["git"] = field(default="git", init=False)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": self.type, "url": self.url}
        if self.depth is not None:
            payload["depth"] = self.depth
        if self.revision is not None:
            payload["revision"] = self.revision
        if self.username is not None:
            payload["username"] = self.username
        if self.password is not None:
            payload["password"] = self.password
        return payload


@dataclass(frozen=True, slots=True)
class TarballSource:
    """Tarball URL source for creating a sandbox."""

    url: str
    type: Literal["tarball"] = field(default="tarball", init=False)

    def to_payload(self) -> dict[str, Any]:
        return {"type": self.type, "url": self.url}


@dataclass(frozen=True, slots=True)
class SnapshotSource:
    """Snapshot source for creating a sandbox."""

    snapshot_id: str
    type: Literal["snapshot"] = field(default="snapshot", init=False)

    def to_payload(self) -> dict[str, Any]:
        return {"type": self.type, "snapshot_id": self.snapshot_id}


Source = GitSource | TarballSource | SnapshotSource
SourceInput: TypeAlias = Source | Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class Resources:
    """Optional sandbox resource requests."""

    vcpus: int | None = None
    memory: int | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.vcpus is not None:
            payload["vcpus"] = self.vcpus
        if self.memory is not None:
            payload["memory"] = self.memory
        return payload


ResourcesInput: TypeAlias = Resources | Mapping[str, Any]


def parse_source(value: SourceInput | None) -> Source | None:
    if value is None:
        return None
    if isinstance(value, (GitSource, TarballSource, SnapshotSource)):
        _raise_on_issues(_validate_source(value))
        return value
    if not isinstance(value, Mapping):
        raise SandboxValidationError(
            [SandboxValidationIssue(path="source", message="must be a mapping or source dataclass")]
        )

    raw = _normalize_mapping_keys(value, {"snapshotId": "snapshot_id"})
    issues: list[SandboxValidationIssue] = []
    source: Source | None
    source_type = raw.get("type")
    if not isinstance(source_type, str):
        issues.append(SandboxValidationIssue(path="source.type", message="is required"))
        _raise_on_issues(issues)
    if source_type == "git":
        source = _parse_git_source(raw, issues)
    elif source_type == "tarball":
        source = _parse_tarball_source(raw, issues)
    elif source_type == "snapshot":
        source = _parse_snapshot_source(raw, issues)
    else:
        issues.append(
            SandboxValidationIssue(
                path="source.type",
                message="must be one of 'git', 'tarball', or 'snapshot'",
            )
        )
        source = None
    _raise_on_issues(issues)
    assert source is not None
    return source


def parse_resources(value: ResourcesInput | None) -> Resources | None:
    if value is None:
        return None
    if isinstance(value, Resources):
        _raise_on_issues(_validate_resources(value))
        return value
    if not isinstance(value, Mapping):
        raise SandboxValidationError(
            [
                SandboxValidationIssue(
                    path="resources",
                    message="must be a mapping or Resources dataclass",
                )
            ]
        )

    raw = dict(value)
    issues: list[SandboxValidationIssue] = []
    resources = Resources(vcpus=raw.get("vcpus"), memory=raw.get("memory"))
    issues.extend(_validate_resources(resources))
    _raise_on_issues(issues)
    return resources


def _parse_git_source(
    raw: Mapping[str, Any], issues: list[SandboxValidationIssue]
) -> GitSource | None:
    url = raw.get("url")
    source = GitSource(
        url=url if isinstance(url, str) else "",
        depth=raw.get("depth"),
        revision=raw.get("revision"),
        username=raw.get("username"),
        password=raw.get("password"),
    )
    issues.extend(_validate_source(source))
    return source if isinstance(url, str) and bool(url) else None


def _parse_tarball_source(
    raw: Mapping[str, Any], issues: list[SandboxValidationIssue]
) -> TarballSource | None:
    url = raw.get("url")
    if not isinstance(url, str) or not url:
        issues.append(SandboxValidationIssue(path="source.url", message="is required"))
        return None
    return TarballSource(url=url)


def _parse_snapshot_source(
    raw: Mapping[str, Any], issues: list[SandboxValidationIssue]
) -> SnapshotSource | None:
    snapshot_id = raw.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id:
        issues.append(SandboxValidationIssue(path="source.snapshot_id", message="is required"))
        return None
    return SnapshotSource(snapshot_id=snapshot_id)


def _validate_source(source: Source) -> list[SandboxValidationIssue]:
    issues: list[SandboxValidationIssue] = []
    if isinstance(source, GitSource):
        if not isinstance(source.url, str) or not source.url:
            issues.append(SandboxValidationIssue(path="source.url", message="is required"))
        if source.depth is not None and (
            isinstance(source.depth, bool) or not isinstance(source.depth, int)
        ):
            issues.append(SandboxValidationIssue(path="source.depth", message="must be an integer"))
        if (source.username is None) != (source.password is None):
            issues.append(
                SandboxValidationIssue(
                    path="source",
                    message="git username and password must be provided together",
                )
            )
        if source.username is not None and not isinstance(source.username, str):
            issues.append(
                SandboxValidationIssue(path="source.username", message="must be a string")
            )
        if source.password is not None and not isinstance(source.password, str):
            issues.append(
                SandboxValidationIssue(path="source.password", message="must be a string")
            )
        if source.revision is not None and not isinstance(source.revision, str):
            issues.append(
                SandboxValidationIssue(path="source.revision", message="must be a string")
            )
        if (
            isinstance(source.depth, int)
            and not isinstance(source.depth, bool)
            and source.depth <= 0
        ):
            issues.append(
                SandboxValidationIssue(
                    path="source.depth",
                    message="must be a positive integer",
                )
            )
    if isinstance(source, TarballSource) and (not isinstance(source.url, str) or not source.url):
        issues.append(SandboxValidationIssue(path="source.url", message="is required"))
    if isinstance(source, SnapshotSource) and (
        not isinstance(source.snapshot_id, str) or not source.snapshot_id
    ):
        issues.append(SandboxValidationIssue(path="source.snapshot_id", message="is required"))
    return issues


def _validate_resources(resources: Resources) -> list[SandboxValidationIssue]:
    issues: list[SandboxValidationIssue] = []
    if resources.vcpus is not None and (
        isinstance(resources.vcpus, bool) or not isinstance(resources.vcpus, int)
    ):
        issues.append(SandboxValidationIssue(path="resources.vcpus", message="must be an integer"))
    if resources.memory is not None and (
        isinstance(resources.memory, bool) or not isinstance(resources.memory, int)
    ):
        issues.append(SandboxValidationIssue(path="resources.memory", message="must be an integer"))
    if (
        isinstance(resources.vcpus, int)
        and not isinstance(resources.vcpus, bool)
        and resources.vcpus != 1
        and resources.vcpus % 2 != 0
    ):
        issues.append(SandboxValidationIssue(path="resources.vcpus", message="must be even"))
    if (
        isinstance(resources.memory, int)
        and not isinstance(resources.memory, bool)
        and isinstance(resources.vcpus, int)
        and not isinstance(resources.vcpus, bool)
    ):
        expected_memory = resources.vcpus * 2048
        if resources.memory != expected_memory:
            issues.append(
                SandboxValidationIssue(
                    path="resources.memory",
                    message=f"must equal resources.vcpus * 2048 ({expected_memory})",
                )
            )
    return issues


def _normalize_mapping_keys(
    mapping: Mapping[str, Any], aliases: Mapping[str, str]
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in mapping.items():
        normalized[aliases.get(str(key), str(key))] = value
    return normalized


def _raise_on_issues(issues: list[SandboxValidationIssue]) -> None:
    if issues:
        raise SandboxValidationError(issues)


class Sandbox(BaseModel):
    """Sandbox metadata from the API."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    memory: int
    vcpus: int
    region: str
    runtime: str
    timeout: int
    status: SandboxStatus
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
    network_policy_data: ApiNetworkPolicy | None = Field(default=None, alias="networkPolicy")
    _network_policy: NetworkPolicy | None = PrivateAttr(default=None)

    @field_validator("network_policy_data", mode="before")
    @classmethod
    def _parse_network_policy_data(cls, value: object) -> ApiNetworkPolicy | None:
        if value is None:
            return None
        if isinstance(value, ApiNetworkPolicy):
            return value
        if isinstance(value, dict):
            return ApiNetworkPolicy.from_payload(value)
        raise TypeError("networkPolicy must be a mapping")

    def model_post_init(self, __context: object) -> None:
        if self.network_policy_data is None:
            self._network_policy = None
            return
        self._network_policy = self.network_policy_data.to_network_policy()

    @property
    def network_policy(self) -> NetworkPolicy | None:
        return self._network_policy


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
    expires_at: int | None = Field(default=None, alias="expiresAt")
    created_at: int = Field(alias="createdAt")
    updated_at: int = Field(alias="updatedAt")


class SnapshotsResponse(BaseModel):
    """API response containing a list of snapshots."""

    snapshots: list[Snapshot]
    pagination: Pagination


class SnapshotResponse(BaseModel):
    """API response containing a snapshot."""

    snapshot: Snapshot


class CreateSnapshotResponse(BaseModel):
    """API response containing a snapshot and the stopped sandbox."""

    snapshot: Snapshot
    sandbox: Sandbox
