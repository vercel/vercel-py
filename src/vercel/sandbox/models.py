from __future__ import annotations

from typing import Literal, TypedDict

from pydantic import BaseModel, ConfigDict, Field

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
    """Completed command with exit code and output."""

    exit_code: int = Field(alias="exitCode")
    stdout: str = ""
    stderr: str = ""


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


class WriteFile(TypedDict):
    """File to write to the sandbox."""

    path: str
    content: bytes


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
