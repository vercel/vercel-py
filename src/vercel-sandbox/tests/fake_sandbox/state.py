"""Records held by the fake Sandbox API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import network_policy, wire
from .filesystem import FakeFilesystem
from .programs import ProgramResult, Stream

_GONE_STATUSES = frozenset({"stopping", "stopped", "failed", "aborted", "snapshotting"})


@dataclass
class FakeSnapshot:
    id: str
    sandbox_name: str
    source_session_id: str
    region: str
    files: FakeFilesystem
    created_at: int
    image: str | None = None
    status: str = "created"
    expires_at: int | None = None
    creation_method: str = "manual"
    parent_id: str | None = None
    updated_at: int = 0

    def payload(self) -> wire.JSON:
        return wire.snapshot_payload(
            id=self.id,
            source_session_id=self.source_session_id,
            region=self.region,
            status=self.status,
            size_bytes=self.files.size(),
            expires_at=self.expires_at,
            created_at=self.created_at,
            updated_at=self.updated_at or self.created_at,
            creation_method=self.creation_method,
            parent_id=self.parent_id,
        )


@dataclass
class FakeDrive:
    id: str
    name: str
    project_id: str
    region: str
    max_size_bytes: int
    created_at: int
    files: FakeFilesystem = field(default_factory=FakeFilesystem)
    current_session_id: str | None = None
    current_sandbox_name: str | None = None
    updated_at: int = 0

    def payload(self) -> wire.JSON:
        return wire.drive_payload(
            id=self.id,
            name=self.name,
            project_id=self.project_id,
            region=self.region,
            max_size_bytes=self.max_size_bytes,
            current_session_id=self.current_session_id,
            current_sandbox_name=self.current_sandbox_name,
            created_at=self.created_at,
            updated_at=self.updated_at or self.created_at,
        )


@dataclass
class FakeCommand:
    id: str
    session_id: str
    name: str
    args: list[str]
    cwd: str
    started_at: int
    result: ProgramResult
    timeout_ms: int | None = None
    killed_at: int | None = None
    kill_signal: int | None = None

    @property
    def natural_end(self) -> int:
        return self.started_at + round(self.result.duration * 1000)

    @property
    def end(self) -> int:
        """When the command exits: naturally, by its timeout, or when killed."""
        ends = [self.natural_end]
        if self.timeout_ms is not None:
            ends.append(self.started_at + self.timeout_ms)
        if self.killed_at is not None:
            ends.append(self.killed_at)
        return min(ends)

    def exit_code(self) -> int:
        if self.killed_at is not None and self.killed_at < self.natural_end:
            return 128 + (self.kill_signal or 15)
        if self.timeout_ms is not None and self.started_at + self.timeout_ms < self.natural_end:
            return 137
        return self.result.exit_code

    def output(self) -> list[tuple[Stream, str]]:
        # A command stopped early printed nothing the fake can attribute to it.
        if self.end < self.natural_end:
            return []
        return self.result.output

    def payload(self, *, now: int, finished: bool = False) -> wire.JSON:
        done = finished or now >= self.end
        return wire.command_payload(
            id=self.id,
            name=self.name,
            args=self.args,
            cwd=self.cwd,
            session_id=self.session_id,
            exit_code=self.exit_code() if done else None,
            started_at=self.started_at,
            duration_ms=(self.end - self.started_at) if done else None,
        )


@dataclass
class FakeSession:
    id: str
    sandbox_name: str
    project_id: str
    region: str
    memory: int
    vcpus: int
    timeout_ms: int
    cwd: str
    requested_at: int
    running_at: int
    fs: FakeFilesystem
    network_policy: wire.JSON | None = None
    network_id: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    source_snapshot_id: str | None = None
    status: str = "pending"
    stopped_at: int | None = None
    updated_at: int = 0
    commands: dict[str, FakeCommand] = field(default_factory=dict)

    def refresh(self, now: int) -> None:
        """Move through the lifecycle the API would have reached by ``now``."""
        if self.status == "pending" and now >= self.running_at:
            self.status = "running"
        if self.status == "running" and now >= self.running_at + self.timeout_ms:
            self.status = "stopped"
            self.stopped_at = self.running_at + self.timeout_ms

    @property
    def gone(self) -> bool:
        return self.status in _GONE_STATUSES

    def payload(self) -> wire.JSON:
        return wire.session_payload(
            id=self.id,
            sandbox_name=self.sandbox_name,
            project_id=self.project_id,
            status=self.status,
            cwd=self.cwd,
            region=self.region,
            memory=self.memory,
            vcpus=self.vcpus,
            timeout=self.timeout_ms,
            requested_at=self.requested_at,
            started_at=self.running_at if self.status != "pending" else None,
            stopped_at=self.stopped_at,
            created_at=self.requested_at,
            updated_at=self.updated_at or self.requested_at,
            network_policy=self.network_policy,
            network_id=self.network_id,
            source_snapshot_id=self.source_snapshot_id,
        )


@dataclass
class FakeSandbox:
    name: str
    project_id: str
    persistent: bool
    image: str | None
    region: str
    failover_regions: list[str]
    vcpus: int
    memory: int
    timeout_ms: int
    created_at: int
    current_session_id: str = ""
    cwd: str = "/vercel/sandbox"
    ports: list[int] = field(default_factory=list)
    system_ports: list[int] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)
    mounts: dict[str, dict[str, str]] | None = None
    network_policy: wire.JSON | None = None
    network_id: str | None = None
    snapshot_expiration: int | None = None
    keep_last_snapshots: wire.JSON | None = None
    current_snapshot_id: str | None = None
    source_snapshot_id: str | None = None
    updated_at: int = 0
    status_updated_at: int = 0
    private_parameters: dict[str, Any] = field(default_factory=dict)

    def payload(self, session: FakeSession | None) -> wire.JSON:
        return wire.sandbox_payload(
            name=self.name,
            current_session_id=self.current_session_id,
            status="stopped" if session is None else session.status,
            persistent=self.persistent,
            image=self.image,
            region=self.region,
            failover_regions=[r for r in self.failover_regions if r != self.region],
            vcpus=self.vcpus,
            memory=self.memory,
            timeout=self.timeout_ms,
            cwd=self.cwd,
            created_at=self.created_at,
            updated_at=self.updated_at or self.created_at,
            status_updated_at=self.status_updated_at or self.created_at,
            current_snapshot_id=self.current_snapshot_id,
            snapshot_expiration=self.snapshot_expiration,
            keep_last_snapshots=self.keep_last_snapshots,
            network_policy=(
                None
                if self.network_policy is None
                else network_policy.sandbox_readback(self.network_policy)
            ),
            network_id=self.network_id,
            tags=self.tags,
            mounts=self.mounts,
            expires_at=(
                session.running_at + session.timeout_ms
                if session is not None and session.status == "running"
                else None
            ),
        )

    def routes(self, *, include_system: bool = False) -> list[wire.JSON]:
        routes = [
            wire.route_payload(port=port, subdomain=f"{self.name}-{port}") for port in self.ports
        ]
        if include_system:
            routes.extend(
                wire.route_payload(port=port, subdomain=f"{self.name}-system-{port}", system=True)
                for port in self.system_ports
            )
        return routes


__all__ = ["FakeCommand", "FakeDrive", "FakeSandbox", "FakeSession", "FakeSnapshot"]
