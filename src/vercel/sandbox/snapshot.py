from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.sandbox import AsyncSandboxOpsClient, SyncSandboxOpsClient
from vercel._internal.sandbox.models import Snapshot as SnapshotModel
from vercel._internal.sandbox.pagination import SnapshotListParams
from vercel._internal.sandbox.snapshot import (
    MIN_SNAPSHOT_EXPIRATION_MS as _MIN_SNAPSHOT_EXPIRATION_MS,
    SnapshotExpiration as _SnapshotExpiration,
)

from ..oidc import Credentials, get_credentials

MIN_SNAPSHOT_EXPIRATION_MS = _MIN_SNAPSHOT_EXPIRATION_MS
SnapshotExpiration = _SnapshotExpiration


@dataclass
class AsyncSnapshot:
    """A Snapshot is a saved state of a Sandbox that can be used to create new Sandboxes."""

    client: AsyncSandboxOpsClient
    snapshot: SnapshotModel

    @property
    def snapshot_id(self) -> str:
        """Unique ID of this snapshot."""
        return self.snapshot.id

    @property
    def source_sandbox_id(self) -> str:
        """The ID of the sandbox from which this snapshot was created."""
        return self.snapshot.source_sandbox_id

    @property
    def status(self) -> Literal["created", "deleted", "failed"]:
        """The status of the snapshot."""
        return self.snapshot.status

    @property
    def size_bytes(self) -> int:
        """Size of the snapshot in bytes."""
        return self.snapshot.size_bytes

    @property
    def created_at(self) -> int:
        """Timestamp when the snapshot was created."""
        return self.snapshot.created_at

    @property
    def expires_at(self) -> int | None:
        """Timestamp when the snapshot expires, or None for no expiration."""
        return self.snapshot.expires_at

    @staticmethod
    async def get(
        *,
        snapshot_id: str,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> AsyncSnapshot:
        """Retrieve an existing snapshot by ID."""
        creds: Credentials = get_credentials(token=token, project_id=project_id, team_id=team_id)
        client = AsyncSandboxOpsClient(team_id=creds.team_id, token=creds.token)
        resp = await client.get_snapshot(snapshot_id=snapshot_id)
        return AsyncSnapshot(client=client, snapshot=resp.snapshot)

    @staticmethod
    def list(
        *,
        limit: int | None = None,
        _internal_page_size: int | None = None,
        since: datetime | int | None = None,
        until: datetime | int | None = None,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> AsyncIterator[SnapshotModel]:
        """List snapshots as an async iterable of snapshot models.

        `_internal_page_size` is a private override for the backend request
        size used during internal pagination. It exists for internal debugging
        and examples and is not part of the supported public contract.
        """
        creds: Credentials = get_credentials(token=token, project_id=project_id, team_id=team_id)
        params = SnapshotListParams(
            project_id=creds.project_id,
            limit=limit,
            internal_page_size=_internal_page_size,
            since=since,
            until=until,
        )

        async def iter_snapshots() -> AsyncIterator[SnapshotModel]:
            current_params = params
            async with AsyncSandboxOpsClient(team_id=creds.team_id, token=creds.token) as client:
                while True:
                    response = await client.list_snapshots(
                        project_id=current_params.project_id,
                        limit=current_params.request_limit,
                        since=current_params.since,
                        until=current_params.until,
                    )
                    snapshots = response.snapshots[: current_params.remaining]
                    for snapshot in snapshots:
                        yield snapshot
                    if response.pagination.next is None:
                        return
                    if (
                        current_params.remaining is not None
                        and len(snapshots) >= current_params.remaining
                    ):
                        return
                    current_params = current_params.with_until(
                        response.pagination.next,
                        yielded_count=len(snapshots),
                    )

        return iter_snapshots()

    async def delete(self) -> None:
        """Delete this snapshot."""
        resp = await self.client.delete_snapshot(snapshot_id=self.snapshot.id)
        self.snapshot = resp.snapshot


@dataclass
class Snapshot:
    """A Snapshot is a saved state of a Sandbox that can be used to create new Sandboxes."""

    client: SyncSandboxOpsClient
    snapshot: SnapshotModel

    @property
    def snapshot_id(self) -> str:
        """Unique ID of this snapshot."""
        return self.snapshot.id

    @property
    def source_sandbox_id(self) -> str:
        """The ID of the sandbox from which this snapshot was created."""
        return self.snapshot.source_sandbox_id

    @property
    def status(self) -> Literal["created", "deleted", "failed"]:
        """The status of the snapshot."""
        return self.snapshot.status

    @property
    def size_bytes(self) -> int:
        """Size of the snapshot in bytes."""
        return self.snapshot.size_bytes

    @property
    def created_at(self) -> int:
        """Timestamp when the snapshot was created."""
        return self.snapshot.created_at

    @property
    def expires_at(self) -> int | None:
        """Timestamp when the snapshot expires, or None for no expiration."""
        return self.snapshot.expires_at

    @staticmethod
    def get(
        *,
        snapshot_id: str,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> Snapshot:
        """Retrieve an existing snapshot by ID."""
        creds: Credentials = get_credentials(token=token, project_id=project_id, team_id=team_id)
        client = SyncSandboxOpsClient(team_id=creds.team_id, token=creds.token)
        resp = iter_coroutine(client.get_snapshot(snapshot_id=snapshot_id))
        return Snapshot(client=client, snapshot=resp.snapshot)

    @staticmethod
    def list(
        *,
        limit: int | None = None,
        _internal_page_size: int | None = None,
        since: datetime | int | None = None,
        until: datetime | int | None = None,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> Iterator[SnapshotModel]:
        """List snapshots as an iterable of snapshot models.

        `_internal_page_size` is a private override for the backend request
        size used during internal pagination. It exists for internal debugging
        and examples and is not part of the supported public contract.
        """
        creds: Credentials = get_credentials(token=token, project_id=project_id, team_id=team_id)
        params = SnapshotListParams(
            project_id=creds.project_id,
            limit=limit,
            internal_page_size=_internal_page_size,
            since=since,
            until=until,
        )

        def iter_snapshots() -> Iterator[SnapshotModel]:
            current_params = params
            with SyncSandboxOpsClient(team_id=creds.team_id, token=creds.token) as client:
                while True:
                    response = iter_coroutine(
                        client.list_snapshots(
                            project_id=current_params.project_id,
                            limit=current_params.request_limit,
                            since=current_params.since,
                            until=current_params.until,
                        )
                    )
                    snapshots = response.snapshots[: current_params.remaining]
                    yield from snapshots
                    if response.pagination.next is None:
                        return
                    if (
                        current_params.remaining is not None
                        and len(snapshots) >= current_params.remaining
                    ):
                        return
                    current_params = current_params.with_until(
                        response.pagination.next,
                        yielded_count=len(snapshots),
                    )

        return iter_snapshots()

    def delete(self) -> None:
        """Delete this snapshot."""
        resp = iter_coroutine(self.client.delete_snapshot(snapshot_id=self.snapshot.id))
        self.snapshot = resp.snapshot
