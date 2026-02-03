from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from vercel.oidc import Credentials, get_credentials

from .api_client import APIClient, AsyncAPIClient
from .models import Snapshot as SnapshotModel


@dataclass
class AsyncSnapshot:
    """A Snapshot is a saved state of a Sandbox that can be used to create new Sandboxes."""

    client: AsyncAPIClient
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
    def expires_at(self) -> int:
        """Timestamp when the snapshot expires."""
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
        client = AsyncAPIClient(team_id=creds.team_id, token=creds.token)
        resp = await client.get_snapshot(snapshot_id=snapshot_id)
        return AsyncSnapshot(client=client, snapshot=resp.snapshot)

    async def delete(self) -> None:
        """Delete this snapshot."""
        resp = await self.client.delete_snapshot(snapshot_id=self.snapshot.id)
        self.snapshot = resp.snapshot


@dataclass
class Snapshot:
    """A Snapshot is a saved state of a Sandbox that can be used to create new Sandboxes."""

    client: APIClient
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
    def expires_at(self) -> int:
        """Timestamp when the snapshot expires."""
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
        client = APIClient(team_id=creds.team_id, token=creds.token)
        resp = client.get_snapshot(snapshot_id=snapshot_id)
        return Snapshot(client=client, snapshot=resp.snapshot)

    def delete(self) -> None:
        """Delete this snapshot."""
        resp = self.client.delete_snapshot(snapshot_id=self.snapshot.id)
        self.snapshot = resp.snapshot

