"""Drive handle state shared by the sync and async runtimes."""

from vercel.drive._internal.errors import DriveResponseError
from vercel.drive._internal.models import DriveMount, DriveState
from vercel.drive._internal.service import DriveService


class DriveHandleBase:
    __slots__ = ("_payload", "_service")

    def __init__(self, *, payload: DriveState, service: DriveService) -> None:
        self._payload = payload
        self._service = service

    @property
    def id(self) -> str:
        """The unique Drive ID."""
        return self._payload.id

    @property
    def parent_drive_id(self) -> str | None:
        """The source Drive ID, if this Drive is a fork."""
        return self._payload.parent_drive_id

    @property
    def root_drive_id(self) -> str | None:
        """The original Drive ID at the root of this fork."""
        return self._payload.root_drive_id

    @property
    def name(self) -> str:
        """The Drive name, unique within its project."""
        return self._payload.name

    @property
    def project_id(self) -> str:
        """The project that owns the Drive."""
        return self._payload.project_id

    @property
    def region(self) -> str:
        """The region where the Drive's data is stored."""
        return self._payload.region

    @property
    def max_size_bytes(self) -> int:
        """The maximum Drive size in bytes."""
        return self._payload.max_size_bytes

    @property
    def current_session_id(self) -> str | None:
        """The sandbox session the Drive is attached to, if any."""
        return self._payload.current_session_id

    @property
    def current_sandbox_name(self) -> str | None:
        """The sandbox the Drive is attached to, if any."""
        return self._payload.current_sandbox_name

    @property
    def created_at(self) -> int:
        """When the Drive was created, in milliseconds since the epoch."""
        return self._payload.created_at

    @property
    def updated_at(self) -> int:
        """When the Drive was last updated, in milliseconds since the epoch."""
        return self._payload.updated_at

    def snapshot(self) -> DriveMount:
        """Return a read-only snapshot mount for this Drive."""
        return DriveMount(self, mode="snapshot")

    def _apply_payload(self, payload: DriveState) -> None:
        if payload.name != self.name or payload.project_id != self.project_id:
            raise DriveResponseError(
                "Drive mutation response returned a different name or project",
                data=payload,
            )
        self._payload = payload


__all__ = ["DriveHandleBase"]
