"""Shared recovery policy for sandbox-level session operations."""

from collections.abc import Awaitable, Callable
from enum import Enum
from typing import Protocol, TypeVar

from vercel.sandbox._internal.errors import (
    SandboxApiError,
    SandboxFilesystemWriteError,
)


class SandboxLifecycle(Enum):
    """A lifecycle condition reported by a sandbox operation."""

    STOPPED = "stopped"
    STOPPING = "stopping"
    SNAPSHOTTING = "snapshotting"


_LIFECYCLE_ERROR_CODES = {
    "sandbox_stopped": SandboxLifecycle.STOPPED,
    "sandbox_stopping": SandboxLifecycle.STOPPING,
    "sandbox_snapshotting": SandboxLifecycle.SNAPSHOTTING,
}


def classify_sandbox_lifecycle_error(error: BaseException) -> SandboxLifecycle | None:
    """Return the supported lifecycle condition carried by ``error``.

    Only direct Sandbox API failures and the direct cause wrapped by a native
    filesystem write failure participate in lifecycle recovery. In particular,
    stream failures and arbitrary exception chains are intentionally excluded.
    """
    api_error: SandboxApiError | None
    if isinstance(error, SandboxApiError):
        api_error = error
    elif isinstance(error, SandboxFilesystemWriteError) and isinstance(
        getattr(error, "cause", None), SandboxApiError
    ):
        api_error = error.cause
    else:
        return None
    if api_error.status_code == 410:
        return SandboxLifecycle.STOPPED
    return None if api_error.code is None else _LIFECYCLE_ERROR_CODES.get(api_error.code)


class SandboxRecoveryCoordinator(Protocol):
    """Provide the current session ID and a shared API resume attempt."""

    def _capture_recovery_session_id(self) -> str:
        """Capture the session identity for an operation attempt."""
        ...

    async def _await_shared_resume(self) -> None:
        """Resume through the API, sharing the attempt with concurrent callers."""
        ...


_ResultT = TypeVar("_ResultT")


async def execute_with_sandbox_recovery(
    operation: Callable[[str], Awaitable[_ResultT]],
    *,
    coordinator: SandboxRecoveryCoordinator,
) -> _ResultT:
    """Run an operation once, optionally recover, and replay it once.

    ``operation`` is called separately for the first attempt and the replay,
    so callers can resolve their current session ID immediately before each
    request. A replay failure is never eligible for another recovery cycle.
    """
    session_id = coordinator._capture_recovery_session_id()
    try:
        return await operation(session_id)
    except Exception as error:
        if classify_sandbox_lifecycle_error(error) is None:
            raise
        await coordinator._await_shared_resume()
    return await operation(coordinator._capture_recovery_session_id())
