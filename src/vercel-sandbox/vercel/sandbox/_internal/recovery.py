"""Shared recovery policy for sandbox-level session operations."""

from collections.abc import Awaitable, Callable
from enum import Enum
from typing import Protocol, TypeVar

from vercel.sandbox._internal.errors import (
    SandboxApiError,
    SandboxFilesystemWriteError,
    SandboxStreamError,
)
from vercel.sandbox._internal.log_stream import STREAM_CLOSED_ERROR_CODE


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
_LIFECYCLE_CODES = {lifecycle: code for code, lifecycle in _LIFECYCLE_ERROR_CODES.items()}


def classify_sandbox_lifecycle_error(error: BaseException) -> SandboxLifecycle | None:
    """Return the supported lifecycle condition carried by ``error``.

    Only direct Sandbox API failures and the direct cause wrapped by a native
    filesystem write failure participate in lifecycle recovery. In particular,
    stream failures and arbitrary exception chains are intentionally excluded,
    and so is a closed stream reported as an HTTP 410, which does not prove the
    sandbox stopped.
    """
    api_error: SandboxApiError | None
    if isinstance(error, SandboxApiError):
        api_error = error
    elif isinstance(error, SandboxFilesystemWriteError):
        cause = getattr(error, "cause", None)
        if not isinstance(cause, SandboxApiError):
            return None
        api_error = cause
    else:
        return None
    if api_error.code == STREAM_CLOSED_ERROR_CODE:
        return None
    if api_error.status_code == 410:
        return SandboxLifecycle.STOPPED
    return None if api_error.code is None else _LIFECYCLE_ERROR_CODES.get(api_error.code)


def _lifecycle_stream_error(error: BaseException) -> SandboxStreamError | None:
    """Return a lifecycle failure as a stream error that is never replayed.

    Once a process has started, replaying the operation would run it again, so
    lifecycle failures while following it must end the operation. They surface
    the same way as a lifecycle failure reported in-band by the process stream.
    """
    lifecycle = classify_sandbox_lifecycle_error(error)
    if lifecycle is None:
        return None
    return SandboxStreamError(str(error), code=_LIFECYCLE_CODES[lifecycle])


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
    can_replay: Callable[[], bool] | None = None,
) -> _ResultT:
    """Run an operation once, optionally recover, and replay it once.

    ``operation`` is called separately for the first attempt and the replay,
    so callers can resolve their current session ID immediately before each
    request. A replay failure is never eligible for another recovery cycle.
    ``can_replay`` is checked after a lifecycle failure; when it returns
    false the failure is raised instead, for operations that must not run
    twice once they have taken effect.
    """
    session_id = coordinator._capture_recovery_session_id()
    try:
        return await operation(session_id)
    except Exception as error:
        if classify_sandbox_lifecycle_error(error) is None:
            raise
        if can_replay is not None and not can_replay():
            raise
        await coordinator._await_shared_resume()
    return await operation(coordinator._capture_recovery_session_id())
