"""Shared recovery policy for sandbox-level session operations."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
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

TRANSITION_POLL_INTERVAL = 0.5
TRANSITION_TIMEOUT = 300.0


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
    """Coordinate one lifecycle recovery before an operation is replayed.

    Returning ``False`` leaves the original operation failure in place. This
    lets runtime-specific coordinators add transition waiting and shared
    recovery without changing the one-attempt/one-replay policy.
    """

    def _capture_recovery_target(self) -> "SandboxRecoveryTarget":
        """Capture the session identity and optional bound handle for an attempt."""
        ...

    async def _recover(self, lifecycle: SandboxLifecycle, target: "SandboxRecoveryTarget") -> bool:
        """Recover the current sandbox session when this runtime supports it."""
        ...


@dataclass(frozen=True, slots=True)
class SandboxRecoveryTarget:
    """The exact session used by an operation's first attempt."""

    session_id: str
    session: object | None


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
    target = coordinator._capture_recovery_target()
    try:
        return await operation(target.session_id)
    except Exception as error:
        lifecycle = classify_sandbox_lifecycle_error(error)
        if lifecycle is None or not await coordinator._recover(lifecycle, target):
            raise
    replay_target = coordinator._capture_recovery_target()
    return await operation(replay_target.session_id)
