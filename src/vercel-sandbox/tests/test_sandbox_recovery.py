import httpx
import pytest

from vercel.sandbox._internal.errors import (
    SandboxApiError,
    SandboxFilesystemWriteError,
    SandboxStreamError,
)
from vercel.sandbox._internal.recovery import (
    SandboxLifecycle,
    classify_sandbox_lifecycle_error,
    execute_with_sandbox_recovery,
)


def _api_error(*, status_code: int = 409, code: str | None = None) -> SandboxApiError:
    data = None if code is None else {"error": {"code": code, "message": "failed"}}
    return SandboxApiError(httpx.Response(status_code, json=data), "failed", data=data)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (_api_error(code="sandbox_stopped"), SandboxLifecycle.STOPPED),
        (_api_error(code="sandbox_stopping"), SandboxLifecycle.STOPPING),
        (_api_error(code="sandbox_snapshotting"), SandboxLifecycle.SNAPSHOTTING),
        (
            SandboxFilesystemWriteError(
                paths=("message.txt",),
                cwd="/vercel/sandbox",
                cause=_api_error(code="sandbox_stopped"),
            ),
            SandboxLifecycle.STOPPED,
        ),
        (_api_error(code="other"), None),
        (_api_error(status_code=410), SandboxLifecycle.STOPPED),
        (SandboxStreamError("stopped", code="sandbox_stopped"), None),
    ],
)
def test_classify_sandbox_lifecycle_error(
    error: BaseException, expected: SandboxLifecycle | None
) -> None:
    assert classify_sandbox_lifecycle_error(error) is expected


class _Coordinator:
    def __init__(self, *, recover: bool = True) -> None:
        self.recover = recover
        self.lifecycle: SandboxLifecycle | None = None

    async def _recover(self, lifecycle: SandboxLifecycle) -> bool:
        self.lifecycle = lifecycle
        return self.recover


async def test_execute_with_sandbox_recovery_replays_once() -> None:
    attempts = 0
    coordinator = _Coordinator()

    async def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _api_error(code="sandbox_stopped")
        return "replayed"

    assert await execute_with_sandbox_recovery(operation, coordinator=coordinator) == "replayed"
    assert attempts == 2
    assert coordinator.lifecycle is SandboxLifecycle.STOPPED


async def test_execute_with_sandbox_recovery_propagates_a_replay_failure() -> None:
    attempts = 0
    replay_error = _api_error(code="sandbox_stopped")

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _api_error(code="sandbox_stopped")
        raise replay_error

    with pytest.raises(SandboxApiError) as exc_info:
        await execute_with_sandbox_recovery(operation, coordinator=_Coordinator())

    assert exc_info.value is replay_error
    assert attempts == 2


async def test_execute_with_sandbox_recovery_preserves_unhandled_failure() -> None:
    error = _api_error(code="sandbox_stopping")
    coordinator = _Coordinator(recover=False)

    async def operation() -> None:
        raise error

    with pytest.raises(SandboxApiError) as exc_info:
        await execute_with_sandbox_recovery(operation, coordinator=coordinator)

    assert exc_info.value is error
    assert coordinator.lifecycle is SandboxLifecycle.STOPPING
