import httpx2 as httpx
import pytest

from vercel.sandbox._internal.errors import (
    SandboxApiError,
    SandboxFilesystemWriteError,
    SandboxStreamError,
)
from vercel.sandbox._internal.recovery import (
    SandboxLifecycle,
    SandboxRecoveryTarget,
    classify_sandbox_lifecycle_error,
    execute_with_sandbox_recovery,
)


def _api_error(*, status_code: int = 409, code: str | None = None) -> SandboxApiError:
    data = None if code is None else {"error": {"code": code, "message": "failed"}}
    return SandboxApiError(httpx.Response(status_code, json=data), "failed", data=data)


_MISSING_CAUSE = object()


def _malformed_write_error(cause: object = _MISSING_CAUSE) -> SandboxFilesystemWriteError:
    error = SandboxFilesystemWriteError.__new__(SandboxFilesystemWriteError)
    BaseException.__init__(error, "malformed write error")
    if cause is not _MISSING_CAUSE:
        error.cause = cause  # type: ignore[assignment]
    return error


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (_api_error(status_code=410, code="sandbox_stopped"), SandboxLifecycle.STOPPED),
        (_api_error(status_code=410, code="sandbox_failed"), SandboxLifecycle.STOPPED),
        (_api_error(status_code=410), SandboxLifecycle.STOPPED),
        (_api_error(code="sandbox_stopping"), SandboxLifecycle.STOPPING),
        (_api_error(code="sandbox_snapshotting"), SandboxLifecycle.SNAPSHOTTING),
        (
            SandboxFilesystemWriteError(
                paths=("message.txt",),
                cwd="/vercel/sandbox",
                cause=_api_error(status_code=410, code="sandbox_not_found"),
            ),
            SandboxLifecycle.STOPPED,
        ),
        (_malformed_write_error(), None),
        (_malformed_write_error(RuntimeError("non-api cause")), None),
        (_api_error(code="other"), None),
        (SandboxStreamError("stopped", code="sandbox_stopped"), None),
    ],
)
def test_classify_sandbox_lifecycle_error(
    error: BaseException, expected: SandboxLifecycle | None
) -> None:
    assert classify_sandbox_lifecycle_error(error) is expected


class _Coordinator:
    def __init__(
        self,
        *,
        recover: bool = True,
        recovery_error: BaseException | None = None,
    ) -> None:
        self.session_id = "session-1"
        self.recover_result = recover
        self.recovery_error = recovery_error
        self.recoveries: list[tuple[SandboxLifecycle, SandboxRecoveryTarget]] = []

    def _capture_recovery_target(self) -> SandboxRecoveryTarget:
        return SandboxRecoveryTarget(session_id=self.session_id, session=None)

    async def _recover(self, lifecycle: SandboxLifecycle, target: SandboxRecoveryTarget) -> bool:
        self.recoveries.append((lifecycle, target))
        if self.recovery_error is not None:
            raise self.recovery_error
        if self.recover_result:
            self.session_id = "session-2"
        return self.recover_result


@pytest.mark.asyncio
async def test_execute_recovers_once_and_replays_on_the_new_target() -> None:
    coordinator = _Coordinator()
    targets: list[str] = []

    async def operation(session_id: str) -> str:
        targets.append(session_id)
        if len(targets) == 1:
            raise _api_error(status_code=410)
        return "result"

    assert await execute_with_sandbox_recovery(operation, coordinator=coordinator) == "result"
    assert targets == ["session-1", "session-2"]
    assert coordinator.recoveries == [
        (
            SandboxLifecycle.STOPPED,
            SandboxRecoveryTarget(session_id="session-1", session=None),
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "recover"),
    [
        (_api_error(status_code=409, code="other"), True),
        (_api_error(status_code=410), False),
    ],
    ids=["unrelated-error", "recovery-declined"],
)
async def test_execute_preserves_the_original_error_without_replay(
    error: SandboxApiError, recover: bool
) -> None:
    coordinator = _Coordinator(recover=recover)
    attempts = 0

    async def operation(_session_id: str) -> None:
        nonlocal attempts
        attempts += 1
        raise error

    with pytest.raises(SandboxApiError) as caught:
        await execute_with_sandbox_recovery(operation, coordinator=coordinator)

    assert caught.value is error
    assert attempts == 1
    assert len(coordinator.recoveries) == (0 if recover else 1)


@pytest.mark.asyncio
async def test_execute_propagates_recovery_failure() -> None:
    recovery_error = RuntimeError("resume failed")
    coordinator = _Coordinator(recovery_error=recovery_error)

    async def operation(_session_id: str) -> None:
        raise _api_error(status_code=410)

    with pytest.raises(RuntimeError) as caught:
        await execute_with_sandbox_recovery(operation, coordinator=coordinator)

    assert caught.value is recovery_error


@pytest.mark.asyncio
async def test_execute_does_not_recover_a_failed_replay() -> None:
    coordinator = _Coordinator()
    attempts = 0
    replay_error = _api_error(status_code=410, code="sandbox_stopped")

    async def operation(_session_id: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _api_error(status_code=410)
        raise replay_error

    with pytest.raises(SandboxApiError) as caught:
        await execute_with_sandbox_recovery(operation, coordinator=coordinator)

    assert caught.value is replay_error
    assert attempts == 2
    assert len(coordinator.recoveries) == 1
