"""Internal Sandbox lifecycle state predicates."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING

from vercel._internal.http import SleepFn
from vercel._internal.unstable.sandbox.errors import (
    SandboxError,
    SandboxOperationTimeoutError,
    SandboxTerminalStateError,
)
from vercel._internal.unstable.sandbox.models import Sandbox, SandboxStatus
from vercel._internal.unstable.sandbox.params import SandboxCreateParams

if TYPE_CHECKING:
    from vercel._internal.unstable.sandbox.api_client import SandboxApiClient


def is_ready_for_create(status: SandboxStatus | None) -> bool:
    """Return True if the sandbox has reached the RUNNING state."""
    return status is not None and status == SandboxStatus.RUNNING


def is_terminal_for_create(status: SandboxStatus | None) -> bool:
    """Return True if the sandbox has reached a terminal failure state.

    Terminal states cannot transition to RUNNING, so a create that reaches
    them has definitively failed.
    """
    if status is None:
        return False
    return status in {
        SandboxStatus.FAILED,
        SandboxStatus.ABORTED,
        SandboxStatus.STOPPED,
        SandboxStatus.STOPPING,
    }


async def create_sandbox_with_wait(
    api_client: SandboxApiClient,
    params: SandboxCreateParams,
    *,
    wait: bool,
    timeout: timedelta | None,
    sleep_fn: SleepFn,
    monotonic_fn: Callable[[], float],
) -> Sandbox:
    """Create a sandbox and optionally poll until the create flow is complete."""

    timeout_seconds = timeout.total_seconds() if timeout is not None else None
    if timeout_seconds is not None and timeout_seconds <= 0:
        _raise_timeout(timeout)
    deadline = monotonic_fn() + timeout_seconds if timeout_seconds is not None else None

    def check_timeout() -> None:
        if deadline is not None and monotonic_fn() >= deadline:
            _raise_timeout(timeout)

    sandbox = await api_client.create(params)
    check_timeout()
    if not wait:
        return sandbox
    if sandbox.current_session is None:
        raise SandboxError("sandbox create returned response with no session")

    status = sandbox.current_session.status
    if is_ready_for_create(status):
        return sandbox
    if is_terminal_for_create(status):
        status_label = status.value if status is not None else "unknown"
        raise SandboxTerminalStateError(f"sandbox create reached terminal state {status_label}")

    while True:
        await _sleep_before_poll(sleep_fn, deadline=deadline, monotonic_fn=monotonic_fn)
        check_timeout()
        polled = await api_client.get_sandbox(sandbox.name)
        sandbox.current_session = polled.current_session
        sandbox._raw = polled._raw
        poll_status = polled.current_session.status if polled.current_session else None
        check_timeout()
        if is_ready_for_create(poll_status):
            return sandbox
        if is_terminal_for_create(poll_status):
            status_label = poll_status.value if poll_status else "unknown"
            raise SandboxTerminalStateError(f"sandbox create reached terminal state {status_label}")


async def _sleep_before_poll(
    sleep_fn: SleepFn,
    *,
    deadline: float | None,
    monotonic_fn: Callable[[], float],
) -> None:
    delay = 1.0
    if deadline is not None:
        remaining = deadline - monotonic_fn()
        if remaining <= 0:
            return
        delay = min(delay, remaining)
    result = sleep_fn(delay)
    if inspect.isawaitable(result):
        await result


def _raise_timeout(timeout: timedelta | None) -> None:
    seconds = timeout.total_seconds() if timeout is not None else 0
    raise SandboxOperationTimeoutError(f"sandbox create exceeded timeout of {seconds}s")


__all__ = [
    "create_sandbox_with_wait",
    "is_ready_for_create",
    "is_terminal_for_create",
]
