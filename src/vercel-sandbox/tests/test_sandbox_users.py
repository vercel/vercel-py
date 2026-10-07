"""Public selection, immutability, and structural capability contracts."""

import re
import subprocess
import sys
from dataclasses import FrozenInstanceError
from inspect import isawaitable
from typing import TYPE_CHECKING

import pytest
from hypothesis import given, strategies as st
from sandbox_fixtures import sandbox_service_options

import vendor.respx as respx
from vercel import sandbox
from vercel._internal.core.session import get_active_sync_session
from vercel.api import session
from vercel.sandbox import SandboxExecution, SandboxFilesystemOperations
from vercel.sandbox._internal.service import get_sync_sandbox_service
from vercel.sandbox._internal.state import ProcessState, SandboxRuntimeSessionState, SandboxState
from vercel.sandbox.sync import SyncSandboxExecution, SyncSandboxFilesystemOperations


def _handles():  # type: ignore[no-untyped-def]
    service = get_sync_sandbox_service(get_active_sync_session())
    state = SandboxRuntimeSessionState(id="sbx_test")
    # Async handles can also be selected synchronously without opening a transport.
    return (
        sandbox.Sandbox(
            payload=SandboxState(name="test", current_session_id=state.id), service=service
        ),
        sandbox.SandboxRuntimeSession(payload=state, service=service),
        sandbox.sync.SyncSandbox(
            payload=SandboxState(name="test", current_session_id=state.id), service=service
        ),
        sandbox.sync.SyncSandboxRuntimeSession(payload=state, service=service),
    )


def test_public_imports_in_fresh_interpreter() -> None:
    result = subprocess.run(
        [sys.executable, "-c", "import vercel.sandbox; import vercel.sandbox.sync"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


@respx.mock
async def test_user_pause_rejection_preserves_process_without_api_call(
    mock_env_clear: None,
) -> None:
    with session(service_options=sandbox_service_options(sync=True)):
        for handle in _handles():
            user = handle.as_user("alice")
            process_type = (
                sandbox.sync.SyncProcess
                if isinstance(
                    handle, (sandbox.sync.SyncSandbox, sandbox.sync.SyncSandboxRuntimeSession)
                )
                else sandbox.Process
            )
            process = process_type(
                payload=ProcessState(
                    id="cmd_test",
                    session_id="sbx_test",
                    name="/bin/sleep",
                    args=("60",),
                    cwd="/home/alice",
                    returncode=None,
                    started_at=1,
                ),
                service=user._context.service,
            )
            for signal in ("STOP", 20, sandbox.ProcessSignal.SIGTTIN, "SIGTTOU"):
                with pytest.raises(NotImplementedError, match="cannot safely pause"):
                    result = process.send_signal(signal)
                    if isawaitable(result):
                        await result
                assert (process.name, process.args, process.cwd) == (
                    "/bin/sleep",
                    ["60"],
                    "/home/alice",
                )
                assert process.returncode is None
                assert process.status is sandbox.ProcessStatus.RUNNING
    assert not respx.calls


@given(st.text().filter(lambda name: re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", name) is None))
def test_invalid_names_fail_at_selection_without_io(username: str) -> None:
    with session(service_options=sandbox_service_options(sync=True)):
        for handle in _handles():
            with pytest.raises(ValueError, match="username"):
                handle.as_user(username)


@given(st.from_regex(r"[a-z_][a-z0-9_-]{0,31}", fullmatch=True))
def test_selection_is_lazy_and_handles_are_immutable(username: str) -> None:
    with session(service_options=sandbox_service_options(sync=True)):
        for handle in _handles():
            user = handle.as_user(username)
            assert user.username == username
            assert user.home_dir == ("/root" if username == "root" else None)
            # Frozen handles cannot replace their identity or filesystem binding.
            with pytest.raises((FrozenInstanceError, AttributeError, TypeError)):
                user.username = "another"
            assert user.username == username
            assert not hasattr(user, "stop")
            assert not hasattr(user, "get_process")
            protocol = (
                SyncSandboxExecution
                if isinstance(
                    handle, (sandbox.sync.SyncSandbox, sandbox.sync.SyncSandboxRuntimeSession)
                )
                else SandboxExecution
            )
            fs_protocol = (
                SyncSandboxFilesystemOperations
                if protocol is SyncSandboxExecution
                else SandboxFilesystemOperations
            )
            assert isinstance(handle, protocol)
            assert isinstance(user, protocol)
            assert isinstance(user.fs, fs_protocol)


if TYPE_CHECKING:
    # Static assignments verify signature compatibility, beyond runtime member checks.
    def _async_contract(
        box: sandbox.Sandbox,
        runtime: sandbox.SandboxRuntimeSession,
        user: sandbox.SandboxUser,
    ) -> tuple[SandboxExecution, ...]:
        return box, runtime, user

    def _sync_contract(
        box: sandbox.sync.SyncSandbox,
        runtime: sandbox.sync.SyncSandboxRuntimeSession,
        user: sandbox.sync.SyncSandboxUser,
    ) -> tuple[SyncSandboxExecution, ...]:
        return box, runtime, user
