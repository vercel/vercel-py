"""Live semantic parity scenarios for `vercel.unstable.sandbox`."""

from uuid import uuid4

import pytest

from ._unstable_scenarios import (
    AsyncDriver,
    NetworkPolicyObservation,
    PersistentObservation,
    ProcessFilesystemObservation,
    StreamingTransferObservation,
    SyncDriver,
    WorkspaceObservation,
    network_policy_flow,
    persistent_snapshot_flow,
    process_filesystem_flow,
    streaming_transfer_flow,
    workspace_command_flow,
)
from .conftest import requires_sandbox_credentials


def _name(scenario: str, mode: str) -> str:
    return f"vercel-py-unstable-{scenario}-{mode}-{uuid4().hex[:10]}"


def _assert_workspace(result: WorkspaceObservation) -> None:
    assert result == WorkspaceObservation(
        stdout="stdout:scenario input\n",
        stderr="stderr:scenario input\n",
        output="SCENARIO INPUT\n",
        exit_code=0,
        context_cleaned_up=True,
    )


def _assert_persistent(result: PersistentObservation) -> None:
    assert result == PersistentObservation(
        discovered=True,
        tags_preserved=True,
        routes_preserved=True,
        project_id_preserved=True,
        current_session_preserved=True,
        routes_cleared=True,
        snapshot_fetched=True,
        snapshot_listed=True,
        restored_content="restored state\n",
        session_output="session follow-up\n",
        session_exit_code=0,
        session_cleaned_up=True,
        resources_cleaned_up=True,
    )


def _assert_process_filesystem(result: ProcessFilesystemObservation) -> None:
    assert result.stdout == "stdout line\n"
    assert result.stderr == "stderr line\n"
    assert result.returncode == 3
    assert result.terminated_returncode != 0
    assert result.timed_out_returncode != 0
    assert result.missing_executable_failed
    assert result.text == "hello\n"
    assert result.binary == b"\x00\xff"
    assert result.missing_read_failed
    assert result.invalid_write_failed


def _assert_network_policy(result: NetworkPolicyObservation) -> None:
    assert result == NetworkPolicyObservation(
        allow_all_created=True,
        custom_returned=True,
        header_names_redacted=True,
        deny_all_returned=True,
        resources_cleaned_up=True,
    )


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_workspace_command_flow_has_sync_async_semantic_parity() -> None:
    async_result = await workspace_command_flow(AsyncDriver(), _name("workspace", "async"))
    sync_result = await workspace_command_flow(SyncDriver(), _name("workspace", "sync"))

    _assert_workspace(async_result)
    _assert_workspace(sync_result)
    assert async_result == sync_result


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_process_filesystem_flow_has_sync_async_semantic_parity() -> None:
    async_result = await process_filesystem_flow(AsyncDriver(), _name("process-fs", "async"))
    sync_result = await process_filesystem_flow(SyncDriver(), _name("process-fs", "sync"))

    _assert_process_filesystem(async_result)
    _assert_process_filesystem(sync_result)


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_streaming_transfer_flow() -> None:
    expected = StreamingTransferObservation(
        digest_matches=True,
        empty_matches=True,
        explicit_mode="0o600",
        missing_download_failed=True,
    )
    async_result = await streaming_transfer_flow(AsyncDriver(), _name("transfer", "async"))
    sync_result = await streaming_transfer_flow(SyncDriver(), _name("transfer", "sync"))
    assert async_result == expected
    assert sync_result == expected


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_network_policy_flow_has_sync_async_semantic_parity() -> None:
    async_result = await network_policy_flow(AsyncDriver(), _name("network-policy", "async"))
    sync_result = await network_policy_flow(SyncDriver(), _name("network-policy", "sync"))

    _assert_network_policy(async_result)
    _assert_network_policy(sync_result)
    assert async_result == sync_result


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_persistent_snapshot_flow_has_sync_async_semantic_parity() -> None:
    async_result = await persistent_snapshot_flow(AsyncDriver(), _name("persist", "async"))
    sync_result = await persistent_snapshot_flow(SyncDriver(), _name("persist", "sync"))

    _assert_persistent(async_result)
    _assert_persistent(sync_result)
    assert async_result == sync_result
