"""Live semantic parity scenarios for `vercel.unstable.sandbox`."""

from uuid import uuid4

import pytest

from ._unstable_scenarios import (
    AsyncDriver,
    PersistentObservation,
    SyncDriver,
    WorkspaceObservation,
    persistent_snapshot_flow,
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
async def test_persistent_snapshot_flow_has_sync_async_semantic_parity() -> None:
    async_result = await persistent_snapshot_flow(AsyncDriver(), _name("persist", "async"))
    sync_result = await persistent_snapshot_flow(SyncDriver(), _name("persist", "sync"))

    _assert_persistent(async_result)
    _assert_persistent(sync_result)
    assert async_result == sync_result
