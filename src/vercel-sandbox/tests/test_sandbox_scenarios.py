"""The live scenarios, against the fake Sandbox API.

These are the same flows and expectations as `tests/live`, through both the
async and sync surfaces, so they check the SDK end to end on every run and
keep the fake aligned with the API wherever a scenario reaches.
"""

import pytest
from fake_sandbox import FakeSandboxApi
from sandbox_scenarios import (
    _ScenarioDriver,
    assert_interactive,
    assert_network_policy,
    assert_persistent,
    assert_process_filesystem,
    assert_streaming_transfer,
    assert_workspace,
    interactive_session_flow,
    network_policy_flow,
    persistent_snapshot_flow,
    process_filesystem_flow,
    streaming_transfer_flow,
    workspace_command_flow,
)

pytestmark = pytest.mark.asyncio


async def test_workspace_command_flow(driver: _ScenarioDriver, fake_api: FakeSandboxApi) -> None:
    assert_workspace(await workspace_command_flow(driver, "workspace"))
    assert not fake_api.sandboxes


async def test_process_filesystem_flow(driver: _ScenarioDriver) -> None:
    assert_process_filesystem(await process_filesystem_flow(driver, "process-fs"))


async def test_streaming_transfer_flow(driver: _ScenarioDriver) -> None:
    assert_streaming_transfer(await streaming_transfer_flow(driver, "transfer"))


async def test_network_policy_flow(driver: _ScenarioDriver, fake_api: FakeSandboxApi) -> None:
    assert_network_policy(await network_policy_flow(driver, "network-policy"))
    assert not fake_api.sandboxes


async def test_persistent_snapshot_flow(driver: _ScenarioDriver, fake_api: FakeSandboxApi) -> None:
    assert_persistent(await persistent_snapshot_flow(driver, "persist"))
    assert not fake_api.sandboxes


async def test_interactive_session_flow(driver: _ScenarioDriver, fake_api: FakeSandboxApi) -> None:
    assert_interactive(await interactive_session_flow(driver, "interactive"))
    (terminal,) = fake_api.terminals
    assert terminal.sizes == [(100, 30), (120, 40)]
