"""Public group selection, provisioning errors, and membership changes."""

import json
import re
from inspect import isawaitable
from typing import Any

import httpx2 as httpx
import pytest
from hypothesis import given, strategies as st
from sandbox_fixtures import sandbox_service_options

import vendor.respx as respx
from vercel import sandbox
from vercel._internal.core.session import get_active_session, get_active_sync_session
from vercel.api import session
from vercel.sandbox import SandboxError, SandboxGroupAlreadyExistsError
from vercel.sandbox._internal.group_core import _GROUP_ALREADY_EXISTS
from vercel.sandbox._internal.service import get_sandbox_service, get_sync_sandbox_service
from vercel.sandbox._internal.state import SandboxRuntimeSessionState, SandboxState

_COMMANDS = "https://sandbox.test/v2/sandboxes/sessions/sbx_test/cmd"


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


def _mock_commands(*, status: int, stdout: str = "", stderr: str = "") -> list[dict[str, Any]]:
    """Answer the default-account lookup, and finish other commands as given."""
    commands: list[dict[str, Any]] = []

    def command(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        commands.append(body)
        is_lookup = body["command"] == "id"
        state = {
            "id": "cmd_test",
            "name": body["command"],
            "args": body.get("args", []),
            "sessionId": "sbx_test",
            "cwd": "/",
            "startedAt": 1,
        }
        records = [
            {"command": state},
            {"stream": "stdout", "data": "1000\n" if is_lookup else stdout},
            {"stream": "stderr", "data": "" if is_lookup else stderr},
            {"command": {**state, "exitCode": 0 if is_lookup else status}},
        ]
        return httpx.Response(200, text="\n".join(json.dumps(record) for record in records))

    respx.post(_COMMANDS).mock(side_effect=command)
    return commands


async def _exercise(sync: bool, operation: Any) -> None:
    """Run an operation against a runtime session handle of the selected API."""
    if sync:
        with session(service_options=sandbox_service_options(sync=True)):
            result = operation(_handles()[3])
            assert not isawaitable(result)
    else:
        async with session(service_options=sandbox_service_options(sync=False)):
            handle = sandbox.SandboxRuntimeSession(
                payload=SandboxRuntimeSessionState(id="sbx_test"),
                service=get_sandbox_service(get_active_session()),
            )
            await operation(handle)


@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize(
    ("status", "stdout", "stderr", "duplicate"),
    [
        (9, _GROUP_ALREADY_EXISTS + "\n", "", True),
        (1, "", "Shared path already exists: /shared/devs", False),
        (9, "", "chmod: permission denied", False),
    ],
    ids=["duplicate", "occupied-shared-path", "setup-exit-nine"],
)
@respx.mock
async def test_create_group_distinguishes_existing_groups_from_provisioning_errors(
    mock_env_clear: None, sync: bool, status: int, stdout: str, stderr: str, duplicate: bool
) -> None:
    assert sandbox.sync.SandboxGroupAlreadyExistsError is SandboxGroupAlreadyExistsError
    _mock_commands(status=status, stdout=stdout, stderr=stderr)

    with pytest.raises(SandboxError) as raised:
        await _exercise(sync, lambda handle: handle.create_group("devs"))
    if duplicate:
        assert isinstance(raised.value, SandboxGroupAlreadyExistsError)
        assert raised.value.groupname == "devs"
    else:
        assert type(raised.value) is SandboxError
        assert stderr in str(raised.value)


@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize(
    "change",
    [
        lambda handle: handle.as_group("devs").add_user("alice"),
        lambda handle: handle.as_group("devs").add_user(handle.as_user("alice")),
        lambda handle: handle.as_group("devs").remove_user("alice"),
        lambda handle: handle.as_user("alice").add_to_group("devs"),
        lambda handle: handle.as_user("alice").add_to_group(handle.as_group("devs")),
        lambda handle: handle.as_user("alice").remove_from_group("devs"),
    ],
    ids=["add-name", "add-handle", "remove", "join-name", "join-handle", "leave"],
)
@pytest.mark.parametrize("fails", [False, True], ids=["succeeds", "fails"])
@respx.mock
async def test_membership_changes_run_privileged_and_report_failures(
    mock_env_clear: None, sync: bool, change: Any, fails: bool
) -> None:
    commands = _mock_commands(status=6 if fails else 0, stderr="group 'devs' does not exist")

    if fails:
        with pytest.raises(SandboxError, match="'alice'.*'devs'.*does not exist") as raised:
            await _exercise(sync, change)
        assert type(raised.value) is SandboxError
    else:
        await _exercise(sync, change)
    # Selecting either handle performs no I/O; one root command changes membership.
    [command] = commands
    assert command["sudo"] is True
    assert {"alice", "devs"} <= set(command["args"])


@given(st.text().filter(lambda name: re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", name) is None))
@respx.mock
def test_invalid_group_names_fail_without_io(name: str) -> None:
    with session(service_options=sandbox_service_options(sync=True)):
        handles = _handles()
        for handle in handles:
            with pytest.raises(ValueError, match="groupname"):
                handle.as_group(name)
        for handle in handles[2:]:
            user, group = handle.as_user("alice"), handle.as_group("devs")
            for change in (handle.create_group, user.add_to_group, user.remove_from_group):
                with pytest.raises(ValueError, match="groupname"):
                    change(name)
            for change in (group.add_user, group.remove_user):
                with pytest.raises(ValueError, match="username"):
                    change(name)
    assert not respx.calls


@given(st.from_regex(r"[a-z_][a-z0-9_-]{0,31}", fullmatch=True))
def test_group_selection_is_lazy_and_handle_properties_are_read_only(groupname: str) -> None:
    with session(service_options=sandbox_service_options(sync=True)):
        for handle in _handles():
            group = handle.as_group(groupname)
            for name, value in (("groupname", "another"), ("shared_dir", "/another")):
                with pytest.raises(AttributeError):
                    setattr(group, name, value)
            assert group.groupname == groupname
            assert group.shared_dir == f"/shared/{groupname}"
            assert not hasattr(group, "run_command")
            assert not hasattr(group, "fs")
