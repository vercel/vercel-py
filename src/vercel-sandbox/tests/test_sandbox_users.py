"""Public selection, read-only properties, and structural capability contracts."""

import base64
import io
import json
import re
import subprocess
import sys
import tarfile
from datetime import timedelta
from inspect import isawaitable, iscoroutinefunction, signature
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock

import httpx2 as httpx
import pytest
from hypothesis import given, strategies as st
from sandbox_fixtures import sandbox_service_options

import vendor.respx as respx
from vercel import sandbox
from vercel._internal.core.session import get_active_sync_session
from vercel.api import session
from vercel.sandbox import (
    SandboxError,
    SandboxExecution,
    SandboxFilesystemOperations,
    SandboxFilesystemWriteError,
    SandboxUserAlreadyExistsError,
)
from vercel.sandbox._internal.service import get_sync_sandbox_service
from vercel.sandbox._internal.state import ProcessState, SandboxRuntimeSessionState, SandboxState
from vercel.sandbox._internal.user_core import _USER_ALREADY_EXISTS
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


@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize(
    ("status", "stdout", "stderr", "duplicate"),
    [
        (9, _USER_ALREADY_EXISTS + "\n", "", True),
        (1, "", "Home path already exists: /home/alice", False),
        (9, "", "useradd: group alice already exists", False),
        (9, "", "chmod: permission denied", False),
    ],
    ids=["duplicate", "occupied-home", "conflicting-group", "setup-exit-nine"],
)
@respx.mock
async def test_create_user_distinguishes_existing_accounts_from_provisioning_errors(
    mock_env_clear: None, sync: bool, status: int, stdout: str, stderr: str, duplicate: bool
) -> None:
    assert sandbox.sync.SandboxUserAlreadyExistsError is SandboxUserAlreadyExistsError

    def command(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
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

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_test/cmd").mock(side_effect=command)

    async def exercise() -> None:
        if sync:
            handle = _handles()[3]
        else:
            from vercel._internal.core.session import get_active_session
            from vercel.sandbox._internal.service import get_sandbox_service

            handle = sandbox.SandboxRuntimeSession(
                payload=SandboxRuntimeSessionState(id="sbx_test"),
                service=get_sandbox_service(get_active_session()),
            )
        with pytest.raises(SandboxError) as raised:
            result = handle.create_user("alice")
            if isawaitable(result):
                await result
        if duplicate:
            assert isinstance(raised.value, SandboxUserAlreadyExistsError)
            assert raised.value.username == "alice"
        else:
            assert type(raised.value) is SandboxError
            assert stderr in str(raised.value)

    if sync:
        with session(service_options=sandbox_service_options(sync=True)):
            await exercise()
    else:
        async with session(service_options=sandbox_service_options(sync=False)):
            await exercise()


@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize("failure", [None, "upload", "chown", "chmod"])
@respx.mock
async def test_user_upload_keeps_payload_out_of_commands(
    mock_env_clear: None, sync: bool, failure: str | None
) -> None:
    commands: list[dict[str, Any]] = []
    events: list[str] = []
    data = b"private payload\x00\xff" * 4096

    def command(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        commands.append(body)
        name = body["command"]
        events.append(name)
        output = {
            "getent": "alice:x:1001:1002::/home/alice:/bin/bash\n",
            "id": "1000\n",
        }.get(name, "")
        state = {
            "id": f"cmd_{len(commands)}",
            "name": name,
            "sessionId": "sbx_test",
            "args": body.get("args", []),
            "cwd": "/",
            "startedAt": 1,
        }
        records = [
            {"command": state},
            {"stream": "stdout", "data": output},
            {"stream": "stderr", "data": "metadata failed" if name == failure else ""},
            {"command": {**state, "exitCode": 1 if name == failure else 0}},
        ]
        return httpx.Response(200, text="\n".join(json.dumps(record) for record in records))

    def upload(request: httpx.Request) -> httpx.Response:
        events.append("upload")
        with tarfile.open(fileobj=io.BytesIO(request.content), mode="r:gz") as archive:
            member = archive.getmember("home/alice/nested/secret.bin")
            source = archive.extractfile(member)
            assert source is not None and source.read() == data
            assert member.mode == 0o600
        if failure == "upload":
            return httpx.Response(
                403, json={"error": {"code": "file_error", "message": "upload failed"}}
            )
        return httpx.Response(204)

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_test/cmd").mock(side_effect=command)
    route = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_test/fs/write").mock(
        side_effect=upload
    )

    async def write() -> None:
        if sync:
            handle = _handles()[3]
        else:
            from vercel._internal.core.session import get_active_session
            from vercel.sandbox._internal.service import get_sandbox_service

            handle = sandbox.SandboxRuntimeSession(
                payload=SandboxRuntimeSessionState(id="sbx_test"),
                service=get_sandbox_service(get_active_session()),
            )
        result = handle.as_user("alice").fs.write_bytes("nested/secret.bin", data, mode=0o600)
        if isawaitable(result):
            await result

    async def exercise() -> None:
        if failure:
            with pytest.raises(SandboxFilesystemWriteError) as raised:
                await write()
            assert raised.value.paths == ("nested/secret.bin",)
            assert raised.value.cwd == "/home/alice"
        else:
            await write()

    if sync:
        with session(service_options=sandbox_service_options(sync=True)):
            await exercise()
    else:
        async with session(service_options=sandbox_service_options(sync=False)):
            await exercise()

    assert route.call_count == 1
    assert all(body["command"] in {"getent", "id", "chown", "chmod"} for body in commands)
    assert base64.b64encode(data).decode() not in json.dumps(commands)
    if failure == "upload":
        assert events == ["getent", "upload"]
    else:
        assert events.index("upload") < events.index("chown")


@respx.mock
async def test_user_pause_rejection_preserves_process_without_api_call(
    mock_env_clear: None,
) -> None:
    with session(service_options=sandbox_service_options(sync=True)):
        for handle in _handles():
            user = handle.as_user("alice")
            process_type = (
                sandbox.sync.SyncCommand
                if isinstance(
                    handle, (sandbox.sync.SyncSandbox, sandbox.sync.SyncSandboxRuntimeSession)
                )
                else sandbox.Command
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
            for signal in ("STOP", 20, sandbox.CommandSignal.SIGTTIN, "SIGTTOU"):
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
                assert process.status is sandbox.CommandStatus.RUNNING
    assert not respx.calls


@given(st.text().filter(lambda name: re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", name) is None))
def test_invalid_names_fail_at_selection_without_io(username: str) -> None:
    with session(service_options=sandbox_service_options(sync=True)):
        for handle in _handles():
            with pytest.raises(ValueError, match="username"):
                handle.as_user(username)


@given(st.from_regex(r"[a-z_][a-z0-9_-]{0,31}", fullmatch=True))
def test_selection_is_lazy_and_handle_properties_are_read_only(username: str) -> None:
    with session(service_options=sandbox_service_options(sync=True)):
        for handle in _handles():
            user = handle.as_user(username)
            assert user.username == username
            assert user.home_dir == ("/root" if username == "root" else None)
            filesystem = user.fs
            for name, value in (
                ("username", "another"),
                ("home_dir", "/another"),
                ("fs", object()),
            ):
                with pytest.raises(AttributeError):
                    setattr(user, name, value)
            assert user.username == username
            assert user.home_dir == ("/root" if username == "root" else None)
            assert user.fs is filesystem
            assert not hasattr(user, "stop")
            assert not hasattr(user, "get_command")
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


@pytest.mark.parametrize("legacy", ["run_process", "create_process"])
@pytest.mark.parametrize("fails", [False, True], ids=["returns", "raises"])
async def test_deprecated_process_methods_preserve_command_contract(
    monkeypatch: pytest.MonkeyPatch, legacy: str, fails: bool
) -> None:
    promoted = legacy.replace("process", "command")
    with session(service_options=sandbox_service_options(sync=True)):
        handles = _handles()
        for handle in (*handles, *(handle.as_user("alice") for handle in handles)):
            old_method = getattr(handle, legacy)
            new_method = getattr(handle, promoted)
            assert signature(old_method) == signature(new_method)
            kwargs: dict[str, Any] = {
                "cwd": "workspace",
                "env": {"CUSTOM": "value"},
                "sudo": True,
                "kill_after": timedelta(seconds=3),
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,
            }
            if legacy == "run_process":
                kwargs.update(check=True, capture_output=False, input=b"payload")
                if "on_start" in signature(old_method).parameters:
                    kwargs["on_start"] = lambda process: None
            else:
                kwargs["stdin"] = subprocess.PIPE

            result = object()
            error = RuntimeError("command failed")
            delegate = (AsyncMock if iscoroutinefunction(new_method) else Mock)(
                return_value=result, side_effect=error if fails else None
            )
            with monkeypatch.context() as patch:
                patch.setattr(type(handle), promoted, delegate)
                with pytest.warns(DeprecationWarning, match=rf"{legacy}.*{promoted}") as warnings:
                    if fails:
                        with pytest.raises(RuntimeError) as raised:
                            value = old_method("python", ("-V",), **kwargs)
                            if isawaitable(value):
                                await value
                        assert raised.value is error
                    else:
                        value = old_method("python", ("-V",), **kwargs)
                        if isawaitable(value):
                            value = await value
                        assert value is result
                assert len(warnings) == 1
                assert warnings[0].filename == __file__
            delegate.assert_called_once_with("python", ("-V",), **kwargs)
            if isinstance(delegate, AsyncMock):
                delegate.assert_awaited_once()


@pytest.mark.parametrize("legacy", ["get_process", "query_processes"])
@pytest.mark.parametrize("fails", [False, True], ids=["returns", "raises"])
async def test_deprecated_process_lookups_delegate_to_commands(
    monkeypatch: pytest.MonkeyPatch, legacy: str, fails: bool
) -> None:
    promoted = {"get_process": "get_command", "query_processes": "query_commands"}[legacy]
    with session(service_options=sandbox_service_options(sync=True)):
        for handle in _handles():
            old_method = getattr(handle, legacy)
            new_method = getattr(handle, promoted)
            result = object()
            error = RuntimeError("lookup failed")
            delegate = (AsyncMock if iscoroutinefunction(new_method) else Mock)(
                return_value=result, side_effect=error if fails else None
            )
            # Keep the old keyword spelling available to callers during migration.
            kwargs = {"process_id": "cmd_test", "wait": True} if legacy == "get_process" else {}
            with monkeypatch.context() as patch:
                patch.setattr(type(handle), promoted, delegate)
                with pytest.warns(DeprecationWarning, match=rf"{legacy}.*{promoted}") as warnings:
                    if fails:
                        with pytest.raises(RuntimeError) as raised:
                            value = old_method(**kwargs)
                            if isawaitable(value):
                                await value
                        assert raised.value is error
                    else:
                        value = old_method(**kwargs)
                        if isawaitable(value):
                            value = await value
                        assert value is result
                assert len(warnings) == 1
                assert warnings[0].filename == __file__
            if legacy == "get_process":
                delegate.assert_called_once_with("cmd_test", wait=True)
            else:
                delegate.assert_called_once_with()
            if isinstance(delegate, AsyncMock):
                delegate.assert_awaited_once()


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
