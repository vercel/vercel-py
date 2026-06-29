import io
import json
import tarfile
from pathlib import PurePosixPath
from typing import cast

import httpx
import pytest
import respx

from vercel import unstable as vercel
from vercel._internal.unstable.sandbox.options import SandboxCredentials
from vercel.unstable import sandbox
from vercel.unstable.sandbox import (
    DirectoryEntry,
    SandboxApiError,
    SandboxFilesystemCommandError,
    SandboxFilesystemWriteError,
    SandboxPathNotFoundError,
    SandboxServiceOptions,
    sync as sandbox_sync,
)


def _sandbox_response(session_id: str = "sbx_1") -> dict[str, object]:
    return {
        "sandbox": {
            "name": "preview",
            "currentSessionId": session_id,
            "status": "running",
        },
        "session": {
            "id": session_id,
            "sourceSandboxName": "preview",
            "projectId": "prj_123",
            "status": "running",
            "cwd": "/vercel/sandbox",
        },
    }


def _command_response(
    command_id: str, session_id: str = "sbx_1", exit_code: int | None = None
) -> dict[str, object]:
    return {
        "command": {
            "id": command_id,
            "name": "sh",
            "args": [],
            "cwd": "/vercel/sandbox",
            "sessionId": session_id,
            "exitCode": exit_code,
            "startedAt": 1,
        }
    }


def _logs_response(stdout: str = "", stderr: str = "") -> httpx.Response:
    records = []
    if stdout:
        records.append({"stream": "stdout", "data": stdout})
    if stderr:
        records.append({"stream": "stderr", "data": stderr})
    return httpx.Response(200, text="".join(json.dumps(item) + "\n" for item in records))


def _session_options() -> list[SandboxServiceOptions]:
    async def credentials_factory() -> SandboxCredentials:
        return SandboxCredentials(token="token", team_id="team_123", project_id="prj_123")

    return [
        SandboxServiceOptions(
            base_url="https://sandbox.test",
            credentials_factory=credentials_factory,
        )
    ]


def _tar_entries(content: bytes) -> dict[str, tuple[bytes, int]]:
    with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as archive:
        entries: dict[str, tuple[bytes, int]] = {}
        for member in archive.getmembers():
            extracted = archive.extractfile(member)
            assert extracted is not None
            entries[member.name] = (extracted.read(), member.mode)
        return entries


@respx.mock
async def test_async_filesystem_native_operations_and_write_composition(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    mkdir = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/mkdir").mock(
        return_value=httpx.Response(204)
    )
    reads = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read").mock(
        side_effect=[httpx.Response(200, content=b"bytes"), httpx.Response(200, content=b"text")]
    )
    writes = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/write").mock(
        return_value=httpx.Response(204)
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        assert isinstance(box.fs, sandbox.SandboxFilesystem)
        assert box.current_session is not None
        assert isinstance(box.current_session.fs, sandbox.SandboxFilesystem)
        for method in ("mkdir", "read_file", "read_text", "write_files"):
            assert not hasattr(box, method)
            assert not hasattr(box.current_session, method)

        await box.fs.mkdir(PurePosixPath("parent"), recursive=False)
        assert await box.fs.read_bytes(PurePosixPath("data.bin")) == b"bytes"
        assert await box.fs.read_text("message.txt") == "text"
        await box.fs.write_bytes("data.bin", b"\x00\x01", mode=0o600)
        await box.fs.write_text("message.txt", "hello", mode=0o640)
        await box.fs.write_text(
            PurePosixPath("file.txt"), "relative", cwd=PurePosixPath("workspace")
        )

    assert json.loads(mkdir.calls[0].request.content) == {
        "path": "parent",
        "recursive": False,
    }
    assert reads.call_count == 2
    assert writes.call_count == 3
    assert _tar_entries(writes.calls[0].request.content) == {
        "vercel/sandbox/data.bin": (b"\x00\x01", 0o600)
    }
    assert _tar_entries(writes.calls[1].request.content) == {
        "vercel/sandbox/message.txt": (b"hello", 0o640)
    }
    assert _tar_entries(writes.calls[2].request.content) == {
        "vercel/sandbox/workspace/file.txt": (b"relative", 0o644)
    }


@respx.mock
async def test_filesystem_write_wraps_api_error(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/write").mock(
        return_value=httpx.Response(
            413, json={"error": {"code": "too_large", "message": "too large"}}
        )
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(SandboxFilesystemWriteError) as exc_info:
            async with box.fs.batch(cwd=PurePosixPath("workspace")) as batch:
                batch.write_text(PurePosixPath("a.txt"), "a")
                batch.write_bytes("b.bin", b"b")

    error = exc_info.value
    assert error.paths == ("a.txt", "b.bin")
    assert error.cwd == "/vercel/sandbox/workspace"
    assert error.cause.code == "too_large"


@respx.mock
async def test_filesystem_target_binding_tracks_sandbox_but_not_runtime_session(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response("sbx_1"))
    )
    respx.patch("https://sandbox.test/v2/sandboxes/preview").mock(
        return_value=httpx.Response(200, json=_sandbox_response("sbx_2"))
    )
    first = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/mkdir").mock(
        return_value=httpx.Response(204)
    )
    second = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_2/fs/mkdir").mock(
        return_value=httpx.Response(204)
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        retained_box_fs = box.fs
        assert box.current_session is not None
        retained_session_fs = box.current_session.fs
        await box.update(current_snapshot_id="snap_1")
        await retained_box_fs.mkdir("current")
        await retained_session_fs.mkdir("original")

    assert second.call_count == 1
    assert first.call_count == 1


@respx.mock
async def test_async_filesystem_batch_stages_one_request_and_skips_aborted_or_empty_batches(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    writes = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/write").mock(
        return_value=httpx.Response(204)
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        staged = box.fs.batch(cwd="workspace")
        with pytest.raises(RuntimeError):
            staged.write_text("outside.txt", "no")
        async with staged as batch:
            batch.write_text("message.txt", "cafe", encoding="ascii", mode=0o640)
            batch.write_bytes("data.bin", b"\x00", mode=0o600)
        with pytest.raises(RuntimeError):
            staged.write_bytes("after.bin", b"no")
        with pytest.raises(RuntimeError):
            async with staged:
                pass

        async with box.fs.batch():
            pass
        with pytest.raises(ValueError):
            async with box.fs.batch() as batch:
                batch.write_text("ignored.txt", "ignored")
                raise ValueError("abort")

    assert writes.call_count == 1
    assert _tar_entries(writes.calls[0].request.content) == {
        "vercel/sandbox/workspace/data.bin": (b"\x00", 0o600),
        "vercel/sandbox/workspace/message.txt": (b"cafe", 0o640),
    }


@respx.mock
async def test_command_backed_filesystem_operations_parse_output_and_pass_paths_as_args(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    requested: list[dict[str, object]] = []
    command_ids = iter(["exists", "is_file", "listdir", "remove", "rename"])

    def start(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert isinstance(payload, dict)
        requested.append(payload)
        return httpx.Response(200, json=_command_response(next(command_ids)))

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(side_effect=start)
    exit_codes = {"exists": 0, "is_file": 1, "listdir": 0, "remove": 0, "rename": 0}
    output = {"listdir": "z name\0file\0.dot\nname\0directory\0"}
    for command_id, exit_code in exit_codes.items():
        respx.get(f"https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/{command_id}").mock(
            return_value=httpx.Response(
                200, json=_command_response(command_id, exit_code=exit_code)
            )
        )
        respx.get(f"https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/{command_id}/logs").mock(
            return_value=_logs_response(output.get(command_id, ""))
        )

    unsafe_path = "-a; printf injected"
    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        assert await box.fs.exists(unsafe_path)
        assert not await box.fs.is_file("missing")
        assert await box.fs.listdir("directory") == [
            DirectoryEntry(path=".dot\nname", kind="directory"),
            DirectoryEntry(path="z name", kind="file"),
        ]
        await box.fs.remove(unsafe_path, recursive=True, missing_ok=True)
        await box.fs.rename(unsafe_path, "destination")

    request_args = [cast(list[str], item["args"]) for item in requested]
    scripts = [args[1] for args in request_args]
    assert all(unsafe_path not in script for script in scripts)
    assert request_args[0][3:] == [unsafe_path, "-e"]
    assert request_args[3][3:] == [unsafe_path, "true", "true"]
    assert request_args[4][3:] == [unsafe_path, "destination"]


@respx.mock
async def test_filesystem_failures_use_filesystem_error_contract(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    read = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read")
    read.mock(
        side_effect=[
            httpx.Response(404, json={"error": {"code": "not_found", "message": "missing"}}),
            httpx.Response(404, json={"error": {"code": "unknown", "message": "missing"}}),
        ]
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_command_response("listdir"))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/listdir").mock(
        return_value=httpx.Response(200, json=_command_response("listdir", exit_code=2))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/listdir/logs").mock(
        return_value=_logs_response("partial", "cannot list")
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(SandboxPathNotFoundError) as missing:
            await box.fs.read_bytes("missing")
        assert missing.value.path == "missing"
        assert missing.value.cause.code == "not_found"

        with pytest.raises(SandboxApiError):
            await box.fs.read_bytes("unclassified")

        with pytest.raises(SandboxFilesystemCommandError) as failed:
            await box.fs.listdir("directory")
        assert failed.value.operation == "listdir"
        assert failed.value.paths == ("directory",)
        assert failed.value.exit_code == 2
        assert failed.value.stdout == "partial"
        assert failed.value.stderr == "cannot list"


@respx.mock
def test_sync_filesystem_capability_uses_sync_boundary(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd").mock(
        return_value=httpx.Response(200, json=_command_response("exists"))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/exists").mock(
        return_value=httpx.Response(200, json=_command_response("exists", exit_code=0))
    )
    respx.get("https://sandbox.test/v2/sandboxes/sessions/sbx_1/cmd/exists/logs").mock(
        return_value=_logs_response()
    )

    with vercel.session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        assert isinstance(box.fs, sandbox_sync.SyncSandboxFilesystem)
        assert box.current_session is not None
        assert isinstance(box.current_session.fs, sandbox_sync.SyncSandboxFilesystem)
        assert box.fs.exists("present")
        for method in ("mkdir", "read_file", "read_text", "write_files"):
            assert not hasattr(box, method)
            assert not hasattr(box.current_session, method)


@respx.mock
def test_sync_filesystem_batch_stages_one_request(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    writes = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/write").mock(
        return_value=httpx.Response(204)
    )

    with vercel.session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        staged = box.fs.batch(cwd="/tmp")
        with staged as batch:
            batch.write_text("message.txt", "hello")
            batch.write_bytes("data.bin", b"\x01", mode=0o600)
        with pytest.raises(RuntimeError):
            with staged:
                pass

    assert writes.call_count == 1
    assert _tar_entries(writes.calls[0].request.content) == {
        "tmp/data.bin": (b"\x01", 0o600),
        "tmp/message.txt": (b"hello", 0o644),
    }
