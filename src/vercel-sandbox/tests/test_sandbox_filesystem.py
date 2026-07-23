import io
import json
import tarfile
from collections.abc import AsyncIterator, Iterator
from pathlib import PurePosixPath
from typing import cast

import httpx
import pytest
import respx
from hypothesis import HealthCheck, given, settings, strategies as st

import vercel
from vercel import sandbox
from vercel.sandbox import (
    DirectoryEntry,
    SandboxApiError,
    SandboxFilesystemCommandError,
    SandboxFilesystemWriteError,
    SandboxPathNotFoundError,
    SandboxServiceOptions,
    SandboxUploadSizeMismatchError,
    sync as sandbox_sync,
)
from vercel.sandbox._internal.options import SandboxCredentials


async def _read_as_chunks(source: bytes, chunk_size: int) -> AsyncIterator[bytes]:
    offset = 0
    while offset < len(source):
        chunk = source[offset : offset + chunk_size]
        yield chunk
        offset += chunk_size


class _SyncByteReader:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    def read(self, n: int = -1) -> bytes:
        if n < 0:
            chunk = self._data[self._offset :]
            self._offset = len(self._data)
        else:
            chunk = self._data[self._offset : self._offset + n]
            self._offset += len(chunk)
        return chunk


class _SyncByteWriter:
    def __init__(self) -> None:
        self.written: list[bytes] = []
        self.closed = False

    def write(self, data: bytes) -> None:
        self.written.append(data)

    def close(self) -> None:
        self.closed = True


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


@pytest.mark.parametrize(
    ("mode", "options", "error_type"),
    [
        ("invalid", {}, ValueError),
        ("rb", {"encoding": "utf-8"}, ValueError),
        ("r", {"size": 1}, ValueError),
        ("rb", {"permissions": 0o600}, ValueError),
        ("wb", {"size": -1}, ValueError),
        ("wb", {"size": True}, TypeError),
        ("wb", {"permissions": 0o1000}, ValueError),
        ("wb", {"permissions": True}, TypeError),
    ],
)
@respx.mock
async def test_filesystem_open_rejects_invalid_options(
    mock_env_clear: None,
    mode: str,
    options: dict[str, object],
    error_type: type[Exception],
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(error_type):
            box.fs.open("data.bin", mode, **options)  # type: ignore[call-overload]


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
async def test_async_unknown_size_writer_publishes_temporary_spool(
    mock_env_clear: None,
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    write = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/write").mock(
        return_value=httpx.Response(204)
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        async with box.fs.open("spooled.bin", "wb") as writer:
            await writer.write(b"spooled")
            await writer.write(b" data")
            assert write.call_count == 0

    assert _tar_entries(write.calls[0].request.content) == {
        "vercel/sandbox/spooled.bin": (b"spooled data", 0o644)
    }


@pytest.mark.parametrize("content", [b"", b"abc"])
@respx.mock
async def test_async_binary_writer_rejects_incomplete_declared_size(
    mock_env_clear: None, content: bytes
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/write").mock(
        return_value=httpx.Response(204)
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(SandboxUploadSizeMismatchError) as exc_info:
            async with box.fs.open("data.bin", "wb", size=4) as writer:
                await writer.write(content)

    error = exc_info.value
    assert (error.path, error.declared, error.consumed, error.early_end) == (
        "data.bin",
        4,
        len(content),
        True,
    )


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
    first_write = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/write").mock(
        return_value=httpx.Response(204)
    )
    second_write = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_2/fs/write").mock(
        return_value=httpx.Response(204)
    )
    first_read = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read").mock(
        return_value=httpx.Response(200, content=b"original")
    )
    second_read = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_2/fs/read").mock(
        return_value=httpx.Response(200, content=b"current")
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        retained_box_fs = box.fs
        assert box.current_session is not None
        retained_session_fs = box.current_session.fs
        await box.update(current_snapshot_id="snap_1")
        await retained_box_fs.mkdir("current")
        await retained_session_fs.mkdir("original")
        async with retained_box_fs.open("current.bin", "wb", size=7) as current_writer:
            await current_writer.write(b"current")
        async with retained_session_fs.open("original.bin", "wb", size=8) as original_writer:
            await original_writer.write(b"original")
        async with retained_box_fs.open("current.bin", "rb") as current_reader:
            assert await current_reader.read() == b"current"
        async with retained_session_fs.open("original.bin", "rb") as original_reader:
            assert await original_reader.read() == b"original"

    assert second.call_count == 1
    assert first.call_count == 1
    assert second_write.call_count == 1
    assert first_write.call_count == 1
    assert second_read.call_count == 1
    assert first_read.call_count == 1


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
def test_sync_unknown_size_writer_publishes_temporary_spool(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    write = respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/write").mock(
        return_value=httpx.Response(204)
    )

    with vercel.session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        with box.fs.open("spooled.bin", "wb") as writer:
            writer.write(b"spooled")
            writer.write(b" data")
            assert write.call_count == 0

    assert _tar_entries(write.calls[0].request.content) == {
        "vercel/sandbox/spooled.bin": (b"spooled data", 0o644)
    }


@pytest.mark.parametrize("content", [b"", b"abc"])
@respx.mock
def test_sync_binary_writer_rejects_incomplete_declared_size(
    mock_env_clear: None, content: bytes
) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/write").mock(
        return_value=httpx.Response(204)
    )

    with vercel.session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(SandboxUploadSizeMismatchError) as exc_info:
            with box.fs.open("data.bin", "wb", size=4) as writer:
                writer.write(content)

    error = exc_info.value
    assert (error.path, error.declared, error.consumed, error.early_end) == (
        "data.bin",
        4,
        len(content),
        True,
    )


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


class _TrackedAsyncStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes], *, failure: BaseException | None = None) -> None:
        self._chunks = chunks
        self._failure = failure
        self.aclose_called = False

    async def __aiter__(self) -> "AsyncIterator[bytes]":
        for chunk in self._chunks:
            yield chunk
        if self._failure is not None:
            raise self._failure

    async def aclose(self) -> None:
        self.aclose_called = True


class _TrackedSyncStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes], *, failure: BaseException | None = None) -> None:
        self._chunks = chunks
        self._failure = failure
        self.close_called = False

    def __iter__(self) -> Iterator[bytes]:
        yield from self._chunks
        if self._failure is not None:
            raise self._failure

    def close(self) -> None:
        self.close_called = True


@given(
    prefix=st.text(
        alphabet=st.characters(min_codepoint=0, max_codepoint=0xD7FF, exclude_characters="\r\n")
    ),
    suffix=st.text(
        alphabet=st.characters(min_codepoint=0, max_codepoint=0xD7FF, exclude_characters="\r\n")
    ),
    chunk_size=st.integers(min_value=1, max_value=8),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
async def test_text_reader_preserves_crlf_split_across_chunks(
    mock_env_clear: None,
    prefix: str,
    suffix: str,
    chunk_size: int,
) -> None:
    prefix_bytes = prefix.encode()
    suffix_bytes = suffix.encode()
    chunks = [
        *(
            prefix_bytes[offset : offset + chunk_size]
            for offset in range(0, len(prefix_bytes), chunk_size)
        ),
        b"\r",
        b"\n",
        *(
            suffix_bytes[offset : offset + chunk_size]
            for offset in range(0, len(suffix_bytes), chunk_size)
        ),
    ]

    with respx.mock:
        respx.post("https://sandbox.test/v2/sandboxes").mock(
            return_value=httpx.Response(200, json=_sandbox_response())
        )
        respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read").mock(
            return_value=httpx.Response(200, stream=_TrackedAsyncStream(chunks))
        )

        async with vercel.session(service_options=_session_options()):
            box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
            async with box.fs.open("data.txt", "r", newline="") as reader:
                assert await reader.readline() == f"{prefix}\r\n"
                assert await reader.read() == suffix


@respx.mock
async def test_read_bytes_response_closed_after_streaming_read(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    stream = _TrackedAsyncStream([b"bytes"])
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        result = await box.fs.read_bytes(PurePosixPath("data.bin"))
        assert result == b"bytes"

    assert stream.aclose_called


@respx.mock
def test_sync_read_bytes_uses_and_closes_unread_stream(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    stream = _TrackedSyncStream([b"abc", b"", b"def"])
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    with vercel.session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        assert box.fs.read_bytes("data.bin") == b"abcdef"

    assert stream.close_called


@respx.mock
def test_sync_read_bytes_closes_response_after_stream_failure(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    failure = RuntimeError("stream failed")
    stream = _TrackedSyncStream([b"partial"], failure=failure)
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read").mock(
        return_value=httpx.Response(200, stream=stream)
    )

    with vercel.session(service_options=_session_options()):
        box = sandbox_sync.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(RuntimeError) as exc_info:
            box.fs.read_bytes("data.bin")
        assert exc_info.value is failure

    assert stream.close_called


@respx.mock
async def test_read_bytes_multiple_chunks(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read").mock(
        return_value=httpx.Response(200, stream=_TrackedAsyncStream([b"abc", b"def", b"ghi"]))
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        result = await box.fs.read_bytes(PurePosixPath("data.bin"))
        assert result == b"abcdefghi"


@respx.mock
async def test_read_bytes_empty_file(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read").mock(
        return_value=httpx.Response(200, stream=_TrackedAsyncStream([]))
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        result = await box.fs.read_bytes(PurePosixPath("empty.txt"))
        assert result == b""


@respx.mock
async def test_read_bytes_missing_path(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read").mock(
        return_value=httpx.Response(
            404, json={"error": {"code": "not_found", "message": "missing"}}
        )
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")
        with pytest.raises(SandboxPathNotFoundError) as exc_info:
            await box.fs.read_bytes(PurePosixPath("missing.txt"))
        assert exc_info.value.path == "missing.txt"
        assert exc_info.value.operation == "read_bytes"
        assert exc_info.value.cause.code == "not_found"


@respx.mock
async def test_read_bytes_and_read_text_still_work(mock_env_clear: None) -> None:
    respx.post("https://sandbox.test/v2/sandboxes").mock(
        return_value=httpx.Response(200, json=_sandbox_response())
    )
    response_count = 0

    def stream_response(request: httpx.Request) -> httpx.Response:
        nonlocal response_count
        response_count += 1
        return httpx.Response(200, stream=_TrackedAsyncStream([b"content"]))

    respx.post("https://sandbox.test/v2/sandboxes/sessions/sbx_1/fs/read").mock(
        side_effect=stream_response
    )

    async with vercel.session(service_options=_session_options()):
        box = await sandbox.create_sandbox(name="preview", runtime="python3.13")

        raw = await box.fs.read_bytes(PurePosixPath("data.bin"))
        assert raw == b"content"

        text = await box.fs.read_text("message.txt")
        assert text == "content"

    assert response_count == 2
