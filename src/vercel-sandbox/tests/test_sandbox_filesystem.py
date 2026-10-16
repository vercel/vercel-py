"""The sandbox filesystem: reads, writes, batches, streams, and recovery."""

from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import PurePosixPath
from typing import Any

import httpx2 as httpx
import pytest
from fake_sandbox import FakeSandboxApi, RecordedRequest
from hypothesis import HealthCheck, given, settings, strategies as st
from sandbox_modes import Mode

from vercel import sandbox
from vercel.sandbox import (
    DirectoryEntry,
    SandboxApiError,
    SandboxFilesystemCommandError,
    SandboxFilesystemWriteError,
    SandboxPathNotFoundError,
    SandboxUploadSizeMismatchError,
    sync as sandbox_sync,
)

_CWD = "/vercel/sandbox"


def _resumes(request: RecordedRequest) -> bool:
    return request.params.get("resume") == "true"


def _reads(path: str) -> Callable[[RecordedRequest], bool]:
    return lambda request: (request.json() or {}).get("path") == path


def _trail(fake_api: FakeSandboxApi) -> list[str]:
    trail = []
    for request in fake_api.requests:
        if request.route == "sandboxes.get":
            trail.append("resume" if _resumes(request) else "lookup")
        else:
            trail.append(f"{request.route}@{request.path_params['session_id']}")
    return trail


class _TrackedStream(httpx.AsyncByteStream, httpx.SyncByteStream):
    """A response body that delivers fixed chunks, may fail, and records closing."""

    def __init__(self, chunks: list[bytes], *, failure: BaseException | None = None) -> None:
        self._chunks = chunks
        self._failure = failure
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk
        if self._failure is not None:
            raise self._failure

    def __iter__(self) -> Iterator[bytes]:
        yield from self._chunks
        if self._failure is not None:
            raise self._failure

    async def aclose(self) -> None:
        self.closed = True

    def close(self) -> None:
        self.closed = True


async def _batch_write(mode: Mode, box: Any) -> None:
    async with mode.enter(box.fs.batch()) as batch:
        batch.write_text("message.txt", "replacement")


_RECOVERABLE: dict[str, tuple[Callable[[Mode, Any], Any], str]] = {
    "read_bytes": (lambda mode, box: mode(box.fs.read_bytes("message.txt")), "fs.read"),
    "write_text": (
        lambda mode, box: mode(box.fs.write_text("message.txt", "replacement")),
        "fs.write",
    ),
    "batch": (_batch_write, "fs.write"),
    "listdir": (lambda mode, box: mode(box.fs.listdir("directory")), "commands.create"),
    "rename": (lambda mode, box: mode(box.fs.rename("before.txt", "after.txt")), "commands.create"),
}


@pytest.mark.parametrize("operation", list(_RECOVERABLE))
async def test_operations_on_a_stopped_session_replay_on_a_replacement(
    mode: Mode, fake_api: FakeSandboxApi, operation: str
) -> None:
    fake_api.seed(files={"message.txt": "original", "directory/listed": "", "before.txt": "moved"})
    run, route = _RECOVERABLE[operation]

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        fake_api.stop("preview")
        result = await run(mode, box)

    assert box.current_session_id == "sbx_2"
    # Command-backed operations go on to wait for the command and read its logs.
    assert _trail(fake_api)[:4] == ["lookup", f"{route}@sbx_1", "resume", f"{route}@sbx_2"]
    files = fake_api.current_session("preview").fs
    if operation == "read_bytes":
        assert result == b"original"
    elif operation == "listdir":
        assert result == [DirectoryEntry(path="listed", kind="file")]
    elif operation == "rename":
        assert files.read(f"{_CWD}/after.txt") == b"moved"
    else:
        assert files.read(f"{_CWD}/message.txt") == b"replacement"


async def test_a_lazy_reader_binds_the_session_it_finally_reads_from(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed(files={"message.txt": "persisted"})

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        reader = box.fs.open("message.txt", "rb")
        assert fake_api.routes() == ["sandboxes.get"]
        fake_api.stop("preview")
        async with mode.enter(reader):
            assert await mode(reader.read()) == b"persisted"

    assert box.current_session_id == "sbx_2"
    assert _trail(fake_api) == ["lookup", "fs.read@sbx_1", "resume", "fs.read@sbx_2"]


async def test_an_open_reader_stays_on_its_session_while_others_recover(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed(files={"recover.txt": "recovered"})
    failure = RuntimeError("stream broke")
    stream = _TrackedStream([b"first", b"-second"], failure=failure)
    fake_api.respond("fs.read", httpx.Response(200, stream=stream), when=_reads("stream.txt"))

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        async with mode.enter(box.fs.open("stream.txt", "rb")) as reader:
            assert await mode(reader.read(1)) == b"f"
            fake_api.stop("preview")
            assert await mode(box.fs.read_bytes("recover.txt")) == b"recovered"
            assert box.current_session_id == "sbx_2"
            assert await mode(reader.read(6)) == b"irst-s"
            with pytest.raises(RuntimeError) as exc_info:
                await mode(reader.read())

    assert exc_info.value is failure
    assert stream.closed
    assert _trail(fake_api) == [
        "lookup",
        "fs.read@sbx_1",
        "fs.read@sbx_1",
        "resume",
        "fs.read@sbx_2",
    ]


async def test_a_sized_writer_pins_its_session_and_does_not_recover(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed()

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        with pytest.raises(SandboxFilesystemWriteError) as exc_info:
            async with mode.enter(box.fs.open("stream.txt", "wb", size=7)) as writer:
                await mode(writer.write(b"partial"))
                fake_api.stop("preview")

    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "sandbox_stopped"
    assert _trail(fake_api) == ["lookup", "resume", "fs.write@sbx_1"]


@pytest.mark.parametrize("error_code", ["sandbox_stopping", "sandbox_snapshotting"])
async def test_a_sized_writer_retries_a_resume_that_lands_mid_transition(
    mode: Mode, fake_api: FakeSandboxApi, error_code: str
) -> None:
    fake_api.seed()
    fake_api.fail("sandboxes.get", status=422, code=error_code, when=_resumes)

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        fake_api.stop("preview")
        async with mode.enter(box.fs.open("stream.txt", "wb", size=4)) as writer:
            await mode(writer.write(b"data"))

    assert _trail(fake_api) == ["lookup", "resume", "resume", "fs.write@sbx_2"]
    assert fake_api.current_session("preview").fs.read(f"{_CWD}/stream.txt") == b"data"


async def test_an_unsized_writer_spools_and_recovers_when_it_publishes(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed()

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        async with mode.enter(box.fs.open("replay.txt", "wb")) as writer:
            await mode(writer.write(b"replay"))
            await mode(writer.write(b"able"))
            assert fake_api.routes() == ["sandboxes.get"]
            fake_api.stop("preview")

    assert _trail(fake_api) == ["lookup", "fs.write@sbx_1", "resume", "fs.write@sbx_2"]
    assert fake_api.current_session("preview").fs.read(f"{_CWD}/replay.txt") == b"replayable"


async def test_session_filesystems_stay_on_their_session(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed(files={"message.txt": "original"})

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        box_fs = box.fs
        session_fs = box.current_session.fs
        lazy_reader = session_fs.open("message.txt", "rb")
        fake_api.stop("preview")

        with pytest.raises(SandboxApiError, match="no longer available"):
            await mode(session_fs.read_bytes("message.txt"))
        with pytest.raises(SandboxApiError, match="no longer available"):
            async with mode.enter(lazy_reader):
                pass
        assert not fake_api.requests_to("sandboxes.get", when=_resumes)

        await mode(box_fs.mkdir("current"))
        async with mode.enter(box_fs.open("current.bin", "wb", size=7)) as writer:
            await mode(writer.write(b"current"))

    replacement = fake_api.current_session("preview")
    assert replacement.id == "sbx_2"
    assert replacement.fs.is_dir(f"{_CWD}/current")
    assert replacement.fs.read(f"{_CWD}/current.bin") == b"current"
    assert len(fake_api.requests_to("fs.read")) == 2


@pytest.mark.parametrize(
    ("mode_", "options", "error_type"),
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
async def test_open_rejects_invalid_options(
    fake_api: FakeSandboxApi, mode_: str, options: dict[str, object], error_type: type[Exception]
) -> None:
    async with fake_api.session():
        box = await sandbox.create_sandbox(name="preview")
        with pytest.raises(error_type):
            box.fs.open("data.bin", mode_, **options)  # type: ignore[call-overload]


async def test_native_operations_write_bytes_text_and_modes(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        filesystem_type = (
            sandbox_sync.SyncSandboxFilesystem if mode.sync else sandbox.SandboxFilesystem
        )
        assert isinstance(box.fs, filesystem_type)
        assert isinstance(box.current_session.fs, filesystem_type)
        for method in ("mkdir", "read_file", "read_text", "write_files"):
            assert not hasattr(box, method)
            assert not hasattr(box.current_session, method)

        await mode(box.fs.mkdir(PurePosixPath("parent"), recursive=False))
        await mode(box.fs.write_bytes("data.bin", b"\x00\x01", mode=0o600))
        await mode(box.fs.write_text("message.txt", "hello", mode=0o640))
        await mode(
            box.fs.write_text(PurePosixPath("file.txt"), "relative", cwd=PurePosixPath("workspace"))
        )
        assert await mode(box.fs.read_bytes(PurePosixPath("data.bin"))) == b"\x00\x01"
        assert await mode(box.fs.read_text("message.txt")) == "hello"
        assert await mode(box.fs.exists("workspace/file.txt"))

    files = fake_api.current_session("preview").fs
    assert files.is_dir(f"{_CWD}/parent")
    assert (files.read(f"{_CWD}/data.bin"), files.mode(f"{_CWD}/data.bin")) == (b"\x00\x01", 0o600)
    assert (files.read(f"{_CWD}/message.txt"), files.mode(f"{_CWD}/message.txt")) == (
        b"hello",
        0o640,
    )
    path = f"{_CWD}/workspace/file.txt"
    assert (files.read(path), files.mode(path)) == (b"relative", 0o644)
    assert fake_api.requests_to("fs.mkdir")[0].json() == {"path": "parent", "recursive": False}
    assert len(fake_api.requests_to("fs.write")) == 3


async def test_an_unsized_writer_uploads_once_at_close(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        async with mode.enter(box.fs.open("spooled.bin", "wb")) as writer:
            await mode(writer.write(b"spooled"))
            await mode(writer.write(b" data"))
            assert not fake_api.requests_to("fs.write")

    assert len(fake_api.requests_to("fs.write")) == 1
    assert fake_api.current_session("preview").fs.read(f"{_CWD}/spooled.bin") == b"spooled data"


@pytest.mark.parametrize("content", [b"", b"abc"])
async def test_a_sized_writer_rejects_short_content(
    mode: Mode, fake_api: FakeSandboxApi, content: bytes
) -> None:
    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        with pytest.raises(SandboxUploadSizeMismatchError) as exc_info:
            async with mode.enter(box.fs.open("data.bin", "wb", size=4)) as writer:
                await mode(writer.write(content))

    error = exc_info.value
    assert (error.path, error.declared, error.consumed, error.early_end) == (
        "data.bin",
        4,
        len(content),
        True,
    )
    assert not fake_api.current_session("preview").fs.exists(f"{_CWD}/data.bin")


async def test_write_failures_name_every_path_in_the_request(fake_api: FakeSandboxApi) -> None:
    fake_api.seed(files={"workspace/b.bin/inner": "a directory is in the way"})

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        with pytest.raises(SandboxFilesystemWriteError) as exc_info:
            async with box.fs.batch(cwd=PurePosixPath("workspace")) as batch:
                batch.write_text(PurePosixPath("a.txt"), "a")
                batch.write_bytes("b.bin", b"b")

    error = exc_info.value
    assert error.paths == ("a.txt", "b.bin")
    assert error.cwd == f"{_CWD}/workspace"
    assert isinstance(error.cause, SandboxApiError)
    assert error.cause.code == "write_file_failed"


async def test_batches_upload_once_and_skip_empty_or_aborted_batches(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        staged = box.fs.batch(cwd="workspace")
        with pytest.raises(RuntimeError):
            staged.write_text("outside.txt", "no")
        async with mode.enter(staged) as batch:
            batch.write_text("message.txt", "cafe", encoding="ascii", mode=0o640)
            batch.write_bytes("data.bin", b"\x00", mode=0o600)
        with pytest.raises(RuntimeError):
            staged.write_bytes("after.bin", b"no")
        with pytest.raises(RuntimeError):
            async with mode.enter(staged):
                pass

        async with mode.enter(box.fs.batch()):
            pass
        with pytest.raises(ValueError):
            async with mode.enter(box.fs.batch()) as batch:
                batch.write_text("ignored.txt", "ignored")
                raise ValueError("abort")

    assert len(fake_api.requests_to("fs.write")) == 1
    files = fake_api.current_session("preview").fs
    assert files.read(f"{_CWD}/workspace/message.txt") == b"cafe"
    assert files.mode(f"{_CWD}/workspace/message.txt") == 0o640
    assert files.mode(f"{_CWD}/workspace/data.bin") == 0o600
    assert not files.exists(f"{_CWD}/ignored.txt")


async def test_command_backed_operations_pass_paths_as_arguments(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    unsafe = "-a; printf injected"
    fake_api.seed(
        files={unsafe: "unsafe", "directory/z name": "", "directory/.dot\nname/inner": ""}
    )

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        assert await mode(box.fs.exists(unsafe))
        assert not await mode(box.fs.is_file("missing"))
        assert await mode(box.fs.listdir("directory")) == [
            DirectoryEntry(path=".dot\nname", kind="directory"),
            DirectoryEntry(path="z name", kind="file"),
        ]
        await mode(box.fs.rename(unsafe, "destination"))
        await mode(box.fs.remove(unsafe, recursive=True, missing_ok=True))

    files = fake_api.current_session("preview").fs
    assert files.read(f"{_CWD}/destination") == b"unsafe"
    assert not files.exists(f"{_CWD}/{unsafe}")
    exists, _is_file, _listdir, rename, remove = (
        request.json()["args"] for request in fake_api.requests_to("commands.create")
    )
    assert all(unsafe not in args[1] for args in (exists, rename, remove))
    assert exists[3:] == [unsafe, "-e"]
    assert rename[3:] == [unsafe, "destination"]
    assert remove[3:] == [unsafe, "true", "true"]


async def test_filesystem_failures_follow_the_error_contract(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed()
    fake_api.respond(
        "fs.read",
        httpx.Response(404, json={"error": {"code": "unknown", "message": "missing"}}),
        when=_reads("unclassified"),
    )

    async with mode.session():
        box = await mode(mode.sandbox.get_sandbox(name="preview"))
        with pytest.raises(SandboxPathNotFoundError) as missing:
            await mode(box.fs.read_bytes(PurePosixPath("missing.txt")))
        assert missing.value.path == "missing.txt"
        assert missing.value.operation == "read_bytes"
        assert missing.value.cause.code == "not_found"

        with pytest.raises(SandboxApiError) as unclassified:
            await mode(box.fs.read_bytes("unclassified"))
        assert not isinstance(unclassified.value, SandboxPathNotFoundError)

        with pytest.raises(SandboxFilesystemCommandError) as failed:
            await mode(box.fs.rename("absent", "destination"))
        assert failed.value.operation == "rename"
        assert failed.value.paths == ("absent", "destination")
        assert failed.value.exit_code == 1
        assert failed.value.stderr == "mv: cannot stat 'absent': No such file or directory\n"


# ---- the wire: streamed reads -------------------------------------------------------


@pytest.mark.parametrize(
    "chunks", [[b"abc", b"", b"def"], [b"abcdef"], []], ids=["chunked", "whole", "empty"]
)
async def test_read_bytes_joins_the_stream_and_closes_it(
    mode: Mode, fake_api: FakeSandboxApi, chunks: list[bytes]
) -> None:
    stream = _TrackedStream(chunks)
    fake_api.respond("fs.read", httpx.Response(200, stream=stream))

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        assert await mode(box.fs.read_bytes("data.bin")) == b"".join(chunks)

    assert stream.closed


async def test_read_bytes_closes_a_stream_that_fails(mode: Mode, fake_api: FakeSandboxApi) -> None:
    failure = RuntimeError("stream failed")
    stream = _TrackedStream([b"partial"], failure=failure)
    fake_api.respond("fs.read", httpx.Response(200, stream=stream))

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))
        with pytest.raises(RuntimeError) as exc_info:
            await mode(box.fs.read_bytes("data.bin"))

    assert exc_info.value is failure
    assert stream.closed


_LINE_TEXT = st.text(
    alphabet=st.characters(min_codepoint=0, max_codepoint=0xD7FF, exclude_characters="\r\n")
)


@given(prefix=_LINE_TEXT, suffix=_LINE_TEXT, chunk_size=st.integers(min_value=1, max_value=8))
@settings(deadline=1000, suppress_health_check=[HealthCheck.function_scoped_fixture])
async def test_text_reads_keep_a_crlf_split_across_chunks(
    mock_env_clear: None, prefix: str, suffix: str, chunk_size: int
) -> None:
    def chunked(data: bytes) -> list[bytes]:
        return [data[offset : offset + chunk_size] for offset in range(0, len(data), chunk_size)]

    fake_api = FakeSandboxApi()
    fake_api.seed()
    stream = _TrackedStream([*chunked(prefix.encode()), b"\r", b"\n", *chunked(suffix.encode())])
    fake_api.respond("fs.read", httpx.Response(200, stream=stream))

    async with fake_api.session():
        box = await sandbox.get_sandbox(name="preview")
        async with box.fs.open("data.txt", "r", newline="") as reader:
            assert await reader.readline() == f"{prefix}\r\n"
            assert await reader.read() == suffix
