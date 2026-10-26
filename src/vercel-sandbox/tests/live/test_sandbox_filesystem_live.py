"""Filesystem mutation semantics against a live Linux Sandbox, in both I/O modes."""

import inspect
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import pytest

from vercel.sandbox import SandboxFilesystemCommandError

from ._sandbox_scenarios import AsyncDriver, SyncDriver
from .conftest import requires_sandbox_credentials


async def _call(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    result = method(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


# @lat: [[vercel-sandbox#Filesystem mutations#Live filesystem mutation tests]]
@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.parametrize("driver", [AsyncDriver(), SyncDriver()], ids=["async", "sync"])
async def test_filesystem_mutations(driver: Any) -> None:
    async with driver.session():
        async with driver.ephemeral_sandbox(f"vercel-py-fs-{uuid4().hex[:10]}") as box:
            fs = box.fs
            directory = await _call(fs.mkdtemp, prefix="fs ' ; $()X")
            try:
                other = await _call(fs.mkdtemp, prefix="fs ' ; $()X", dir=directory)
                assert other.startswith(directory + "/fs ' ; $()X")
                filename = "-data ' ; $()\n"
                path = directory + "/" + filename
                await _call(fs.write_bytes, path, b"original\0\xff")
                await _call(fs.write_text, path, "café", encoding="latin1", append=True)
                await _call(fs.write_bytes, path, b"\0\xff", append=True)
                expected = b"original\0\xffcaf\xe9\0\xff"
                assert await _call(fs.read_bytes, path) == expected
                await _call(fs.write_bytes, path, b"", append=True)
                assert await _call(fs.read_bytes, path) == expected
                await _call(fs.chmod, filename, 0o640, cwd=directory)
                metadata = await _call(
                    box.run_process,
                    "stat",
                    ["-c", "%u:%g:%a", "--", path],
                    capture_output=True,
                    check=True,
                )
                uid, gid, mode = metadata.stdout.strip().split(":")
                assert mode == "640"
                # Exercise a real ownership change through a privileged user context.
                root_fs = box.as_user("root").fs
                await _call(root_fs.chown, path, "root", "root")
                await _call(root_fs.chown, path, int(uid), int(gid))
                await _call(fs.chown, path, uid)  # Omitting gid preserves the group.
                metadata = await _call(
                    box.run_process,
                    "stat",
                    ["-c", "%u:%g", "--", path],
                    capture_output=True,
                    check=True,
                )
                assert metadata.stdout.strip() == f"{uid}:{gid}"
                await _call(fs.symlink, filename, "link", cwd=directory)
                assert await _call(fs.read_bytes, directory + "/link") == expected
                await _call(fs.copyfile, "link", "copy", cwd=directory)
                assert await _call(fs.read_bytes, directory + "/copy") == expected
                await _call(fs.truncate, "copy", 3, cwd=directory)
                assert await _call(fs.read_bytes, directory + "/copy") == b"ori"
                await _call(fs.truncate, "copy", 5, cwd=directory)
                assert await _call(fs.read_bytes, directory + "/copy") == b"ori\0\0"
                await _call(fs.truncate, "copy", cwd=directory)
                assert await _call(fs.read_bytes, directory + "/copy") == b""
                await _call(fs.write_text, "created", "new", append=True, cwd=directory)
                assert await _call(fs.read_text, "created", cwd=directory) == "new"
                for method, args, kwargs in [
                    (fs.chmod, ("missing", 0o600), {}),
                    (fs.chown, ("missing", int(uid)), {}),
                    (fs.symlink, (filename, "link"), {}),
                    (fs.truncate, ("missing/file",), {}),
                    (fs.mkdtemp, ("tmp", "missing"), {}),
                    (fs.copyfile, (filename, "."), {}),
                    (fs.write_bytes, ("missing/file", b"data"), {"append": True}),
                ]:
                    with pytest.raises(SandboxFilesystemCommandError):
                        await _call(method, *args, cwd=directory, **kwargs)
            finally:
                await _call(fs.remove, directory, recursive=True)
