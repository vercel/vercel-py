"""Live metadata and symlink semantics for both filesystem APIs."""

import inspect
from pathlib import PurePosixPath
from typing import Any
from uuid import uuid4

import pytest

from vercel.sandbox import SandboxFilesystemCommandError, SandboxPathNotFoundError

from ._sandbox_scenarios import AsyncDriver, SyncDriver
from .conftest import requires_sandbox_credentials


async def _call(method: Any, *args: Any, **kwargs: Any) -> Any:
    result = method(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("driver", [AsyncDriver(), SyncDriver()], ids=["async", "sync"])
async def test_filesystem_metadata_and_symlinks(driver: Any) -> None:
    async with driver.session():
        async with driver.ephemeral_sandbox(f"vercel-py-fs-metadata-{uuid4().hex[:10]}") as box:
            cwd = PurePosixPath("workspace")
            target = "-target; $(touch injected) \n"
            await _call(box.fs.mkdir, cwd)
            await _call(box.fs.write_text, target, "hello", cwd=cwd, mode=0o640)
            for name, destination in [("link", target), ("chain", "link"), ("dangling", "absent")]:
                await _call(
                    box.run_process, "ln", ["-s", "--", destination, name], cwd=cwd, check=True
                )
            assert box.current_session is not None
            for fs in (box.fs, box.current_session.fs):
                info = await _call(fs.stat, PurePosixPath("link"), cwd=cwd)
                own = await _call(fs.lstat, "link", cwd=cwd)
                assert info.is_file() and not info.is_symlink()
                assert info.st_size == 5
                assert info.st_mode & 0o777 == 0o640
                assert info.st_mtime > 0
                assert info.st_uid >= 0 and info.st_gid >= 0
                assert own.is_symlink() and not own.is_file()
                assert (await _call(fs.stat, cwd)).is_dir()
                assert await _call(fs.readlink, "link", cwd=cwd) == target
                resolved = await _call(fs.realpath, "chain", cwd=cwd)
                assert resolved.startswith("/") and resolved.endswith("/workspace/" + target)
                assert (await _call(fs.stat, resolved)).st_ino == info.st_ino
                assert (await _call(fs.lstat, "dangling", cwd=cwd)).is_symlink()
                assert await _call(fs.readlink, "dangling", cwd=cwd) == "absent"
                for operation in (fs.stat, fs.lstat, fs.readlink, fs.realpath):
                    with pytest.raises(SandboxPathNotFoundError):
                        await _call(operation, "absent", cwd=cwd)
                for operation in (fs.stat, fs.realpath):
                    with pytest.raises(SandboxPathNotFoundError):
                        await _call(operation, "dangling", cwd=cwd)
                with pytest.raises(SandboxFilesystemCommandError):
                    await _call(fs.readlink, target, cwd=cwd)
            assert not await _call(box.fs.exists, "injected", cwd=cwd)
