"""Exercise directory leases against the real proper-lockfile package."""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import anyio
import pytest
from anyio.streams.buffered import BufferedByteReceiveStream

from vercel.workflow._internal.file_lock import FileLockTimeoutError, file_lock

PROPER_LOCKFILE_VERSION = "4.1.2"


@pytest.fixture(scope="session")
def proper_lockfile_entry() -> Path:
    override = os.getenv("VERCEL_PROPER_LOCKFILE_JS")
    entry = Path(override).expanduser() / "index.js" if override else None
    if entry is not None and entry.is_file() and shutil.which("node"):
        return entry
    if override:
        reason = "VERCEL_PROPER_LOCKFILE_JS must name a package directory, and node must exist"
    elif shutil.which("node") and (npm := shutil.which("npm")):
        shared = (
            Path(tempfile.gettempdir()) / f"vercel-py-proper-lockfile-{PROPER_LOCKFILE_VERSION}"
        )
        entry = shared / "node_modules" / "proper-lockfile" / "index.js"
        if entry.is_file():
            return entry
        # Publish a private install atomically, as the devalue interop tests
        # do: parallel workers must not read an npm install in progress.
        staging = Path(tempfile.mkdtemp(prefix=f"{shared.name}-"))
        result = subprocess.run(
            [
                npm,
                "install",
                f"proper-lockfile@{PROPER_LOCKFILE_VERSION}",
                "--no-save",
                "--no-audit",
                "--no-fund",
                "--prefix",
                str(staging),
            ],
            capture_output=True,
            timeout=300,
            check=False,
        )
        installed = staging / "node_modules" / "proper-lockfile" / "index.js"
        if result.returncode == 0 and installed.is_file():
            try:
                staging.rename(shared)
            except OSError:
                if not entry.is_file():
                    return installed
                shutil.rmtree(staging)
            return entry
        shutil.rmtree(staging, ignore_errors=True)
        reason = "could not install proper-lockfile"
    else:
        reason = "node and npm are required"
    if os.getenv("CI"):
        pytest.fail(reason)
    pytest.skip(reason)


_NODE_LOCK = """
const lockfile = require(process.argv[1]);
(async () => {
  try {
    const release = await lockfile.lock(process.argv[2], {
      realpath: false, stale: 2000, update: 1000, retries: 0,
      onCompromised(error) { console.error(error); process.exit(2); },
    });
    console.log('acquired');
    if (process.argv[3] === 'hold') {
      process.stdin.once('data', async () => {
        await release();
        process.exit(0);
      });
    } else {
      await release();
    }
  } catch (error) {
    console.log(error.code);
  }
})();
"""


async def _attempt(entry: Path, target: Path) -> str:
    result = await anyio.run_process(["node", "-e", _NODE_LOCK, str(entry), str(target), "once"])
    return result.stdout.decode().strip()


@contextlib.asynccontextmanager
async def _holder(command):
    async with await anyio.open_process(command) as process:
        try:
            assert process.stdout is not None
            output = BufferedByteReceiveStream(process.stdout)
            with anyio.fail_after(10):
                assert (await output.receive_until(b"\n", 1024)).removesuffix(b"\r") == b"acquired"
            yield process
        finally:
            with anyio.CancelScope(shield=True):
                if process.returncode is None:
                    process.kill()
                await process.wait()


async def test_node_respects_python_heartbeat(tmp_path, proper_lockfile_entry) -> None:
    target = tmp_path / "resource"
    async with file_lock(target, stale=2) as lease:
        assert await _attempt(proper_lockfile_entry, target) == "ELOCKED"
        # Exceed both the stale threshold and the precision probe's possible
        # one-second offset; the Python heartbeat must keep Node out.
        await anyio.sleep(3.2)
        assert await _attempt(proper_lockfile_entry, target) == "ELOCKED"
        lease.check()
    assert await _attempt(proper_lockfile_entry, target) == "acquired"


async def test_python_respects_node_heartbeat_and_release(tmp_path, proper_lockfile_entry) -> None:
    target = tmp_path / "resource"
    command = ["node", "-e", _NODE_LOCK, str(proper_lockfile_entry), str(target), "hold"]
    async with _holder(command) as node:
        await anyio.sleep(3.2)
        with pytest.raises(FileLockTimeoutError):
            async with file_lock(target, stale=2, timeout=0):
                pytest.fail("stole Node's live lock")
        assert node.stdin is not None
        await node.stdin.send(b"release\n")
        assert await node.wait() == 0
        async with file_lock(target, stale=2, timeout=1):
            pass


async def test_python_recovers_crashed_node_lock(tmp_path, proper_lockfile_entry) -> None:
    target = tmp_path / "resource"
    command = ["node", "-e", _NODE_LOCK, str(proper_lockfile_entry), str(target), "hold"]
    async with _holder(command) as node:
        node.kill()
        await node.wait()
    path = Path(f"{target}.lock")
    assert path.is_dir()
    os.utime(path, (0, 0))
    async with file_lock(target, stale=2, timeout=0):
        pass
    assert not path.exists()


async def test_node_recovers_crashed_python_lock(tmp_path, proper_lockfile_entry) -> None:
    target = tmp_path / "resource"
    script = """
import sys
from pathlib import Path
import anyio
from vercel.workflow._internal.file_lock import file_lock
async def hold():
    async with file_lock(Path(sys.argv[1]), stale=2):
        print('acquired', flush=True)
        await anyio.sleep_forever()
anyio.run(hold)
"""
    async with _holder([sys.executable, "-c", script, str(target)]) as python:
        python.kill()
        await python.wait()
    path = Path(f"{target}.lock")
    assert path.is_dir()
    os.utime(path, (0, 0))
    assert await _attempt(proper_lockfile_entry, target) == "acquired"
    assert not path.exists()
