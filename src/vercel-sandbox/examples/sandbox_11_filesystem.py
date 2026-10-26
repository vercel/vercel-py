#!/usr/bin/env python3
"""Create a private workspace, append a log, copy files, and manage metadata."""

from uuid import uuid4

import anyio
from dotenv import load_dotenv

from vercel import sandbox
from vercel.api import session

load_dotenv()


async def main() -> None:
    async with session():
        async with sandbox.create_sandbox(name=f"vercel-py-files-{uuid4().hex[:12]}") as box:
            workspace = await box.fs.mkdtemp(prefix="workspace")
            try:
                await box.fs.write_text("run.log", "started\n", cwd=workspace)
                await box.fs.write_text("run.log", "finished\n", append=True, cwd=workspace)
                await box.fs.copyfile("run.log", "backup.log", cwd=workspace)
                await box.fs.chmod("backup.log", 0o600, cwd=workspace)
                identity = await box.run_process("id", ["-u"], capture_output=True, check=True)
                assert identity.stdout is not None
                await box.fs.chown("backup.log", int(identity.stdout.strip()), cwd=workspace)
                await box.fs.symlink("backup.log", "latest.log", cwd=workspace)
                print(await box.fs.read_text("latest.log", cwd=workspace), end="")
                await box.fs.truncate("run.log", cwd=workspace)
                assert await box.fs.read_text("run.log", cwd=workspace) == ""
            finally:
                await box.fs.remove(workspace, recursive=True)


if __name__ == "__main__":
    anyio.run(main)
