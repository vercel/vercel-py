#!/usr/bin/env python3
"""Let isolated users collaborate through a group's shared directory."""

import asyncio
from datetime import timedelta
from uuid import uuid4

from dotenv import load_dotenv

from vercel import sandbox
from vercel.sandbox import SandboxFilesystemError

load_dotenv()


async def main() -> None:
    async with sandbox.create_sandbox(
        name=f"vercel-py-groups-{uuid4().hex[:12]}",
        execution_time_limit=timedelta(minutes=3),
    ) as box:
        researcher = await box.create_user("researcher")
        coder = await box.create_user("coder")
        reviewer = await box.create_user("reviewer")

        # Members read and write the group's shared directory; homes stay private.
        project = await box.create_group("project")
        await project.add_user(researcher)
        await coder.add_to_group(project)

        spec = f"{project.shared_dir}/spec.txt"
        await researcher.fs.write_text(spec, "API spec v2\n")
        print(f"coder reads {spec}: {(await coder.fs.read_text(spec)).strip()}")

        # Files created by commands in the shared directory belong to the group too.
        status = f"{project.shared_dir}/status.txt"
        await coder.run_command("bash", ["-c", 'echo done > "$1"', "write", status], check=True)
        print(f"researcher reads {status}: {(await researcher.fs.read_text(status)).strip()}")

        try:
            await reviewer.fs.read_text(spec)
        except SandboxFilesystemError:
            print("reviewer is not a member and cannot read the shared directory")
        else:
            raise AssertionError("group isolation failed")

        # Membership changes apply to the next command or read.
        await project.remove_user(coder)
        denied = await coder.run_command("cat", [spec], capture_output=True)
        assert denied.returncode != 0
        print("coder lost access after leaving the group")


if __name__ == "__main__":
    asyncio.run(main())
