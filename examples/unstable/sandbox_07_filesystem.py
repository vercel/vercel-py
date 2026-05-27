#!/usr/bin/env python3
"""Write, run, and read files in an unstable Sandbox session."""

import asyncio
from uuid import uuid4

from dotenv import load_dotenv

from vercel.unstable import sandbox
from vercel.unstable.sandbox import WriteFile

load_dotenv()


async def main() -> None:
    name = f"vercel-py-fs-{uuid4().hex[:12]}"
    async with sandbox.create_sandbox(
        name=name,
        runtime="python3.13",
        execution_time_limit=60_000,
    ) as sandbox_:
        await sandbox_.mkdir("work")
        await sandbox_.write_files(
            [
                WriteFile(
                    path="work/generate.py",
                    content=(
                        "from pathlib import Path\n"
                        "Path('work/output.txt').write_text('filesystem ok\\n')\n"
                    ),
                )
            ]
        )
        command = await sandbox_.run_command("python", ["work/generate.py"])
        assert command.exit_code == 0

        content = await sandbox_.read_text("work/output.txt")
        assert content == "filesystem ok\n"
        print(content, end="")


if __name__ == "__main__":
    asyncio.run(main())
