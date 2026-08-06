#!/usr/bin/env python3
"""Fork a configured Sandbox and work on its filesystem in isolation."""

import asyncio
from datetime import timedelta
from uuid import uuid4

from dotenv import load_dotenv

from vercel import sandbox

load_dotenv()


async def main() -> None:
    suffix = uuid4().hex[:12]
    source_name = f"vercel-py-fork-source-{suffix}"
    fork_name = f"vercel-py-fork-child-{suffix}"
    snapshot = None

    try:
        async with sandbox.create_sandbox(
            name=source_name,
            execution_time_limit=timedelta(minutes=2),
            env={"INHERITED_MESSAGE": "hello from the source"},
            tags={"example": "fork-source"},
        ) as source:
            await source.fs.write_text("workspace/message.txt", "source version\n")

            # A fork starts from the source's current snapshot when it has one.
            # Creating one here makes the filesystem state copied by this demo
            # explicit and lets us clean it up when the example finishes.
            snapshot = await source.snapshot()

            # Omitted settings, including the environment, are inherited from
            # the source. Values passed here override the inherited setting.
            async with sandbox.fork_sandbox(
                source_sandbox=source.name,
                name=fork_name,
                tags={"example": "fork-child"},
            ) as forked:
                assert await forked.fs.read_text("workspace/message.txt") == "source version\n"

                result = await forked.run_process(
                    "python",
                    ["-c", "import os; print(os.environ['INHERITED_MESSAGE'])"],
                    capture_output=True,
                    check=True,
                )
                assert result.stdout == "hello from the source\n"

                await forked.fs.write_text("workspace/message.txt", "fork version\n")
                assert await source.fs.read_text("workspace/message.txt") == "source version\n"
                print(f"forked {source.name} as {forked.name} from {snapshot.id}")
    finally:
        if snapshot is not None:
            await snapshot.delete()


if __name__ == "__main__":
    asyncio.run(main())
