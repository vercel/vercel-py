#!/usr/bin/env python3
"""Run, inspect, stream, and terminate an unstable Sandbox command."""

import asyncio
from uuid import uuid4

from dotenv import load_dotenv

from vercel.unstable import sandbox

load_dotenv()


async def main() -> None:
    name = f"vercel-py-command-{uuid4().hex[:12]}"
    async with sandbox.create_sandbox(
        name=name,
        runtime="python3.13",
        execution_time_limit=60_000,
    ) as sandbox_:
        command = await sandbox_.start_command(
            "sh",
            ["-c", "for i in 1 2 3; do echo line-$i; sleep 1; done; sleep 30"],
        )
        print(f"started command {command.id}")

        async for event in command.logs():
            if event.stream == "stdout":
                print(event.data, end="")
                break

        fetched = await sandbox_.get_command(command.id)
        print(f"fetched command {fetched.id} with status {fetched.status}")

        commands = await sandbox_.query_commands()
        print(f"session has {len(commands)} command(s)")

        await command.kill()
        print(f"killed command {command.id}")


if __name__ == "__main__":
    asyncio.run(main())
