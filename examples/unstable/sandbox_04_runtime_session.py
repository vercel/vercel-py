#!/usr/bin/env python3
"""Create an unstable Sandbox runtime session."""

import asyncio
from uuid import uuid4

from dotenv import load_dotenv

from vercel.unstable import sandbox

load_dotenv()


async def main() -> None:
    name = f"vercel-py-runtime-{uuid4().hex[:12]}"
    async with sandbox.create_sandbox(
        name=name,
        runtime="python3.13",
        execution_time_limit=60_000,
    ) as sandbox_:
        async with sandbox_.session() as runtime_session:
            result = await runtime_session.run_command("python", ["--version"])
            print(f"runtime session command exited with {result.exit_code}")


if __name__ == "__main__":
    asyncio.run(main())
