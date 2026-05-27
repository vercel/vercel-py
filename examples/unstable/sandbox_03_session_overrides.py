#!/usr/bin/env python3
"""Configure unstable Sandbox through a scoped SDK session."""

import asyncio
from uuid import uuid4

from dotenv import load_dotenv

from vercel import unstable as vercel
from vercel.unstable import sandbox
from vercel.unstable.sandbox import SandboxServiceOptions

load_dotenv()


async def main() -> None:
    name = f"vercel-py-session-{uuid4().hex[:12]}"
    async with vercel.session(service_options=[SandboxServiceOptions()]):
        async with sandbox.create_sandbox(
            name=name,
            runtime="python3.13",
            execution_time_limit=60_000,
        ) as sandbox_:
            print(f"created sandbox {sandbox_.name} using scoped options")


if __name__ == "__main__":
    asyncio.run(main())
