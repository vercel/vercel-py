#!/usr/bin/env python3
"""Inject one standalone Sandbox client into a long-lived service."""

import asyncio
from datetime import timedelta

from dotenv import load_dotenv

from vercel.sandbox import SandboxClient

load_dotenv()


class ScriptRunner:
    def __init__(self, client: SandboxClient) -> None:
        self._client = client

    async def run(self, script: str) -> str:
        async with self._client.create_sandbox(execution_time_limit=timedelta(minutes=1)) as box:
            result = await box.run_process(
                "python",
                ["-c", script],
                capture_output=True,
                check=True,
            )
            return result.stdout or ""


async def main() -> None:
    # Construction performs no I/O, so clients fit synchronous dependency
    # injection and application configuration. The application owns cleanup.
    client = SandboxClient.create()
    runner = ScriptRunner(client)
    try:
        print(await runner.run("print('hello from a standalone client')"), end="")
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
