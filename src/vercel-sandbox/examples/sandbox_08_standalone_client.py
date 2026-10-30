#!/usr/bin/env python3
"""Inject one standalone Sandbox client into a long-lived service."""

import asyncio
from datetime import timedelta

import anyio
from dotenv import load_dotenv

from vercel.sandbox import SandboxClient, SandboxServiceOptions

load_dotenv()


class ScriptRunner:
    def __init__(self, client: SandboxClient) -> None:
        self._client = client

    async def run(self, script: str) -> str:
        async with self._client.create_sandbox(execution_time_limit=timedelta(minutes=1)) as box:
            # Bound the whole command call, including retries and output.
            with anyio.fail_after(30):
                result = await box.run_command(
                    "python",
                    ["-c", script],
                    capture_output=True,
                    check=True,
                )
            return result.stdout or ""


async def main() -> None:
    # Construction performs no I/O, so clients fit synchronous dependency
    # injection and application configuration. The application owns cleanup.
    client = SandboxClient.create(
        options=SandboxServiceOptions(request_timeout=timedelta(minutes=2)),
    )
    runner = ScriptRunner(client)
    try:
        print(await runner.run("print('hello from a standalone client')"), end="")
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
