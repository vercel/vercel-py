#!/usr/bin/env python3
"""Example: Low-level async PTY session.

This example exercises ``AsyncSandbox.open_pty()`` and
``vercel.sandbox.pty.AsyncPTYSession`` directly without taking over the local
terminal or assembling PTY websocket/bootstrap details manually.

Usage:
    python examples/sandbox_14_pty_test.py
"""

import asyncio
from datetime import timedelta

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox
from vercel.sandbox.pty import AsyncPTYSession

load_dotenv()

EXPECTED_OUTPUT = "PTY_OK"


async def collect_output(session: AsyncPTYSession, *, timeout: float = 30.0) -> bytes:
    """Read PTY output until the expected marker appears."""

    async def _collect() -> bytes:
        output = b""
        async for data in session.iter_output():
            output += data
            if EXPECTED_OUTPUT.encode() in output:
                return output
        return output

    return await asyncio.wait_for(_collect(), timeout=timeout)


async def main() -> int:
    print("=" * 60)
    print("Low-level AsyncPTYSession Example")
    print("=" * 60)
    print()
    print("Creating sandbox with interactive=True...")

    async with await AsyncSandbox.create(
        interactive=True,
        timeout=timedelta(minutes=5),
    ) as sandbox:
        print(f"Sandbox ID: {sandbox.sandbox_id}")
        print(f"Interactive port: {sandbox.interactive_port}")
        print()
        print("Opening PTY session via AsyncSandbox.open_pty()...")

        async with await sandbox.open_pty(
            ["/bin/bash"],
            cols=100,
            rows=30,
        ) as session:
            print(f"PTY process ID: {session.process_id}")
            print(f"PTY server process ID: {session.server_process_id}")
            print()
            print("Sending ready signal and resize event...")
            await session.ready()
            await session.resize(100, 30)
            print("Writing a simple command through the PTY...")
            await session.write(f"printf '{EXPECTED_OUTPUT}\\n'; pwd; exit\n")

            try:
                output = await collect_output(session)
            except asyncio.TimeoutError:
                print("Timed out waiting for PTY output.")
                return 1

            output_text = output.decode("utf-8", errors="replace")
            print()
            print("Received PTY output:")
            print("-" * 60)
            print(output_text.rstrip())
            print("-" * 60)

            if EXPECTED_OUTPUT not in output_text:
                print("Expected PTY output marker was not observed.")
                return 1

    print()
    print("Low-level PTY session flow completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
