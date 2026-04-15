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
READY_OUTPUT = "PTY_READY"
DONE_OUTPUT = "PTY_DONE"
PROMPT_MARKER = "$ "


async def collect_output_until(
    session: AsyncPTYSession,
    marker: str,
    *,
    timeout: float = 30.0,
    require_full_line: bool = False,
) -> bytes:
    """Read PTY output until the expected marker appears."""

    async def _collect() -> bytes:
        output = b""
        marker_bytes = marker.encode()
        async for data in session.stream:
            output += data
            if require_full_line:
                if any(line.rstrip(b"\r") == marker_bytes for line in output.splitlines()):
                    return output
            elif marker_bytes in output:
                return output
        return output

    return await asyncio.wait_for(_collect(), timeout=timeout)


async def run_and_collect(
    session: AsyncPTYSession,
    command: str,
    marker: str,
    *,
    timeout: float = 30.0,
    require_full_line: bool = False,
) -> bytes:
    """Send a command to the PTY and collect output until a marker appears."""

    await session.stream.send(command.encode())
    return await collect_output_until(
        session,
        marker,
        timeout=timeout,
        require_full_line=require_full_line,
    )


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
            print("Waiting for the shell prompt...")
            try:
                initial_output = await collect_output_until(session, PROMPT_MARKER)
            except asyncio.TimeoutError:
                print("Timed out waiting for the shell prompt.")
                return 1

            initial_output_text = initial_output.decode("utf-8", errors="replace")
            print()
            print("Received initial PTY output:")
            print("-" * 60)
            print(initial_output_text.rstrip())
            print("-" * 60)

            print("Verifying PTY input/output round-trip...")
            try:
                ready_output = await run_and_collect(
                    session,
                    f"printf '{READY_OUTPUT}\\n'\n",
                    READY_OUTPUT,
                    require_full_line=True,
                )
            except asyncio.TimeoutError:
                print("Timed out waiting for PTY round-trip confirmation.")
                return 1

            ready_output_text = ready_output.decode("utf-8", errors="replace")
            print()
            print("Received PTY round-trip output:")
            print("-" * 60)
            print(ready_output_text.rstrip())
            print("-" * 60)

            print("Writing a simple command through the PTY stream...")

            try:
                output = await run_and_collect(
                    session,
                    f"printf '{EXPECTED_OUTPUT}\\n'; pwd; printf '{DONE_OUTPUT}\\n'; exit\n",
                    DONE_OUTPUT,
                    require_full_line=True,
                )
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
            if DONE_OUTPUT not in output_text:
                print("PTY command completion marker was not observed.")
                return 1

    print()
    print("Low-level PTY session flow completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
