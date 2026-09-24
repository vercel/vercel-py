#!/usr/bin/env python3
"""Drive an interactive PTY session inside a sandbox."""

import asyncio
from datetime import timedelta
from uuid import uuid4

from dotenv import load_dotenv

from vercel import sandbox
from vercel.api import session
from vercel.sandbox import InteractiveSession

load_dotenv()

READY = "PTY_READY"
DONE = "PTY_DONE"


async def _read_until(pty: InteractiveSession, marker: str, *, timeout: float = 60.0) -> str:
    """Collect output until ``marker`` appears on a line of its own.

    A PTY echoes input, so the marker text arrives once as the typed command
    before the shell ever runs it. Matching whole lines avoids acting on that
    echo.
    """

    async def collect() -> str:
        output = ""
        async for chunk in pty:
            output += chunk.decode("utf-8", errors="replace")
            if any(line.rstrip("\r") == marker for line in output.splitlines()):
                return output
        return output

    return await asyncio.wait_for(collect(), timeout=timeout)


async def main() -> None:
    async with session():
        await _main()


async def _main() -> None:
    name = f"vercel-py-interactive-{uuid4().hex[:12]}"

    async with sandbox.create_sandbox(
        name=name,
        execution_time_limit=timedelta(minutes=2),
    ) as box:
        async with box.open_interactive("/bin/bash", cols=100, rows=30) as pty:
            # The shell needs a round trip before it reliably echoes output, so
            # confirm it is live before sending the command under test.
            await pty.send(f"printf '\\n{READY}\\n'\n".encode())
            await _read_until(pty, READY)
            print("interactive shell is ready")

            await pty.resize(120, 40)

            await pty.send(f"tty; printf '\\n{DONE}\\n'\n".encode())
            output = await _read_until(pty, DONE)
            print("-" * 60)
            print(output.rstrip())
            print("-" * 60)

            # `tty` prints a terminal device only when the process owns a PTY.
            assert "/dev/pts/" in output, "command did not run under a PTY"

            await pty.send(b"exit 3\n")
            returncode = await pty.wait()
            print(f"interactive process exited with {returncode}")
            assert returncode == 3, f"unexpected exit code {returncode}"

    print("Interactive session example completed successfully.")


if __name__ == "__main__":
    asyncio.run(main())
