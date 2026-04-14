"""Interactive shell session management."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import termios
import tty
from typing import TYPE_CHECKING

import websockets

from vercel._internal.sandbox.pty_session import (
    build_ws_url,
    setup_sandbox_environment,
    start_pty_server,
)

from .client import PTYClient

if TYPE_CHECKING:
    from ..command import AsyncCommand
    from ..sandbox import AsyncSandbox


async def start_interactive_shell(
    sandbox: AsyncSandbox,
    command: list[str] | None = None,
    *,
    env: dict[str, str] | None = None,
    cwd: str | None = None,
    sudo: bool = False,
) -> None:
    """Start an interactive shell session with the sandbox.

    This takes over the terminal and provides a full interactive experience,
    forwarding stdin/stdout between the local terminal and the remote sandbox.

    Args:
        sandbox: The sandbox to connect to (must be created with interactive=True).
        command: Command to execute (default: ["/bin/bash"]).
        env: Additional environment variables.
        cwd: Working directory.
        sudo: Run with elevated privileges.

    Raises:
        RuntimeError: If sandbox doesn't have interactive support enabled.
    """
    if not sandbox.interactive_port:
        raise RuntimeError(
            "Sandbox was not created with interactive=True. "
            "Create with: await AsyncSandbox.create(interactive=True)"
        )

    command = command or ["/bin/bash"]

    # Setup sandbox environment (install PTY server if needed)
    print("Setting up interactive session...", file=sys.stderr)
    await setup_sandbox_environment(sandbox)

    # Start PTY server
    print("Starting PTY server...", file=sys.stderr)
    cmd, conn_info = await start_pty_server(sandbox, command, env=env, cwd=cwd, sudo=sudo)

    client = await PTYClient.connect(build_ws_url(sandbox, conn_info))
    try:
        await _run_interactive_loop(client, cmd)
    finally:
        await client.close()


async def _run_interactive_loop(client: PTYClient, cmd: AsyncCommand) -> None:
    """Main interactive loop - forward stdin/stdout between terminal and sandbox.

    Args:
        client: Connected PTY client.
        cmd: The PTY server command handle.
    """
    # Get initial terminal size
    try:
        cols, rows = os.get_terminal_size()
    except OSError:
        cols, rows = 80, 24

    # Send ready and initial resize
    await client.send_ready()
    await client.send_resize(cols, rows)

    # Save terminal settings
    stdin_fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(stdin_fd)

    # Event to signal shutdown
    stop_event = asyncio.Event()

    # Get the event loop for thread-safe scheduling
    loop = asyncio.get_running_loop()

    try:
        # Put terminal in raw mode
        tty.setraw(stdin_fd)

        # Handle SIGWINCH (terminal resize) - safely schedule from signal context
        def on_resize(signum, frame):
            try:
                new_cols, new_rows = os.get_terminal_size()
                loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(client.send_resize(new_cols, new_rows))
                )
            except OSError:
                pass

        signal.signal(signal.SIGWINCH, on_resize)

        # Create tasks for stdin and stdout forwarding
        stdin_task = asyncio.create_task(_forward_stdin(client, stop_event, loop))
        stdout_task = asyncio.create_task(_forward_stdout(client))

        # Wait for either to complete (connection closed or error)
        done, pending = await asyncio.wait(
            [stdin_task, stdout_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Signal tasks to stop and cancel remaining
        stop_event.set()
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    finally:
        # Restore terminal settings
        termios.tcsetattr(stdin_fd, termios.TCSADRAIN, old_settings)
        signal.signal(signal.SIGWINCH, signal.SIG_DFL)
        print("\n\rSession ended.", file=sys.stderr)


async def _forward_stdin(
    client: PTYClient, stop_event: asyncio.Event, loop: asyncio.AbstractEventLoop
) -> None:
    """Forward local stdin to the PTY client.

    Uses asyncio's add_reader for efficient non-blocking I/O instead of polling.

    Args:
        client: Connected PTY client.
        stop_event: Event that signals when to stop.
        loop: The event loop for registering the reader.
    """
    stdin_fd = sys.stdin.fileno()
    data_ready = asyncio.Event()

    def on_stdin_ready():
        data_ready.set()

    loop.add_reader(stdin_fd, on_stdin_ready)
    try:
        while not stop_event.is_set() and client.is_open:
            # Wait for stdin data or stop signal
            wait_task = asyncio.create_task(data_ready.wait())
            stop_task = asyncio.create_task(stop_event.wait())

            done, pending = await asyncio.wait(
                [wait_task, stop_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            if stop_event.is_set():
                break

            # Read and send available data
            data_ready.clear()
            try:
                data = os.read(stdin_fd, 4096)
                if data:
                    await client.send_input_bytes(data)
                else:
                    # EOF on stdin
                    break
            except OSError:
                break
    finally:
        loop.remove_reader(stdin_fd)


async def _forward_stdout(client: PTYClient) -> None:
    """Forward PTY output to local stdout.

    Args:
        client: Connected PTY client.
    """
    try:
        async for data in client.raw_messages():
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
    except websockets.ConnectionClosed:
        # Connection closing is expected when session ends
        pass
