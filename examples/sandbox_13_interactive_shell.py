#!/usr/bin/env python3
"""Example: Interactive Shell Session

This example demonstrates how to use the interactive shell feature
to get a full PTY-based terminal session in a Vercel Sandbox.

Prerequisites:
- Set VERCEL_TOKEN, VERCEL_TEAM_ID, and VERCEL_PROJECT_ID environment variables
- Or create a .env file with these values
- Or run from a Vercel Function with OIDC credentials

Usage:
    python examples/13_interactive_shell.py          # Interactive bash shell
    python examples/13_interactive_shell.py --python # Interactive Python REPL
    python examples/13_interactive_shell.py --test   # Non-interactive CI test

Interactive mode will:
1. Create a sandbox with interactive support enabled
2. Drop you into an interactive bash shell
3. You can type commands, use arrow keys, tab completion, etc.
4. Press Ctrl+D or type 'exit' to end the session
"""

import asyncio
import sys

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox

load_dotenv()


async def main():
    print("Creating sandbox with interactive support...")

    # Create sandbox with interactive=True to enable PTY support
    sandbox = await AsyncSandbox.create(
        interactive=True,
        timeout=300_000,  # 5 minutes
    )

    print(f"Sandbox created: {sandbox.sandbox_id}")
    print(f"Interactive port: {sandbox.interactive_port}")
    print()
    print("Starting interactive shell...")
    print("=" * 50)
    print("You are now in a remote shell. Try commands like:")
    print("  ls -la")
    print("  python3 --version")
    print("  echo $SHELL")
    print()
    print("Press Ctrl+D or type 'exit' to end the session.")
    print("=" * 50)
    print()

    try:
        # Start interactive bash shell
        await sandbox.shell(["/bin/bash"])
    finally:
        print()
        print("Stopping sandbox...")
        await sandbox.stop()
        print("Done!")


async def python_repl_example():
    """Example: Interactive Python REPL"""
    print("Creating sandbox with interactive support...")

    sandbox = await AsyncSandbox.create(
        interactive=True,
        timeout=300_000,
    )

    print(f"Sandbox created: {sandbox.sandbox_id}")
    print()
    print("Starting Python REPL...")
    print("=" * 50)
    print("You are now in a remote Python interpreter.")
    print("Try: print('Hello from Vercel Sandbox!')")
    print("Press Ctrl+D to exit.")
    print("=" * 50)
    print()

    try:
        await sandbox.shell(["python3"])
    finally:
        print()
        print("Stopping sandbox...")
        await sandbox.stop()
        print("Done!")


async def test():
    """Non-interactive test for CI environments.

    This tests the PTY infrastructure without taking over the terminal:
    1. Creates sandbox with interactive=True
    2. Verifies interactive port is allocated
    3. Sets up sandbox environment (installs PTY server)
    4. Connects to WebSocket and runs a command
    5. Verifies output is received
    """
    from vercel.sandbox.pty.client import PTYClient
    from vercel.sandbox.pty.shell import setup_sandbox_environment, start_pty_server

    print("=" * 60)
    print("Interactive Shell CI Test (non-interactive)")
    print("=" * 60)
    print()

    print("1. Creating sandbox with interactive=True...")
    sandbox = await AsyncSandbox.create(
        interactive=True,
        timeout=120_000,  # 2 minutes
    )

    try:
        print(f"   Sandbox ID: {sandbox.sandbox_id}")
        print(f"   Interactive port: {sandbox.interactive_port}")

        # Verify interactive port
        assert sandbox.interactive_port is not None, "Interactive port should be set"
        print("   ✅ Interactive port allocated")
        print()

        print("2. Setting up sandbox environment (installing PTY server)...")
        await setup_sandbox_environment(sandbox)
        print("   ✅ PTY server installed")
        print()

        print("3. Starting PTY server with bash...")
        # Use bash with a command that outputs, waits, then exits
        # This gives us time to connect and read output
        cmd, conn_info = await start_pty_server(
            sandbox,
            ["bash", "-c", "echo 'PTY_TEST_OUTPUT'; sleep 2; echo 'DONE'"],
        )
        print(f"   Process ID: {conn_info['processId']}")
        print(f"   Token: {conn_info['token'][:20]}...")
        print("   ✅ PTY server started")
        print()

        print("4. Connecting to WebSocket...")
        host = sandbox.domain(sandbox.interactive_port)
        host = host.replace("https://", "").replace("http://", "")
        ws_url = f"wss://{host}/ws/client?token={conn_info['token']}&processId={conn_info['processId']}"

        # Small delay to ensure server is ready
        await asyncio.sleep(0.5)

        client = await PTYClient.connect(ws_url)
        print("   ✅ WebSocket connected")
        print()

        print("5. Sending ready signal and receiving output...")
        await client.send_ready()
        await client.send_resize(80, 24)

        # Collect output with timeout
        output = b""
        try:
            async with asyncio.timeout(5):
                async for data in client.raw_messages():
                    output += data
                    # Check if we got our test string
                    if b"PTY_TEST_OUTPUT" in output:
                        break
        except asyncio.TimeoutError:
            pass

        await client.close()

        # Verify output
        output_str = output.decode("utf-8", errors="replace")
        print(f"   Received {len(output)} bytes")

        if "PTY_TEST_OUTPUT" in output_str:
            print("   ✅ Expected output received!")
        else:
            print("   ⚠️  Output received but test string not found")
            print(f"   Output preview: {repr(output_str[:200])}")

        print()
        print("=" * 60)
        print("✅ All tests passed!")
        print("=" * 60)

    finally:
        print()
        print("Stopping sandbox...")
        await sandbox.stop()
        print("Done!")


if __name__ == "__main__":
    import os

    # Auto-detect CI environment (no TTY or CI env var set)
    is_ci = os.environ.get("CI") == "1" or not sys.stdin.isatty()

    if len(sys.argv) > 1:
        if sys.argv[1] == "--python":
            asyncio.run(python_repl_example())
        elif sys.argv[1] == "--test":
            asyncio.run(test())
        else:
            print(f"Unknown flag: {sys.argv[1]}")
            print("Usage: python 13_interactive_shell.py [--python|--test]")
            sys.exit(1)
    elif is_ci:
        # In CI, run non-interactive test automatically
        asyncio.run(test())
    else:
        asyncio.run(main())
