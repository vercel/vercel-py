#!/usr/bin/env python3
"""Example: Interactive Shell Session

This example demonstrates how to use the interactive shell feature
to get a full PTY-based terminal session in a Vercel Sandbox.

Prerequisites:
- Set VERCEL_TOKEN, VERCEL_TEAM_ID, and VERCEL_PROJECT_ID environment variables
- Or create a .env file with these values
- Or run from a Vercel Function with OIDC credentials

Usage:
    python examples/sandbox_13_interactive_shell.py          # Interactive bash shell
    python examples/sandbox_13_interactive_shell.py --python # Interactive Python REPL

Interactive mode will:
1. Create a sandbox with interactive support enabled
2. Drop you into an interactive bash shell
3. You can type commands, use arrow keys, tab completion, etc.
4. Press Ctrl+D or type 'exit' to end the session
"""

import asyncio
import os
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


if __name__ == "__main__":
    # This example is intentionally interactive; CI runs should skip it.
    is_ci = os.environ.get("CI") == "1"
    has_tty = sys.stdin.isatty() and sys.stdout.isatty()

    if len(sys.argv) > 1:
        if sys.argv[1] == "--python":
            asyncio.run(python_repl_example())
        else:
            print(f"Unknown flag: {sys.argv[1]}")
            print("Usage: python sandbox_13_interactive_shell.py [--python]")
            sys.exit(1)
    elif is_ci or not has_tty:
        reason = "CI environment detected" if is_ci else "no interactive TTY detected"
        print(f"Skipping interactive shell example: {reason}.")
        print("Run this example from a local terminal to exercise AsyncSandbox.shell().")
        sys.exit(0)
    else:
        asyncio.run(main())
