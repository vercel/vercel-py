"""CLI entry point that delegates to npx sandbox."""

from __future__ import annotations

import os
import shutil
import sys


def main() -> int:
    """Run npx sandbox with all command line arguments.

    This delegates to the TypeScript CLI for full functionality including
    interactive shell, SSH, and other commands.

    Returns:
        Exit code (0 for success, non-zero for error).
    """
    if not shutil.which("npx"):
        print(
            "Error: 'npx' is not available. Please install Node.js and npm to use the CLI.",
            file=sys.stderr,
        )
        print(
            "\nNote: The SDK works without Node.js for programmatic use:",
            file=sys.stderr,
        )
        print(
            "  from vercel.sandbox import AsyncSandbox",
            file=sys.stderr,
        )
        print(
            "  sandbox = await AsyncSandbox.create()",
            file=sys.stderr,
        )
        return 1

    # Replace current process with npx sandbox
    os.execvp("npx", ["npx", "sandbox"] + sys.argv[1:])

    # This line is never reached (execvp replaces the process)
    return 0


if __name__ == "__main__":
    sys.exit(main())
