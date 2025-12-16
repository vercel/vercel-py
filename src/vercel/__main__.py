"""CLI entry point that delegates to npx vercel."""

import os
import shutil
import sys


def main():
    """Run npx vercel with all command line arguments."""

    if not shutil.which("npx"):
        print(
            "Error: 'npx' is not available. Please install Node.js and npm to use this command.",
            file=sys.stderr,
        )
        return 1

    os.execvp("npx", ["npx", "vercel"] + sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
