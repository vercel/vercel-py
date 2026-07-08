"""Command-line entry point for ``python -m vercel.queue``."""

from __future__ import annotations

import sys

from ._internal.cli import main

if __name__ == "__main__":
    sys.exit(main())
