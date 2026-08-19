"""``python -m vercel.workflow`` -- see `vercel.workflow._internal.cli`."""

import sys

from vercel.workflow._internal.cli import main

if __name__ == "__main__":
    sys.exit(main())
