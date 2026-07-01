#!/usr/bin/env python
from __future__ import annotations

import os
import sys


def main() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "chunks_project.settings")
    from django.core.management import execute_from_command_line  # noqa: PLC0415

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
