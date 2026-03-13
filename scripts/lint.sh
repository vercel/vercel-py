#!/usr/bin/env bash
set -euo pipefail

uv run pytest tests/test_sourcecode.py -k "Lint" -v "$@"
