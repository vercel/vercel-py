#!/usr/bin/env bash
set -euo pipefail

uv run --all-packages pytest tests/test_sourcecode.py -k "Lint" -v "$@"
