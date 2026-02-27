#!/usr/bin/env bash
set -euo pipefail

uv run pytest -v --capture=tee-sys tests/test_examples.py -n auto "$@"
