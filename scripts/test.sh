#!/usr/bin/env bash
set -euo pipefail

uv run pytest -v --capture=tee-sys --ignore=tests/test_examples.py "$@"
