#!/usr/bin/env bash
set -euo pipefail

uv run --all-packages pytest -v --capture=tee-sys --ignore=tests/test_examples.py "$@"
