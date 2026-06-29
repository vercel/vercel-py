#!/usr/bin/env bash
set -euo pipefail

uv run --all-packages pytest -v --capture=tee-sys tests/test_examples.py -n auto "$@"
