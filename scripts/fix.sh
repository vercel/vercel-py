#!/usr/bin/env bash
set -euo pipefail

uv run ruff check --fix src tests examples
uv run ruff format src tests examples
