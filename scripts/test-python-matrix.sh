#!/usr/bin/env bash
set -euo pipefail

uv run --no-sync tox run "$@"
