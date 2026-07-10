#!/usr/bin/env sh
set -eu

. "$(dirname "$0")/helpers/run-quietly.sh"

run_quietly "pre-commit checks" uv run poe pre-commit
