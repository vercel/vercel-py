#!/usr/bin/env sh
set -eu

. "$(dirname "$0")/helpers/pre-push-commit.sh"
. "$(dirname "$0")/helpers/run-quietly.sh"

run_quietly "pre-push checks" uv run poe pre-push
