#!/usr/bin/env sh
set -eu

. "$(dirname "$0")/helpers/pre-push-commit.sh"

printf '\n'
if FORCE_COLOR=1 CLICOLOR_FORCE=1 PY_COLORS=1 POE_VERBOSITY=-1 uv run poe pre-push; then
    status=0
else
    status=$?
fi
printf '\n'
exit "$status"
