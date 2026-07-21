#!/usr/bin/env sh
set -eu

printf '\n'
if FORCE_COLOR=1 CLICOLOR_FORCE=1 PY_COLORS=1 uv run poe pre-commit; then
    status=0
else
    status=$?
fi
printf '\n'
exit "$status"
