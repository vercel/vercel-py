#!/bin/sh
# Run the Turborepo version that CI uses, with the workspace environment first
# on PATH. Tasks in turbo.json call `poe` directly: Turborepo runs commands that
# start with `uv` one at a time.
root=$(cd "$(dirname "$0")/.." && pwd)
PATH="$root/.venv/bin:$root/.venv/Scripts:$PATH"
export PATH
exec npx --yes turbo@2.11.11 "$@"
