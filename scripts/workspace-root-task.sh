#!/usr/bin/env bash
set -euo pipefail

if (($# == 0)); then
  echo "usage: $0 <poe-task> [args ...]" >&2
  exit 2
fi

# shellcheck source=scripts/poe/workspace-poe.sh
. "$(dirname "${BASH_SOURCE[0]}")/poe/workspace-poe.sh"

workspace_poe_enter_tree

task="$1"
shift

workspace_poe_run_scoped_uv "" --all-packages poe "${workspace_poe_poe_args[@]}" "$task" "$@"
