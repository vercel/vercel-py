#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/poe/workspace-poe.sh
. "$(dirname "${BASH_SOURCE[0]}")/poe/workspace-poe.sh"

workspace_poe_split_args "$@"
workspace_poe_run_workspace_task "$(basename "${BASH_SOURCE[0]}" .sh)"
