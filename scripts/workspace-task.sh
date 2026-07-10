#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/poe/workspace-poe.sh
. "$(dirname "${BASH_SOURCE[0]}")/poe/workspace-poe.sh"

workspace_poe_enter_tree
workspace_poe_split_args "$@"
workspace_poe_task="$(basename "${BASH_SOURCE[0]}" .sh)"
case "$workspace_poe_task" in
  # These root Poe tasks are public aliases back to this runner. Root scopes must
  # use the matching internal *-root task to avoid recursing indefinitely.
  fix|lint|test|typecheck)
    WORKSPACE_POE_ROOT_TASK="${workspace_poe_task}-root" workspace_poe_run_workspace_task "$workspace_poe_task"
    ;;
  *)
    workspace_poe_run_workspace_task "$workspace_poe_task"
    ;;
esac
