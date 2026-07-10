#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/poe/workspace-poe.sh
. "$(dirname "${BASH_SOURCE[0]}")/poe/workspace-poe.sh"

workspace_poe_enter_tree

qa_scopes=()
qa_verbose=0
qa_quiet=0

if [[ -n "${POE_EXTRA_ARGS:-}" ]]; then
  echo "qa does not accept tool-specific arguments after --" >&2
  exit 2
fi

qa_usage() {
  cat <<'EOF'
Usage: ./scripts/qa.sh [-q|--quiet] [-v|--verbose] [scope ...]

Runs lint, typecheck, and test for the selected workspace scopes.
EOF
}

while (($#)); do
  case "$1" in
    -h|--help)
      qa_usage
      exit 0
      ;;
    -q|--quiet)
      qa_quiet=$((qa_quiet + 1))
      shift
      ;;
    -v|--verbose)
      qa_verbose=$((qa_verbose + 1))
      shift
      ;;
    --)
      echo "qa does not accept tool-specific arguments after --" >&2
      exit 2
      ;;
    -*)
      echo "qa only accepts -q/--quiet and -v/--verbose options" >&2
      exit 2
      ;;
    *)
      qa_scopes+=("$1")
      shift
      ;;
  esac
done

qa_poe_flags=()
while ((qa_quiet > 0)); do
  qa_poe_flags+=(-q)
  qa_quiet=$((qa_quiet - 1))
done
while ((qa_verbose > 0)); do
  qa_poe_flags+=(-v)
  qa_verbose=$((qa_verbose - 1))
done

qa_run() {
  local task="$1"
  printf '==> %s\n' "$task"
  workspace_poe_subcommand_args=()
  workspace_poe_poe_args=(-q)
  if ((${#qa_poe_flags[@]})); then
    workspace_poe_poe_args+=("${qa_poe_flags[@]}")
  fi
  POE_EXTRA_ARGS= WORKSPACE_POE_ROOT_TASK="${task}-root" \
    workspace_poe_run_workspace_task "$task"
}

workspace_poe_scope_args=()
if ((${#qa_scopes[@]})); then
  workspace_poe_scope_args+=("${qa_scopes[@]}")
fi
qa_run lint
qa_run typecheck
qa_run test
