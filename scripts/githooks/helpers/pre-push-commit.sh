#!/usr/bin/env sh

if [ "${WORKSPACE_POE_GIT_SCOPE:-}" != commit ]; then
  return 0
fi

set_pre_push_base() {
  if [ -n "${WORKSPACE_POE_GIT_BASE:-}" ] || [ -z "${1:-}" ]; then
    return 0
  fi

  configured_base=$(git config --get "branch.$1.gh-merge-base" 2>/dev/null || true)
  if [ -z "$configured_base" ]; then
    return 0
  fi

  candidate="origin/$configured_base"
  if git rev-parse --verify --quiet "$candidate^{commit}" >/dev/null; then
    WORKSPACE_POE_GIT_BASE=$candidate
    export WORKSPACE_POE_GIT_BASE
  else
    printf 'Configured pre-push base %s is unavailable; falling back to the default base.\n' "$candidate" >&2
  fi
}

if [ -n "${WORKSPACE_POE_GIT_COMMIT:-}" ]; then
  current_branch=$(git symbolic-ref --quiet --short HEAD 2>/dev/null || true)
  set_pre_push_base "$current_branch"
  return 0
fi

zero_sha=0000000000000000000000000000000000000000
if [ ! -t 0 ]; then
  while read -r local_ref local_sha remote_ref remote_sha; do
    if [ -n "$local_ref" ] && [ "$local_sha" != "$zero_sha" ]; then
      WORKSPACE_POE_GIT_COMMIT=$local_sha
      export WORKSPACE_POE_GIT_COMMIT
      case "$local_ref" in
        refs/heads/*) set_pre_push_base "${local_ref#refs/heads/}" ;;
      esac
      return 0
    fi
  done || true
fi

WORKSPACE_POE_GIT_COMMIT=$(git rev-parse HEAD)
export WORKSPACE_POE_GIT_COMMIT
current_branch=$(git symbolic-ref --quiet --short HEAD 2>/dev/null || true)
set_pre_push_base "$current_branch"
