#!/usr/bin/env sh

run_quietly() {
  run_quietly_name=$1
  shift

  run_quietly_log=$(mktemp "${TMPDIR:-/tmp}/vercel-py-hook.XXXXXX")
  trap 'rm -f "$run_quietly_log"' EXIT HUP INT TERM

  if FORCE_COLOR=1 CLICOLOR_FORCE=1 PY_COLORS=1 "$@" >"$run_quietly_log" 2>&1; then
    rm -f "$run_quietly_log"
    trap - EXIT HUP INT TERM
    return 0
  else
    run_quietly_status=$?
  fi

  printf '%s failed with exit status %s; output follows:\n' \
    "$run_quietly_name" "$run_quietly_status" >&2
  cat "$run_quietly_log" >&2
  rm -f "$run_quietly_log"
  trap - EXIT HUP INT TERM
  return "$run_quietly_status"
}
