#!/usr/bin/env sh

if [ "${WORKSPACE_POE_GIT_SCOPE:-}" != commit ] || [ -n "${WORKSPACE_POE_GIT_COMMIT:-}" ]; then
  return 0
fi

zero_sha=0000000000000000000000000000000000000000
if [ ! -t 0 ]; then
  while read -r local_ref local_sha remote_ref remote_sha; do
    if [ -n "$local_ref" ] && [ "$local_sha" != "$zero_sha" ]; then
      WORKSPACE_POE_GIT_COMMIT=$local_sha
      export WORKSPACE_POE_GIT_COMMIT
      return 0
    fi
  done || true
fi

WORKSPACE_POE_GIT_COMMIT=$(git rev-parse HEAD)
export WORKSPACE_POE_GIT_COMMIT
