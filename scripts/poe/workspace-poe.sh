# shellcheck shell=bash

set -o pipefail

workspace_poe_color_output=0

workspace_poe_supports_color() {
  if [[ -n "${NO_COLOR:-}" && "${NO_COLOR:0:1}" != 0 ]]; then
    return 1
  fi
  if [[ -n "${FORCE_COLOR:-}" && "${FORCE_COLOR:0:1}" != 0 ]]; then
    return 0
  fi
  if [[ -n "${CLICOLOR_FORCE:-}" && "${CLICOLOR_FORCE:0:1}" != 0 ]]; then
    return 0
  fi
  if [[ -n "${PY_COLORS:-}" && "${PY_COLORS:0:1}" != 0 ]]; then
    return 0
  fi
  [[ -t 1 ]]
}

if workspace_poe_supports_color; then
  workspace_poe_color_output=1
fi
workspace_poe_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
workspace_poe_helper="$workspace_poe_script_dir/tasks/poe"
workspace_poe_scope_args=()
workspace_poe_subcommand_args=()
workspace_poe_poe_args=(-q)
workspace_poe_snapshot_dir=""
workspace_poe_jobs_file=""

workspace_poe_cleanup_snapshot() {
  if [[ -n "$workspace_poe_snapshot_dir" ]]; then
    rm -rf "$workspace_poe_snapshot_dir"
    workspace_poe_snapshot_dir=""
  fi
}

workspace_poe_cleanup_jobs() {
  local pid
  local label
  local log_file
  local status_file
  if [[ -n "$workspace_poe_jobs_file" && -f "$workspace_poe_jobs_file" ]]; then
    while IFS=$'\t' read -r pid label log_file status_file; do
      rm -f "$log_file" "$status_file"
    done < "$workspace_poe_jobs_file"
    rm -f "$workspace_poe_jobs_file"
    workspace_poe_jobs_file=""
  fi
}

workspace_poe_cleanup() {
  workspace_poe_cleanup_jobs
  workspace_poe_cleanup_snapshot
}

workspace_poe_install_cleanup_trap() {
  trap workspace_poe_cleanup EXIT INT TERM
}

workspace_poe_enter_tree() {
  local mode="${WORKSPACE_POE_GIT_SCOPE:-tree}"
  local commit
  local root

  case "$mode" in
    tree|"")
      return 0
      ;;
    staged)
      ;;
    commit)
      ;;
    *)
      echo "WORKSPACE_POE_GIT_SCOPE must be 'tree', 'staged', or 'commit'" >&2
      return 2
      ;;
  esac

  if [[ "${WORKSPACE_POE_GIT_SCOPE_ACTIVE:-}" == "$mode" ]]; then
    return 0
  fi

  root="$(git rev-parse --show-toplevel)"
  workspace_poe_snapshot_dir="$(mktemp -d "${TMPDIR:-/tmp}/vercel-py-${mode}.XXXXXX")"
  case "$mode" in
    staged)
      git -C "$root" checkout-index --all --force --prefix="$workspace_poe_snapshot_dir/"
      ;;
    commit)
      commit="${WORKSPACE_POE_GIT_COMMIT:-}"
      if [[ -z "$commit" ]]; then
        commit="$(git -C "$root" rev-parse HEAD)"
      fi
      git -C "$root" archive "$commit" | tar -x -f - -C "$workspace_poe_snapshot_dir"
      export WORKSPACE_POE_GIT_COMMIT="$commit"
      ;;
  esac
  ln -s "$root/.git" "$workspace_poe_snapshot_dir/.git"
  mkdir -p "$root/.mypy_cache" "$root/.ruff_cache"
  ln -s "$root/.mypy_cache" "$workspace_poe_snapshot_dir/.mypy_cache"
  ln -s "$root/.ruff_cache" "$workspace_poe_snapshot_dir/.ruff_cache"

  export WORKSPACE_POE_PROJECT_ROOT="$root"
  export WORKSPACE_POE_GIT_SCOPE_ACTIVE="$mode"
  workspace_poe_install_cleanup_trap
  cd "$workspace_poe_snapshot_dir" || return
}

workspace_poe_parallel_enabled() {
  case "${WORKSPACE_POE_PARALLEL:-}" in
    0|false|no)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

workspace_poe_reset_jobs() {
  workspace_poe_cleanup_jobs
  workspace_poe_jobs_file="$(mktemp "${TMPDIR:-/tmp}/workspace-poe-jobs.XXXXXX")"
  workspace_poe_install_cleanup_trap
}

workspace_poe_start_job() {
  local label="$1"
  shift
  local log_file
  local status_file
  local pid

  if [[ -z "$workspace_poe_jobs_file" ]]; then
    workspace_poe_reset_jobs
  fi

  log_file="$(mktemp "${TMPDIR:-/tmp}/workspace-poe-log.XXXXXX")"
  status_file="$(mktemp "${TMPDIR:-/tmp}/workspace-poe-status.XXXXXX")"
  (
    set +e
    workspace_poe_jobs_file=""
    workspace_poe_snapshot_dir=""
    if ((workspace_poe_color_output)); then
      FORCE_COLOR=1 CLICOLOR_FORCE=1 PY_COLORS=1 "$@" > "$log_file" 2>&1
    else
      "$@" > "$log_file" 2>&1
    fi
    printf '%s\n' "$?" > "$status_file"
  ) &
  pid=$!
  printf '%s\t%s\t%s\t%s\n' "$pid" "$label" "$log_file" "$status_file" >> "$workspace_poe_jobs_file"
}

workspace_poe_wait_jobs() {
  local pid
  local label
  local log_file
  local status_file
  local status=0

  if [[ -z "$workspace_poe_jobs_file" || ! -f "$workspace_poe_jobs_file" ]]; then
    return 0
  fi

  while IFS=$'\t' read -r pid label log_file status_file; do
    if ! wait "$pid"; then
      status=1
    fi
  done < "$workspace_poe_jobs_file"
  return "$status"
}

workspace_poe_print_jobs() {
  local pid
  local label
  local log_file
  local status_file
  local status
  local failed=0

  if [[ -z "$workspace_poe_jobs_file" || ! -f "$workspace_poe_jobs_file" ]]; then
    return 0
  fi

  while IFS=$'\t' read -r pid label log_file status_file; do
    status=0
    if [[ -s "$status_file" ]]; then
      IFS= read -r status < "$status_file"
    else
      status=1
    fi
    if ((status != 0)); then
      failed=1
    fi
    workspace_poe_print_job "$label" "$status" "$log_file"
  done < "$workspace_poe_jobs_file"
  workspace_poe_cleanup_jobs
  return "$failed"
}

workspace_poe_uv_no_color() {
  env -u FORCE_COLOR -u CLICOLOR_FORCE -u PY_COLORS NO_COLOR=1 uv "$@"
}

workspace_poe_packages() {
  paste \
    <(workspace_poe_uv_no_color workspace list) \
    <(workspace_poe_uv_no_color workspace list --paths)
}

workspace_poe_package_root() {
  local package="$1"
  local package_name
  local package_path
  while IFS=$'\t' read -r package_name package_path; do
    if [[ "$package_name" == "$package" ]]; then
      printf '%s\n' "$package_path"
      return 0
    fi
  done < <(workspace_poe_packages)
  return 1
}

workspace_poe_split_args() {
  workspace_poe_scope_args=()
  workspace_poe_subcommand_args=()

  local passthrough=0
  local arg
  for arg in "$@"; do
    if ((passthrough)); then
      workspace_poe_subcommand_args+=("$arg")
      continue
    fi
    if [[ "$arg" == -- ]]; then
      passthrough=1
      continue
    fi
    if [[ "$arg" == -* ]]; then
      passthrough=1
      workspace_poe_subcommand_args+=("$arg")
      continue
    fi
    workspace_poe_scope_args+=("$arg")
  done
}

workspace_poe_write_scopes() {
  local task="$1"
  local scope_file="$2"
  WORKSPACE_POE_SCOPE_TASK="$task"
  export WORKSPACE_POE_SCOPE_TASK
  if ((${#workspace_poe_scope_args[@]})); then
    workspace_poe_resolve_scopes "${workspace_poe_scope_args[@]}" > "$scope_file"
  else
    workspace_poe_resolve_scopes > "$scope_file"
  fi
  unset WORKSPACE_POE_SCOPE_TASK
}

workspace_poe_join_tab_paths() {
  local paths="$1"
  local path_args=()
  if [[ -n "$paths" ]]; then
    IFS=$'\t' read -r -a path_args <<< "$paths"
    if ((${#path_args[@]})); then
      workspace_poe_join_args "${path_args[@]}"
    fi
  fi
}

workspace_poe_invoke_task() {
  local task="$1"
  shift
  if ((${#workspace_poe_subcommand_args[@]})); then
    "$@" poe "${workspace_poe_poe_args[@]}" "$task" "${workspace_poe_subcommand_args[@]}"
  else
    "$@" poe "${workspace_poe_poe_args[@]}" "$task"
  fi
}

workspace_poe_task_for_package() {
  local task="$1"
  local package="$2"
  if [[ "$package" == root && -n "${WORKSPACE_POE_ROOT_TASK:-}" ]]; then
    printf '%s\n' "$WORKSPACE_POE_ROOT_TASK"
  else
    printf '%s\n' "$task"
  fi
}

workspace_poe_resolve_scopes() {
  python3 "$workspace_poe_script_dir/workspace_poe_resolve.py" "$@"
}

workspace_poe_run_uv() {
  if ((workspace_poe_color_output)); then
    if [[ -n "${WORKSPACE_POE_PROJECT_ROOT:-}" ]]; then
      env POE="$workspace_poe_helper" FORCE_COLOR=1 CLICOLOR_FORCE=1 PY_COLORS=1 uv run --project "$WORKSPACE_POE_PROJECT_ROOT" --no-sync "$@"
    else
      env POE="$workspace_poe_helper" FORCE_COLOR=1 CLICOLOR_FORCE=1 PY_COLORS=1 uv run "$@"
    fi
  else
    if [[ -n "${WORKSPACE_POE_PROJECT_ROOT:-}" ]]; then
      env POE="$workspace_poe_helper" uv run --project "$WORKSPACE_POE_PROJECT_ROOT" --no-sync "$@"
    else
      env POE="$workspace_poe_helper" uv run "$@"
    fi
  fi
}

workspace_poe_run_scoped_uv() {
  local scope_args="$1"
  shift
  if [[ -n "$scope_args" ]]; then
    WORKSPACE_POE_SCOPE_ARGS="$scope_args" workspace_poe_run_uv "$@"
  else
    workspace_poe_run_uv "$@"
  fi
}

workspace_poe_run_color_if_supported() {
  if ((workspace_poe_color_output)); then
    FORCE_COLOR="${FORCE_COLOR:-1}" CLICOLOR_FORCE="${CLICOLOR_FORCE:-1}" PY_COLORS="${PY_COLORS:-1}" "$@"
  else
    "$@"
  fi
}

workspace_poe_join_args() {
  if (($#)); then
    printf '%q ' "$@"
  fi
}

workspace_poe_single_whole_scope() {
  local scope_file="$1"
  local package
  local package_path
  local paths
  local count=0

  while IFS=$'\t' read -r package package_path paths; do
    count=$((count + 1))
    if [[ -n "$paths" ]]; then
      return 1
    fi
  done < "$scope_file"

  [[ "$count" -eq 1 ]]
}

workspace_poe_direct_package() {
  local scope_file="$1"
  local task="$2"
  shift 2
  local package
  local package_path
  local paths
  local scope_args=""

  IFS=$'\t' read -r package package_path paths < "$scope_file"
  if [[ -n "$paths" ]]; then
    IFS=$'\t' read -r -a path_args <<< "$paths"
    if ((${#path_args[@]})); then
      scope_args="$(workspace_poe_join_args "${path_args[@]}")"
    fi
  fi
  cd "$package_path" || return
  export WORKSPACE_POE_PACKAGE="$package"
  if [[ "$package" == root ]]; then
    if (($#)); then
      workspace_poe_run_scoped_uv "$scope_args" --all-packages poe "${workspace_poe_poe_args[@]}" "$task" "$@"
    else
      workspace_poe_run_scoped_uv "$scope_args" --all-packages poe "${workspace_poe_poe_args[@]}" "$task"
    fi
  else
    if (($#)); then
      workspace_poe_run_scoped_uv "$scope_args" --package "$package" poe "${workspace_poe_poe_args[@]}" "$task" "$@"
    else
      workspace_poe_run_scoped_uv "$scope_args" --package "$package" poe "${workspace_poe_poe_args[@]}" "$task"
    fi
  fi
}

workspace_poe_run_resolved_task() {
  local package_path="$1"
  local scope_args="$2"
  local package_task="$3"
  local uv_scope="$4"
  local package="$5"

  cd "$package_path" || return
  export WORKSPACE_POE_PACKAGE="$package"
  if [[ "$uv_scope" == --all-packages ]]; then
    workspace_poe_invoke_task "$package_task" workspace_poe_run_scoped_uv "$scope_args" --all-packages
  else
    workspace_poe_invoke_task "$package_task" workspace_poe_run_scoped_uv "$scope_args" --package "$package"
  fi
}

workspace_poe_run_workspace_task() {
  local task="$1"
  local scope_file
  local package
  local package_path
  local paths
  local scope_args
  local package_task

  scope_file="$(mktemp)"
  workspace_poe_write_scopes "$task" "$scope_file"

  if workspace_poe_single_whole_scope "$scope_file"; then
    IFS=$'\t' read -r package package_path paths < "$scope_file"
    package_task="$(workspace_poe_task_for_package "$task" "$package")"
    if ((${#workspace_poe_subcommand_args[@]})); then
      workspace_poe_direct_package "$scope_file" "$package_task" "${workspace_poe_subcommand_args[@]}"
    else
      workspace_poe_direct_package "$scope_file" "$package_task"
    fi
    rm -f "$scope_file"
    return
  fi

  if workspace_poe_parallel_enabled; then
    workspace_poe_reset_jobs
    while IFS=$'\t' read -r package package_path paths; do
      scope_args="$(workspace_poe_join_tab_paths "$paths")"
      package_task="$(workspace_poe_task_for_package "$task" "$package")"
      if [[ "$package" == root ]]; then
        workspace_poe_start_job root workspace_poe_run_resolved_task "$package_path" "$scope_args" "$package_task" --all-packages root
      else
        workspace_poe_start_job "$package" workspace_poe_run_resolved_task "$package_path" "$scope_args" "$package_task" --package "$package"
      fi
    done < "$scope_file"
    rm -f "$scope_file"
    workspace_poe_wait_jobs || true
    workspace_poe_print_jobs
    return
  fi

  while IFS=$'\t' read -r package package_path paths; do
    if [[ "$package" == root ]]; then
      continue
    fi
    scope_args="$(workspace_poe_join_tab_paths "$paths")"
    package_task="$(workspace_poe_task_for_package "$task" "$package")"
    (
      cd "$package_path" || exit
      export WORKSPACE_POE_PACKAGE="$package"
      workspace_poe_invoke_task "$package_task" workspace_poe_run_scoped_uv "$scope_args" --package "$package"
    ) 2>&1 | workspace_poe_format_output "$package"
  done < "$scope_file"

  while IFS=$'\t' read -r package package_path paths; do
    if [[ "$package" != root ]]; then
      continue
    fi
    scope_args="$(workspace_poe_join_tab_paths "$paths")"
    package_task="$(workspace_poe_task_for_package "$task" "$package")"
    (
      cd "$package_path" || exit
      export WORKSPACE_POE_PACKAGE="$package"
      workspace_poe_invoke_task "$package_task" workspace_poe_run_scoped_uv "$scope_args" --all-packages
    ) 2>&1 | workspace_poe_format_output root
  done < "$scope_file"
  rm -f "$scope_file"
}

workspace_poe_label_color() {
  local label="$1"
  local colors=(31 32 33 34 35 36 91 92 93 94 95 96)
  local hash
  hash="$(printf '%s' "$label" | cksum)"
  hash="${hash%% *}"
  printf '%s' "${colors[hash % ${#colors[@]}]}"
}

workspace_poe_format_output() {
  local package_label="$1"
  local package_color
  if ((workspace_poe_color_output)); then
    package_color="$(workspace_poe_label_color "$1")"
    package_label="$(printf '\033[1;%sm%s\033[0m' "$package_color" "$1")"
  fi
  sed -e "s/^Poe => /${package_label}: /" -e "t" -e "s/^/${package_label}: /"
}

workspace_poe_print_job() {
  local label="$1"
  local status="$2"
  local log_file="$3"
  local label_color
  local display_label="$label"

  if ((workspace_poe_color_output)); then
    label_color="$(workspace_poe_label_color "$label")"
    display_label="$(printf '\033[1;%sm%s\033[0m' "$label_color" "$label")"
  fi

  printf '==> %s\n' "$display_label"
  if [[ -s "$log_file" ]]; then
    cat "$log_file"
    case "$(tail -c 1 "$log_file")" in
      $'\n'|"")
        ;;
      *)
        printf '\n'
        ;;
    esac
  fi
  if ((status != 0)); then
    printf '<== %s failed with exit code %s\n' "$display_label" "$status"
  fi
}
