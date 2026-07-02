# shellcheck shell=bash

workspace_poe_color_output=0
if [[ -t 0 ]]; then
  workspace_poe_color_output=1
fi
workspace_poe_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
workspace_poe_helper="$workspace_poe_script_dir/tasks/poe"
workspace_poe_scope_args=()
workspace_poe_subcommand_args=()

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
    workspace_poe_join_args "${path_args[@]}"
  fi
}

workspace_poe_invoke_task() {
  local task="$1"
  shift
  if ((${#workspace_poe_subcommand_args[@]})); then
    "$@" poe -q "$task" "${workspace_poe_subcommand_args[@]}"
  else
    "$@" poe -q "$task"
  fi
}

workspace_poe_resolve_scopes() {
  python3 "$workspace_poe_script_dir/workspace_poe_resolve.py" "$@"
}

workspace_poe_run_uv() {
  if ((workspace_poe_color_output)); then
    env POE="$workspace_poe_helper" FORCE_COLOR=1 CLICOLOR_FORCE=1 PY_COLORS=1 uv run "$@"
  else
    env POE="$workspace_poe_helper" uv run "$@"
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
    scope_args="$(workspace_poe_join_args "${path_args[@]}")"
  fi
  cd "$package_path" || return
  if [[ "$package" == root ]]; then
    workspace_poe_run_scoped_uv "$scope_args" --all-packages poe -q "$task" "$@"
  else
    workspace_poe_run_scoped_uv "$scope_args" --package "$package" poe -q "$task" "$@"
  fi
}

workspace_poe_run_workspace_task() {
  local task="$1"
  local scope_file
  local package
  local package_path
  local paths
  local scope_args

  scope_file="$(mktemp)"
  workspace_poe_write_scopes "$task" "$scope_file"

  if workspace_poe_single_whole_scope "$scope_file"; then
    if ((${#workspace_poe_subcommand_args[@]})); then
      workspace_poe_direct_package "$scope_file" "$task" "${workspace_poe_subcommand_args[@]}"
    else
      workspace_poe_direct_package "$scope_file" "$task"
    fi
    rm -f "$scope_file"
    return
  fi

  while IFS=$'\t' read -r package package_path paths; do
    if [[ "$package" == root ]]; then
      continue
    fi
    scope_args="$(workspace_poe_join_tab_paths "$paths")"
    (
      cd "$package_path" || exit
      workspace_poe_invoke_task "$task" workspace_poe_run_scoped_uv "$scope_args" --package "$package"
    ) 2>&1 | workspace_poe_format_output "$package"
  done < "$scope_file"

  while IFS=$'\t' read -r package package_path paths; do
    if [[ "$package" != root ]]; then
      continue
    fi
    scope_args="$(workspace_poe_join_tab_paths "$paths")"
    (
      cd "$package_path" || exit
      workspace_poe_invoke_task "$task" workspace_poe_run_scoped_uv "$scope_args" --all-packages
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
