# Workspace Poe Tasks

This directory contains the shared task system for workspace package checks. It
keeps package `pyproject.toml` files small while still letting each package own
its local configuration differences.

## Layout

- `poe.toml` is the shared Poe include. Workspace members include this file from
  `[tool.poe]`.
- `tasks/` contains executable wrappers for common tools. The wrappers print the
  concrete command and append Poe extra args consistently.
- `workspace_poe.py` contains the Python workspace runner used by top-level
  scripts and Poe tasks. It uses lograil for concurrent process dashboards and
  plain-mode output capture.
- `workspace_poe_resolve.py` attributes package names and paths to workspace
  packages for scoped runs.

The top-level `scripts/fix.sh`, `scripts/lint.sh`, `scripts/test.sh`, and
`scripts/typecheck.sh` are symlinks to `scripts/workspace-task.sh`. The symlink
name selects the Poe task to run, and the script delegates to the Python runner.
The root `pyproject.toml` also exposes top-level Poe commands for `lint`,
`typecheck`, `test`, and `qa`.

## Package Setup

Every workspace package that wants the default tasks should include the shared
Poe config:

```toml
[tool.poe]
include = "../../scripts/poe/poe.toml"
verbosity = -1
```

Use the relative path appropriate for the package. The root package uses:

```toml
[tool.poe]
include = "scripts/poe/poe.toml"
verbosity = -1
```

`verbosity = -1` must stay in the primary package config. Poe does not apply
`verbosity` from included configs.

## Default Tasks

The shared include defines these Poe tasks:

- `lint`: runs `$RUFF_CHECK` and `$RUFF_FORMAT` in parallel.
- `fix`: runs `$RUFF_CHECK_FIX`, then `$RUFF_FORMAT_FIX`.
- `typecheck`: runs `$POE typecheck-mypy` and `$POE typecheck-ty` in parallel.
- `typecheck-mypy`: runs `$MYPY`.
- `typecheck-ty`: runs `$TY`.
- `test`: runs `$PYTEST` with pytest-xdist `-n auto` by default.

Most packages should not redefine these tasks. Prefer tool configuration in
`pyproject.toml` and inherit the shared tasks.

Set `WORKSPACE_POE_PARALLEL=0` to run workspace and shared package checks
sequentially. `false` and `no` are accepted as equivalent opt-outs. This also
disables the default pytest-xdist worker flag.

## Tool Wrappers

`poe.toml` exposes these environment variables:

- `POE`: nested Poe task runner, `tasks/poe`.
- `PYTEST`: pytest wrapper, `tasks/pytest`.
- `RUFF_CHECK`: ruff check wrapper, `tasks/ruff-check`.
- `RUFF_CHECK_FIX`: ruff check --fix wrapper, `tasks/ruff-check-fix`.
- `RUFF_FORMAT`: ruff format check wrapper, `tasks/ruff-format`.
- `RUFF_FORMAT_FIX`: ruff format wrapper, `tasks/ruff-format-fix`.
- `MYPY`: mypy wrapper, `tasks/mypy`.
- `TY`: ty wrapper, `tasks/ty`.

The wrappers default to the current workspace scope:

- explicit wrapper args, if present;
- otherwise `WORKSPACE_POE_SCOPE_ARGS`, when set by a top-level runner;
- otherwise `tests examples` for Ruff wrappers at the workspace root, or `.`.

The `mypy` wrapper also adds `--config-file <workspace-root>/pyproject.toml`
unless the caller provides a config file. This keeps package mypy commands
portable regardless of current working directory. Unless the caller provides
`--cache-dir`, the wrapper uses `.mypy_cache/<package-name>` for workspace
package checks and `.mypy_cache/root` for root checks.

The `pytest` wrapper adds `-n auto` unless the caller provides `-n` or
`--numprocesses`, or disables parallel mode with `WORKSPACE_POE_PARALLEL=0`.

## Local Overrides

Only override tasks for real package differences.

Examples:

```toml
[tool.poe.tasks.typecheck-mypy]
cmd = "$MYPY --python-version 3.12"
```

```toml
[tool.poe.tasks.test]
cmd = "python -c \"pass\""
```

For pytest defaults, prefer package-local pytest configuration instead of command
arguments:

```toml
[tool.pytest.ini_options]
addopts = "--no-header --capture=tee-sys"
asyncio_mode = "auto"
testpaths = ["tests"]
```

For ruff and ty defaults, prefer their normal `pyproject.toml` configuration.
For mypy, prefer the shared root `pyproject.toml` unless a package genuinely
needs a local override.

## Top-Level Runners

The symlinked runners accept zero or more scopes before `--`, and tool args after
`--`:

```sh
./scripts/lint.sh
./scripts/fix.sh vercel-oidc
./scripts/typecheck.sh vercel-oidc
./scripts/test.sh tests/unit/test_time.py -- -k coerce_duration
```

The equivalent Poe commands are available at the workspace root:

```sh
uv run poe lint vercel-oidc
uv run poe typecheck vercel-oidc
uv run poe test tests/unit/test_time.py -- -k coerce_duration
uv run poe qa tests/unit/test_time.py
```

`qa` runs lint, typecheck, and test for the selected scopes. It accepts only
scope arguments and `-q`/`--quiet` or `-v`/`--verbose`; it intentionally rejects
tool-specific passthrough after `--`.

Scopes can be workspace package names, `root`, or paths. Path scopes are mapped
to owning packages and rewritten relative to the package task working directory.

When a run targets exactly one whole package, the runner executes that package
directly. Multi-package and path-scoped runs are rendered by lograil.
Interactive TTYs get the grouped dashboard; CI, pipes, and
`LOGRAIL_OUTPUT=plain` get timestamped plain output with process labels.

Root runs execute after package runs. Root tasks use `uv run --all-packages` so
workspace packages remain importable for root tests. At the workspace root,
public Poe tasks such as `test` dispatch back through the top-level runner, so
root-scope execution uses internal `test-root`, `lint-root`, and
`typecheck-root` task names to avoid recursion.

Set `WORKSPACE_POE_GIT_SCOPE=staged` to run a workspace task against a temporary
snapshot of the staged Git index instead of the current working tree:

```sh
WORKSPACE_POE_GIT_SCOPE=staged uv run poe check-news-fragments
WORKSPACE_POE_GIT_SCOPE=staged uv run poe lint tests/unit/test_release_system.py
```

Staged mode materializes `git checkout-index --all` into a temporary directory,
links that snapshot back to the real `.git` directory, and runs the normal
workspace task machinery from the snapshot while using the real project for
`uv run`. This keeps pre-commit checks focused on staged files and avoids
unrelated dirty worktree changes influencing hook results.

The managed `pre-commit.checks` hook invokes `uv run poe pre-commit`, which runs
lint and typecheck concurrently through the Python/lograil runner.

Set `WORKSPACE_POE_GIT_SCOPE=commit` to run a workspace task against a commit
tree instead of the current working tree:

```sh
WORKSPACE_POE_GIT_SCOPE=commit uv run poe lint tests/unit/test_release_system.py
```

Commit mode materializes `git archive` for `WORKSPACE_POE_GIT_COMMIT`, or
`HEAD` when that variable is unset, into a temporary directory. Managed pre-push
hooks use this mode and set `WORKSPACE_POE_GIT_COMMIT` from Git's pre-push
input so checks run against the commit tree being pushed.

The managed `pre-push.checks` hook invokes `uv run poe pre-push`, which runs
news-fragment, lint, typecheck, and test checks concurrently through the
Python/lograil runner.

## Maintenance

When changing this system, verify all shell code with system's default bash
(helps catching new bash-isms on macOS).

```sh
shellcheck -x scripts/build.sh scripts/test-examples.sh scripts/poe/tasks/poe scripts/poe/tasks/tool
/bin/bash -n scripts/build.sh scripts/test-examples.sh scripts/poe/tasks/poe scripts/poe/tasks/tool
python3 -m py_compile scripts/poe/workspace_poe.py scripts/poe/workspace_poe_resolve.py scripts/workspace-task.sh scripts/qa.sh scripts/workspace-root-task.sh
```

Run at least one symlinked runner directly:

```sh
./scripts/lint.sh src/vercel-oidc/vercel/oidc/__init__.py
./scripts/typecheck.sh vercel-headers
```

Avoid new shell orchestration. Put structured workspace logic in
`workspace_poe.py` or `workspace_poe_resolve.py` instead.
