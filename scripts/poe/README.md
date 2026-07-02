# Workspace Poe Tasks

This directory contains the shared task system for workspace package checks. It
keeps package `pyproject.toml` files small while still letting each package own
its local configuration differences.

## Layout

- `poe.toml` is the shared Poe include. Workspace members include this file from
  `[tool.poe]`.
- `tasks/` contains executable wrappers for common tools. The wrappers print the
  concrete command and append Poe extra args consistently.
- `workspace-poe.sh` contains the workspace runner used by top-level scripts.
- `workspace_poe_resolve.py` attributes package names and paths to workspace
  packages for scoped runs.

The top-level `scripts/fix.sh`, `scripts/lint.sh`, `scripts/test.sh`, and
`scripts/typecheck.sh` are symlinks to `scripts/workspace-task.sh`. The symlink
name selects the Poe task to run.

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

- `lint`: runs `$RUFF_CHECK`, then `$RUFF_FORMAT`.
- `fix`: runs `$RUFF_CHECK_FIX`, then `$RUFF_FORMAT_FIX`.
- `typecheck`: runs `$POE typecheck-mypy`, then `$POE typecheck-ty`.
- `typecheck-mypy`: runs `$MYPY`.
- `typecheck-ty`: runs `$TY`.
- `test`: runs `$PYTEST`.

Most packages should not redefine these tasks. Prefer tool configuration in
`pyproject.toml` and inherit the shared tasks.

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
portable regardless of current working directory.

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

Scopes can be workspace package names, `root`, or paths. Path scopes are mapped
to owning packages and rewritten relative to the package task working directory.

When a run targets exactly one whole package, the runner executes that package
directly with no output prefixing. Multi-package and path-scoped runs prefix each
output line with the package name. Package colors are enabled when stdin is a TTY
and selected by a stable hash.

Root runs execute after package runs. Root tasks use `uv run --all-packages` so
workspace packages remain importable for root tests.

## Maintenance

When changing this system, verify all shell code with system's default bash
(helps catching new bash-isms on macOS).

```sh
shellcheck -x scripts/workspace-task.sh scripts/poe/workspace-poe.sh scripts/build.sh scripts/fix.sh scripts/test-examples.sh scripts/poe/tasks/poe scripts/poe/tasks/tool
/bin/bash -n scripts/workspace-task.sh scripts/poe/workspace-poe.sh scripts/build.sh scripts/fix.sh scripts/test-examples.sh scripts/poe/tasks/poe scripts/poe/tasks/tool
python3 -m py_compile scripts/poe/workspace_poe_resolve.py
```

Run at least one symlinked runner through `/bin/bash`:

```sh
/bin/bash scripts/lint.sh src/vercel-oidc/vercel/oidc/__init__.py
/bin/bash scripts/typecheck.sh vercel-headers
```

Avoid Bash 4-only features in shell files. In particular, do not use associative
arrays, `mapfile`, or `readarray`. Put structured workspace logic in
`workspace_poe_resolve.py` instead.
