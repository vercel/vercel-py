# Contributing

## Setup

Install dependencies and local Git hooks:

```sh
uv sync
uv run poe setup
```

`uv run poe setup` runs `uv run poe sync-githooks`, which registers repo-local
Git hook config for scripts named `scripts/githooks/<event>.<name>.<ext>`. Each
hook is installed as `hook.<event>-<name>` and marked as managed by this repo;
unmarked user hook config is not overwritten. Pre-commit hooks are installed
with `WORKSPACE_POE_GIT_SCOPE=staged` so they run against the staged index, and
pre-push hooks are installed with `WORKSPACE_POE_GIT_SCOPE=commit` so they run
against the commit tree being pushed instead of unrelated dirty worktree
changes.

The `pre-commit.checks` hook runs `uv run poe pre-commit`, which runs lint and
typecheck in parallel with buffered output. The `pre-push.checks` hook runs
`uv run poe pre-push`, which runs news-fragment, lint, typecheck, and test
checks in parallel with buffered output. The news-fragment check requires
changed package code to have a news fragment. Git hooks are local clone state,
so each checkout needs this once.

## Development Commands

Run the standard checks before opening a PR:

```sh
uv run poe qa
```

`qa` runs lint, typecheck, and tests. It accepts package and file scopes, plus
`-q`/`--quiet` and `-v`/`--verbose`; because it is an aggregate task, it does
not accept tool-specific arguments after `--`. It is fail-fast, so later checks
do not run after an earlier check fails.

The individual checks are also available as top-level Poe tasks:

```sh
uv run poe lint
uv run poe typecheck
uv run poe test
```

For focused work, pass scopes before `--` and tool arguments after `--`:

```sh
uv run poe qa tests/unit/test_release_system.py
uv run poe test tests/unit/test_time.py -- -k coerce_duration
uv run poe test vercel-oidc
uv run poe typecheck vercel-queue
```

Auto-fix formatting and simple lint issues with:

```sh
uv run poe fix
```

The workspace task system is documented in `scripts/poe/README.md`.

## News Fragments

User-visible package changes need a news fragment:

```text
changes/<package>/<id>.<type>.md
```

Valid types are `breaking`, `feature`, `bugfix`, `docs`, and `internal`.
News fragment content should be concise changelog text; the release script adds
the leading bullet marker when needed.

Example:

```text
changes/vercel-cache/123.bugfix.md
```

Use `uv run poe lint-towncrier` to validate existing news fragments. The Git
`pre-push.news-fragments` hook enforces that changed package code has a
news fragment, but it does not replace the full lint/test/typecheck suite.

Release PR mechanics, version bump rules, dependency cascades, and publishing
are documented in `RELEASING.md`.

## Package Builds

Build all workspace packages in dependency order with:

```sh
uv run poe build-packages
```

For a focused package build, use `uv build` directly:

```sh
uv build --package vercel-cache --no-sources
```

`--no-sources` is important for publishability checks because it verifies the
wheel metadata without local workspace source overrides.

## Commit Messages

Keep commit messages short and specific. Use a title line of 50 characters or
fewer, wrap body lines at 72 characters, and explain what changed and why. Do
not include `Co-authored-by:` lines.
