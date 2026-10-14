# Tooling

Repository tooling for checks, changelogs, releases, and publishing, all driven from the workspace root through Poe tasks.

Contributor commands are listed in [[CONTRIBUTING.md]]; this page explains the design behind them.

## Workspace tasks

Lint, typecheck, and test tasks are defined once and inherited by every package, then fanned out by a Python runner.

Packages include the shared [[scripts/poe/poe.toml]] and only override tasks for genuine differences. The runner in [[scripts/poe/workspace_poe.py]] maps scopes — package names, `root`, or paths — to owning packages and runs them in parallel; the full design is in [[scripts/poe/README.md]].

Example tests are opt-in: a package declares its own `test-examples` task and owns discovery, credentials, setup, and cleanup, so live examples skip safely when secrets are missing.

### Git hooks

Repo-managed hooks run the same checks against exactly what is being committed or pushed, not the dirty worktree.

`uv run poe setup` installs hooks from `scripts/githooks/`. Pre-commit runs against a snapshot of the staged index and pre-push against the pushed commit tree, selected with `WORKSPACE_POE_GIT_SCOPE`. CI uses the pull request's base commit and remains authoritative.

## Releases

Packages are released independently, with version bumps derived from news fragments and workspace dependency cascades.

Release mechanics are documented in [[RELEASING.md]]. Package discovery and ordering come from uv workspace metadata via [[scripts/workspace.py#packages]] and [[scripts/workspace.py#topological_names]], never from a hard-coded package list.

### News fragments

Every user-visible package change carries a fragment at `changes/<package>/<id>.<type>.md`, which becomes exactly one changelog entry.

Types map to bumps: `breaking` is major (minor while a package is `0.x`), `feature` is minor, and `bugfix`, `docs`, and `internal` are patch; the largest bump per package wins. [[scripts/release.py#compute_releases]] adds dependency-only patch releases for dependents whose workspace lower bounds must move.

Packages at the unpublished `0.0.0` sentinel are exempt from enforcement and cascades; their first fragment promotes them to `0.1.0`. CI enforces fragments with `uv run poe check-news-fragments`.

### Dynamic dependencies

Published dependency lists are generated at build time so workspace lower bounds always match the sibling versions being released.

Packages declare dependencies under `[tool.vercel.release.dependencies]` and mark `project.dependencies` dynamic. [[scripts/hatch_build.py#WorkspaceDependenciesMetadataHook]] rewrites workspace requirements to the current sibling versions while preserving markers, extras, and upper bounds. Local development resolves the same packages through `[tool.uv.sources]` workspace links.

The workspace root itself is not buildable: [[scripts/no_root_build.py]] is its build backend and always fails.

### Publishing

Merged release PRs are published by GitHub Actions through PyPI trusted publishing; maintainers never upload from their machines.

[[scripts/publish.py]] has `detect`, `build`, and `publish` stages that communicate through a JSON plan. Every package builds and passes installed-wheel checks before any upload. Release PRs carry the `release` label, which makes CI run the same stages with `--dry-run`.

Most packages also publish a `-bundle` variant with their third-party dependencies vendored, assembled by [[scripts/bundle_release.py#build_bundle_package]]. The common HTTP stack is vendored once into a shared `vercel-internal-shared-vendored-deps` distribution so bundles do not each carry a copy. AnyIO and the integrated frameworks stay ordinary peer dependencies because they share state with application code.

## Knowledge graph checks

This `lat.md/` graph is validated in CI so links into source code and between sections cannot silently rot.

The `lat.md check` job in [[.github/workflows/ci.yml]] runs Lat's checker on every push and pull request, and **CI Overall** requires it to pass. Locally, run `lat check` after changing documented code or these files; agent-facing workflow instructions live in the generated section of [[AGENTS.md]].
