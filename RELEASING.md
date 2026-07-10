# Releasing

This repository releases workspace packages independently. A release may include
one package or several packages, depending on handwritten news fragments and
workspace dependency cascades.

## Package Order

Package discovery and release/build ordering come from `uv` workspace metadata,
not from a hard-coded package list.

Useful commands:

```sh
python scripts/workspace.py list --names --topological
python scripts/workspace.py version vercel-cache
python scripts/workspace.py version-file vercel-cache
```

## News Fragments

User-visible changes need a news fragment under:

```text
changes/<package>/<id>.<type>.md
```

Valid types are:

- `breaking` for major changes, or minor changes while the package is `0.x`
- `feature` for minor changes
- `bugfix` for patch fixes
- `docs` for patch documentation changes
- `internal` for patch internal changes

Example:

```text
changes/vercel-cache/123.bugfix.md
```

News fragment content should be one or more concise changelog bullets. The
release script adds `- ` when a line does not already start with a bullet.

Dependency-only cascade releases do not need handwritten news fragments. The
release script writes `Update dependencies.` for those changelog entries.

## Release Prep

Run:

```sh
uv run poe release-status
uv run poe release
```

`release-status` prints the pending release set without editing files.

`release` does all release PR file updates:

- requires a clean Git working tree
- creates a release branch named `<gh-username>/release-<timestamp>`
- finds packages with news fragments
- computes the semver bump for each package
- walks reverse workspace dependencies
- adds dependency-only patch releases for dependents whose workspace dependency
  lower bounds need to move
- updates package `version.py` files
- prepends per-package `CHANGELOG.md` entries
- removes consumed news fragments
- runs `uv lock`
- stages the release diff
- opens `git commit -v` with a prepopulated `Release Packages` commit message
- pushes the current branch
- opens a pull request with `gh pr create` non-interactively

News-fragment-backed changelog bullets include PR numbers when Git history for
the news fragment contains GitHub squash subjects like
`pkg: fix cache cleanup (#123)` or merge subjects like
`Merge pull request #123 from user/branch`.
Dependency-only cascade entries still render as `Update dependencies.` without a
PR suffix.

Review the staged verbose diff in the commit editor before saving the release
commit. After a successful commit, the script pushes the current branch and
opens the release PR.

## Version Bumps

Initial bump mapping:

- `breaking` -> `major`
- `feature` -> `minor`
- `bugfix`, `docs`, `internal` -> `patch`

For `0.x` packages, `breaking` bumps minor instead of major.

If a package has multiple news fragments, the largest bump wins. If a package is
also pulled in by a dependency cascade, its handwritten news fragment bump still
wins over the dependency-only patch bump.

## Dynamic Dependencies

Package `pyproject.toml` files keep publishable dependency declarations in:

```toml
[tool.vercel.release.dependencies]
dependencies = [
    "vercel-headers>=0.6.0",
]
```

`project.dependencies` is dynamic. The Hatch metadata hook rewrites workspace
dependencies at build time to use the current sibling package versions, while
preserving markers, extras, and upper bounds. Third-party dependencies pass
through unchanged.

Local development still uses `[tool.uv.sources]` workspace links.

There is a tiny `hatch_build.py` loader in each package root because Hatch's
custom metadata hook path is resolved relative to the package both during local
builds and during wheel builds from sdists. The shared implementation lives in
`scripts/hatch_build.py`, and sdists include it as `_vercel_hatch_build.py` so
sdist rebuilds are self-contained.

## Publishing

Publishing is done by GitHub Actions through PyPI trusted publishing. Do not
publish packages from a maintainer machine.

When the release PR is merged to `main`, `.github/workflows/publish.yml`:

- detects packages whose version file or changelog changed in the merge commit
- publishes changed packages in dependency order
- builds each package with `uv build --package <name> --no-sources` into an
  isolated workflow directory
- publishes only that package's artifacts with `uv publish` and OIDC trusted
  publishing
- creates and pushes `<package>-v<version>` tags after successful publish
- creates matching GitHub releases

The workflow skips the PyPI upload when the exact version already exists, but
still creates the tag and GitHub release idempotently. This lets a failed tag or
release step be retried after a successful upload.

## Validation

Before merging a release PR, run at least:

```sh
uv run poe lint-towncrier
uv run pytest tests/unit/test_release_system.py
uv build --package vercel-cache --no-sources
```

For broader confidence, run:

```sh
./scripts/lint.sh
./scripts/typecheck.sh
./scripts/test.sh
```

`lint-towncrier` validates news fragment package names, news fragment filename
types, and non-empty news fragment content. The Git `pre-push-news-fragments`
hook generated from `scripts/githooks/pre-push.news-fragments.sh` runs `uv run
poe check-news-fragments` to require news fragments for changed package code
without maintaining a hard-coded package registry. `sync-githooks` installs
pre-push hooks with `WORKSPACE_POE_GIT_SCOPE=commit`, so this check runs against
the commit tree being pushed. The news fragment format is Towncrier-style, but
enforcement is repo-local because stock Towncrier does not support
`changes/<package>/` discovery without enumerating every package.
