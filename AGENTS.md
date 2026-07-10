# Project tools

- `uv` for package management, virtual environment construction, and running scripts
- `ruff` for linting and formatting
- `mypy` for type checking

# Commands

- `uv run poe qa` — Run lint, typecheck, and tests. Accepts package or file
  scopes plus `-q`/`--quiet` and `-v`/`--verbose`; does not accept
  tool-specific options after `--`.
- `uv run poe test` — Run tests (excludes example tests).
- `uv run poe lint` — Run lint checks (ruff check + format).
- `uv run poe typecheck` — Run type checks (mypy + ty).
- `uv run poe fix` — Auto-fix lint issues and reformat code with ruff.
- `./scripts/test-python-matrix.sh` — Run tests through tox across local Python
  versions. Accepts tox args before `--`, then normal `test.sh` scopes and
  pytest args after `--`.
- `./scripts/test-examples.sh` — Run example tests in parallel. Accepts extra pytest args.

The workspace `test`, `lint`, and `typecheck` Poe commands accept zero or more
scopes before `--` and tool args after `--`. The aggregate `qa` command accepts
scopes but intentionally rejects tool-specific arguments after `--`.

Example: `uv run poe test tests/unit/test_time.py -- -k coerce_duration`
Or by package name: `uv run poe test vercel-oidc`.
Matrix example: `./scripts/test-python-matrix.sh -e py310,py314 -- vercel-queue -- -k subscriptions`.

The workspace task system is documented in `scripts/poe/README.md`. Workspace
packages should include `scripts/poe/poe.toml` and inherit the shared `lint`,
`typecheck`, and `test` tasks unless they have package-specific behavior.

Example package `pyproject.toml` Poe setup:

```toml
[tool.poe]
include = "../../scripts/poe/poe.toml"
verbosity = -1
```

# Commit Message Guidance

- Keep commit messages short and specific.
- Use a title line of 50 characters or fewer.
- Wrap commit message body lines at 72 characters.
- Explain what changed and why.
- Do not list file-by-file changes that are obvious from the diff.
- Do not include any `Co-authored-by:` line.
