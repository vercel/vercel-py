# Project tools

- `uv` for package management, virtual environment construction, and running scripts
- `ruff` for linting and formatting
- `mypy` for type checking

# Scripts

- `./scripts/test.sh` — Run tests (excludes example tests).
- `./scripts/test-examples.sh` — Run example tests in parallel. Accepts extra pytest args.
- `./scripts/lint.sh` — Run lint checks (ruff check + format).
- `./scripts/typecheck.sh` — Run type checks (mypy + ty).
- `./scripts/fix.sh` — Auto-fix lint issues and reformat code with ruff.

The workspace `test`, `lint`, and `typecheck` scripts accept zero or more
scopes before `--` and tool args after `--`.

Example: `./scripts/test.sh tests/unit/test_time.py -- -k coerce_duration`
Or by package name: `./scripts/test.sh vercel-oidc`.

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
