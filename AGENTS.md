# Project tools

- `uv` for package management, virtual environment construction, and running scripts
- `ruff` for linting and formatting
- `mypy` for type checking

# Scripts

- `./scripts/test.sh` — Run all tests including lint and typecheck (excludes example tests). Accepts extra pytest args.
  Example: `./scripts/test.sh -q tests/unit/test_filesystem.py`
- `./scripts/test-examples.sh` — Run example tests in parallel. Accepts extra pytest args.
- `./scripts/lint.sh` — Run lint tests only (ruff check + format). Accepts extra pytest args.
- `./scripts/typecheck.sh` — Run typecheck tests only (mypy + module imports). Accepts extra pytest args.
- `./scripts/fix.sh` — Auto-fix lint issues and reformat code with ruff.

# Commit Message Guidance

- Keep commit messages short and specific.
- Use a title line of 50 characters or fewer.
- Wrap commit message body lines at 72 characters.
- Explain what changed and why.
- Do not list file-by-file changes that are obvious from the diff.
- Do not include any `Co-authored-by:` line.

# Code Organization

- Put implementation in `src/vercel/_internal/...`.
- Treat public packages as API composition points: import internal implementations and re-export only the names that are intentionally public.
- Do not export helpers, adapters, aliases, or implementation dependencies from a public package just because another module needs them.
- Internal modules should import reusable code from `_internal`, not from public `vercel.*` facade packages.
