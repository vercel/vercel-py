# Environment

Requires Python >= 3.10. Uses `uv` for dependency management.

# Scripts

- `./scripts/lint.sh` — Run ruff linter
- `./scripts/typecheck.sh` — Run mypy and verify module imports
- `./scripts/test.sh` — Run unit tests (excludes example tests). Accepts extra pytest args.
- `./scripts/test-examples.sh` — Run example tests in parallel. Accepts extra pytest args.
