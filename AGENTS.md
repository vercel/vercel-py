# Environment

Requires Python >= 3.10. Uses `uv` for dependency management.

# Scripts

- `./scripts/test.sh` — Run all tests including lint and typecheck (excludes example tests). Accepts extra pytest args.
- `./scripts/test-examples.sh` — Run example tests in parallel. Accepts extra pytest args.
- `./scripts/lint.sh` — Run lint tests only (ruff check + format). Accepts extra pytest args.
- `./scripts/typecheck.sh` — Run typecheck tests only (mypy + module imports). Accepts extra pytest args.
- `./scripts/fix.sh` — Auto-fix lint issues and reformat code with ruff.
