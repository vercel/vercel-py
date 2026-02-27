# Scripts

- `./scripts/test.sh` — Run all tests including lint and typecheck (excludes example tests). Accepts extra pytest args.
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
