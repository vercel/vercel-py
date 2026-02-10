# AGENTS.md

## Local Workflow

- Install/sync dependencies: `uv sync`
- Run all tests: `uv run pytest`
- Run a focused test file: `uv run pytest tests/path/to/test_file.py`
- Run lint checks: `uv run ruff check .`
- Auto-fix lint issues where possible: `uv run ruff check --fix .`
- Format code: `uv run ruff format .`

## Before Opening a PR

- Run `uv run ruff check .`
- Run `uv run ruff format --check .` (or `uv run ruff format .`)
- Run relevant tests for changed areas, then run `uv run pytest` if changes are broad

## Commit Message Guidance

- Keep commit messages short and specific.
- Use a title line of 50 characters or fewer.
- Wrap commit message body lines at 72 characters.
- Explain what changed and why.
- Do not list file-by-file changes that are obvious from the diff.
- Do not include any `Co-authored-by:` line.

### Good examples

- `Add shared HTTP transport helpers`
- `Move iter_coroutine to a dedicated module`
- `Fix async request hook header handling`
