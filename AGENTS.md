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

## Iter-Coroutine + Base/Runtime Migration Pattern

Use this as the default shape when refactoring sync+async modules to reduce
duplication.

### Core principles

- Keep public API stable: same exported names, signatures, return types, and
  behavior.
- Make the internal core async-first.
- Make sync entrypoints thin wrappers over async core via `iter_coroutine(...)`
  only when the wrapped coroutine is non-suspending in sync mode.
- Route HTTP through `vercel._http` clients/transports; avoid direct
  `httpx.Client`/`httpx.AsyncClient` construction in refactored feature modules.

### Recommended structure

- Create a private async base class for shared logic:
  - Example shape: `_Base<Domain>Client` with async methods for shared ops.
  - Keep parsing/validation/result-shaping helpers in this layer.
- Add private sync/async concrete classes:
  - Sync uses `SyncTransport(...)` and sync callbacks.
  - Async uses `AsyncTransport(...)` and awaitable callbacks.
- Keep public sync functions as wrappers that call
  `iter_coroutine(base_client.async_method(...))`.
- Keep public async functions as direct `await` on the same async methods.

### Base + runtime split (for true runtime-specific behavior)

When sync and async must differ materially (threading vs asyncio scheduling),
use a runtime contract and shared orchestration:

- Define one runtime method name (for example, `upload(...)`).
- Implement two runtimes:
  - blocking runtime: threadpool/locks/sync callback handling.
  - async runtime: `asyncio.create_task`/`asyncio.wait`/awaitable callbacks.
- Keep common orchestration shared:
  - validation
  - chunk/part iteration helpers
  - normalization/order of results
  - final response shaping

### `iter_coroutine` guardrails

- Safe: sync wrappers around coroutines that complete without real suspension.
- Unsafe: coroutines that rely on event-loop scheduling (network awaits,
  `asyncio.sleep`, task scheduling, etc.).
- For mixed callback paths, use explicit `inspect.isawaitable(...)` checks in
  async code rather than forcing everything through `iter_coroutine`.

### Testing expectations for migrations

- Prefer integration-style tests with `respx` that verify real request flow and
  sync/async parity.
- Do not rely only on monkeypatch tests that assert internal call shape.
- Validate before commit:
  - `uv run ruff check .`
  - `uv run ruff format --check .`
  - targeted tests for changed modules
  - `uv run pytest` when changes are broad
