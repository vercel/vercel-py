# Iter-Coroutine + Base/Runtime Migration

Refactor sync+async modules to eliminate duplication using the iter-coroutine
pattern. This skill covers the full migration workflow: identifying candidates,
structuring internal modules, writing transport-agnostic business logic, and
wiring up sync/async public APIs.

## When to use

- A module has parallel sync and async implementations with duplicated logic.
- You're adding a new feature that needs both sync and async public APIs.
- You're refactoring an existing feature to use the shared HTTP transport layer.

## Core principles

- **Public API is stable.** Same exported names, signatures, return types, and
  behavior. Migrations are internal refactors, not API changes.
- **Internal core is async-first.** All shared business logic lives in async
  methods on a base class.
- **Sync entrypoints are thin wrappers** over the async core via
  `iter_coroutine(...)`, used only when the wrapped coroutine is non-suspending
  in sync mode.
- **HTTP goes through `vercel._internal.http`** transports. Never construct
  `httpx.Client` or `httpx.AsyncClient` directly in feature modules.

## Architecture overview

```
Public API (sync wrappers + async functions)
    │
    │ iter_coroutine() one-shot bridge (sync path)
    │ await (async path)
    ▼
Concrete Clients (SyncFooClient / AsyncFooClient)
    │
    │ inherit from
    ▼
BaseFooClient (all async methods — shared business logic)
    │
    │ uses
    ▼
RequestClient (transport-agnostic async API)
    │
    ▼
Transport (SyncTransport or AsyncTransport)
    │
    ▼
httpx.Client or httpx.AsyncClient
```

## How `iter_coroutine` works

```python
# src/vercel/_internal/iter_coroutine.py
def iter_coroutine(coro: Coroutine[None, None, _T]) -> _T:
    """Execute a non-suspending coroutine synchronously."""
    try:
        coro.send(None)
    except StopIteration as ex:
        return ex.value
    else:
        raise RuntimeError(f"coroutine {coro!r} did not stop after one iteration!")
    finally:
        coro.close()
```

It drives a coroutine forward exactly one step. If the coroutine completes
without suspending, its return value is extracted from the `StopIteration`
exception. If it tries to yield/await, it raises `RuntimeError`.

### Safety rules

| Safe                                              | Unsafe                                           |
|---------------------------------------------------|--------------------------------------------------|
| Coroutines that only call sync code via async API | Coroutines that `await` real I/O                 |
| `SyncTransport.send()` (sync under async facade)  | `AsyncTransport.send()` (real `await`)           |
| Sync sleep, sync file I/O                         | `asyncio.sleep`, `asyncio.create_task`           |
| `inspect.isawaitable()` guarded branches          | Unguarded `await` on user callbacks              |

## Step-by-step migration

### 1. Create the transport-agnostic request client

The request client wraps a `BaseTransport` and exposes an async API. The
transport implementation determines whether I/O is actually sync or async.

```python
# src/vercel/_internal/foo/core.py
from vercel._internal.http import (
    AsyncTransport, BaseTransport, SyncTransport,
    create_base_client, create_base_async_client,
)

class FooRequestClient:
    def __init__(
        self,
        *,
        transport: BaseTransport,
        sleep_fn: SleepFn = asyncio.sleep,
        await_callback: bool = True,
    ) -> None:
        self._transport = transport
        self._sleep_fn = sleep_fn
        self._await_callback = await_callback

    async def request(self, method: str, path: str, **kwargs) -> Any:
        # Retry logic, error mapping, progress callbacks — all async-shaped
        # but non-suspending when backed by SyncTransport
        resp = await self._transport.send(method, path, **kwargs)
        ...
```

Factory functions configure transport + sleep + callback behavior:

```python
def create_sync_request_client(timeout: float = 30.0) -> FooRequestClient:
    transport = SyncTransport(create_base_client(timeout=timeout))
    return FooRequestClient(
        transport=transport,
        sleep_fn=_sync_sleep,         # time.sleep, not asyncio.sleep
        await_callback=False,          # don't await user callbacks
    )

def create_async_request_client(timeout: float = 30.0) -> FooRequestClient:
    transport = AsyncTransport(create_base_async_client(timeout=timeout))
    return FooRequestClient(transport=transport)  # defaults are async-friendly
```

### 2. Write the shared base client

All business logic goes in async methods on a base class:

```python
class BaseFooClient:
    def __init__(self, *, request_client: FooRequestClient) -> None:
        self._request_client = request_client

    async def do_thing(self, path: str, *, token: str) -> Result:
        # Validation, request building, response parsing — all here
        raw = await self._request_client.request("POST", path, token=token)
        return build_result(raw)
```

### 3. Add sync and async concrete clients

```python
class SyncFooClient(BaseFooClient):
    def __init__(self) -> None:
        super().__init__(request_client=create_sync_request_client())

    def close(self) -> None:
        self._request_client.close()

    def __enter__(self) -> SyncFooClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class AsyncFooClient(BaseFooClient):
    def __init__(self) -> None:
        super().__init__(request_client=create_async_request_client())

    async def aclose(self) -> None:
        await self._request_client.aclose()

    async def __aenter__(self) -> AsyncFooClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()
```

### 4. Wire up public API functions

```python
# src/vercel/foo/ops.py
from vercel._internal.iter_coroutine import iter_coroutine

_T = TypeVar("_T")

def _run_sync(
    operation: Callable[[SyncFooClient], Coroutine[None, None, _T]],
) -> _T:
    with SyncFooClient() as client:
        return iter_coroutine(operation(client))


# Sync public API
def do_thing(path: str, *, token: str | None = None) -> Result:
    token = ensure_token(token)
    return _run_sync(lambda c: c.do_thing(path, token=token))


# Async public API
async def do_thing_async(path: str, *, token: str | None = None) -> Result:
    token = ensure_token(token)
    async with AsyncFooClient() as client:
        return await client.do_thing(path, token=token)
```

## Base + runtime split

When sync and async paths differ materially (e.g., threading vs asyncio for
concurrent uploads), use a runtime object:

```python
# Sync runtime — ThreadPoolExecutor
class _SyncUploadRuntime:
    def upload(self, *, parts, upload_fn, ...) -> list[PartResult]:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
            # Submit work, collect results
            ...

# Async runtime — anyio task group
class _AsyncUploadRuntime:
    async def upload(self, *, parts, upload_fn, ...) -> list[PartResult]:
        semaphore = anyio.Semaphore(MAX_CONCURRENCY)
        async with anyio.create_task_group() as tg:
            # Start tasks with semaphore limiting
            ...
```

The base client calls the runtime polymorphically:

```python
class BaseFooClient:
    async def _do_upload(self, ...) -> list[PartResult]:
        parts = await _await_if_necessary(
            self._runtime.upload(
                parts=chunks,
                upload_fn=self._make_upload_fn(),
                ...
            )
        )
        return order_parts(parts)
```

Concrete clients inject the right runtime and upload function:

```python
class SyncFooClient(BaseFooClient):
    def __init__(self) -> None:
        super().__init__(runtime=create_sync_runtime())

    def _make_upload_fn(self):
        # Wrap async method with iter_coroutine for sync execution
        return lambda **kw: iter_coroutine(self._client.upload_part(**kw))


class AsyncFooClient(BaseFooClient):
    def __init__(self) -> None:
        super().__init__(runtime=create_async_runtime())

    def _make_upload_fn(self):
        # Return the async method directly
        return self._client.upload_part
```

### Shared orchestration

Keep these in the base layer, not duplicated per runtime:

- Input validation
- Chunk/part iteration helpers
- Result normalization and ordering
- Final response shaping

## Callback handling

For callbacks that may be sync or async:

```python
async def _emit_progress(
    callback: Callable[[Event], None] | Callable[[Event], Awaitable[None]] | None,
    event: Event,
    *,
    await_callback: bool,
) -> None:
    if callback is None:
        return
    result = callback(event)
    if await_callback and inspect.isawaitable(result):
        await cast(Awaitable[None], result)
```

- Sync clients set `await_callback=False` — callbacks are always sync.
- Async clients set `await_callback=True` — callbacks may be awaitable.

## Testing expectations

- Prefer integration-style tests with `respx` that verify real request flow and
  sync/async parity.
- Do not rely only on monkeypatch tests that assert internal call shape.
- Test both sync and async paths for every operation.
- Validate before commit:
  - `./scripts/lint.sh`
  - `./scripts/test.sh` (targeted or full)

## Reference implementation

The blob module is the canonical example of this pattern:

- **Transport:** `src/vercel/_internal/http/transport.py`
- **Request client:** `src/vercel/_internal/blob/core.py` → `BlobRequestClient`
- **Base client:** `src/vercel/_internal/blob/core.py` → `BaseBlobOpsClient`
- **Sync client:** `src/vercel/_internal/blob/core.py` → `SyncBlobOpsClient`
- **Async client:** `src/vercel/_internal/blob/core.py` → `AsyncBlobOpsClient`
- **Runtime split:** `src/vercel/_internal/blob/multipart.py`
- **Public API:** `src/vercel/blob/ops.py`
- **Types re-export:** `src/vercel/blob/types.py`
