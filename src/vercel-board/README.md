# vercel-board

Python SDK for Vercel Board.

```sh
pip install vercel-board
```

```sh
uv add vercel-board
```

## Quick start

`board()` creates a lazy handle with no I/O. Async is the default surface:

```python
import asyncio
from vercel.board import board


async def main() -> None:
    b = board()
    msg = await b.post("dev", "Deploy finished")
    reply = await b.reply(msg.id, "Smoke tests passed")
    print(reply.id, reply.thread)


asyncio.run(main())
```

The same surface is available synchronously from `vercel.board.sync`, with identical names and arguments:

```python
from vercel.board.sync import board

b = board()
msg = b.post("dev", "Deploy finished")
reply = b.reply(msg.id, "Smoke tests passed")
print(reply.id, reply.thread)
```

For Vercel hosted boards, pass `name=` or `team=` to target a named board or a specific Vercel team. By default, a named board is created on first use; pass `ensure=False` to fail with `BoardNotFoundError` instead (when `BOARD_URL` is set, requests go directly to that configured board endpoint):

```python
b = board(name="ops", team="my-team", ensure=False)
```

## Profiles and permission narrowing

`as_profile()` returns a new handle acting under a profile and/or narrowed to a channel permission scope. Passing `can=` monotonically narrows any existing scope and can never widen it:

```python
import asyncio
from vercel.board import Can, Permission, board


async def main() -> None:
    base = board()
    triage = base.as_profile(
        "triage",
        avatar="https://example.com/triage.png",
        can={"dev/": [Permission.READ, Permission.POST]},
    )

    # Further narrowing intersects with the parent scope
    read_only = triage.as_profile(can={"dev/api": [Permission.READ]})
    me = await read_only.me()
    print(me.handle, me.can.allows(Permission.POST, "dev/api"))  # False

    # Suppress BOARD_PROFILE from the environment
    unprofiled = base.as_profile(None)
    print((await unprofiled.me()).profile)  # None

    scope = Can.from_input({"dev/": [Permission.READ, Permission.POST]})
    narrowed = scope.narrow({"dev/api": [Permission.READ]})
    print(narrowed.to_header())


asyncio.run(main())
```

## Core messaging

```python
import asyncio
from vercel.board import board


async def main() -> None:
    b = board()

    info = await b.discovery()
    caller = await b.me()
    print(info.url, caller.handle)

    msg = await b.post(
        "dev",
        "Investigating latency spike",
        labels={"status": "open"},
        idempotency_key="incident-42",
    )
    found = await b.get_posted("incident-42")
    assert found.id == msg.id

    reply = await b.reply(msg.id, "Mitigation rolled out", after=msg.id)
    await b.edit(reply.id, "Mitigation verified", revision=reply.revision)
    await b.react(msg.id, "thumbs_up")
    await b.label(
        msg.id,
        set={"status": "resolved"},
        if_labels={"status": "open"},
    )
    await b.delete(reply.id)


asyncio.run(main())
```

`post()` also accepts `attachments=[...]`, `no_sync=True`, and `sync=[...]` (note: `sync=[...]` targets `BOARD_URL` HTTP boards). `react(..., remove=True)` removes a reaction, and `label(..., unset=[...])` removes labels.

## Configuration and authentication

By default, the SDK targets Vercel's hosted Board API unless `BOARD_URL` is set. Authentication resolves automatically from deployment OIDC, `BOARD_TOKEN`, `VERCEL_AUTH_TOKEN`, GitHub Actions OIDC, or local `vercel login` credentials (and loopback `vb serve` tokens for local `BOARD_URL` endpoints). `VERCEL_TOKEN` is never used.

Override options for a block with `vercel.api.session`:

```python
import asyncio
from vercel.api import session
from vercel.board import BoardServiceOptions, board, static_credentials_factory


async def main() -> None:
    options = BoardServiceOptions(
        team="team_123",
        credentials_factory=static_credentials_factory("my-token"),
        timeout=15,
    )
    async with session(service_options=[options]):
        msg = await board().post("dev", "Hello from session")
        print(msg.id)


asyncio.run(main())
```

Or create a standalone `BoardClient` owning its own session:

```python
import asyncio
from vercel.board import BoardClient, BoardServiceOptions, static_credentials_factory


async def main() -> None:
    options = BoardServiceOptions(
        url="https://board.example.com",
        credentials_factory=static_credentials_factory("my-token"),
    )
    async with BoardClient.create(options=options) as client:
        msg = await client.board().post("dev", "Hello from client")
        print(msg.id)


asyncio.run(main())
```

For synchronous code, import `BoardClient`, `BoardServiceOptions`, and `static_credentials_factory` from `vercel.board.sync` and use plain `with` blocks. Mixing async calls into a sync session, or the reverse, is rejected.

## Error handling

Board-specific errors inherit from `BoardError`, exposing `.code`, `.status_code`, `.hint`, `.retry_after`, `.request_id`, and `.origin` (session lifecycle and mode-mismatch errors raise `VercelSessionError` / `VercelSessionClosedError` from `vercel-internal-core`):

```python
import asyncio
from vercel.board import (
    BoardError,
    BoardLabelConflictError,
    BoardNotFoundError,
    BoardPermissionError,
    BoardRateLimitError,
    BoardRevisionConflictError,
    board,
)


async def main() -> None:
    b = board(name="ops", ensure=False)
    try:
        msg = await b.post("dev", "Status update")
        await b.label(msg.id, set={"status": "done"}, if_labels={"status": "open"})
    except BoardNotFoundError:
        print("Board or message does not exist")
    except BoardPermissionError:
        print("Caller lacks permission")
    except (BoardRevisionConflictError, BoardLabelConflictError) as exc:
        print(f"Precondition failed: {exc.code}")
    except BoardRateLimitError as exc:
        print(f"Rate limited; retry after {exc.retry_after}")
    except BoardError as exc:
        print(f"Board error ({exc.origin}): {exc}")


asyncio.run(main())
```

Additional subclasses: `BoardValidationError` (also a `ValueError`), `BoardCredentialsError`, `BoardApiError`, `BoardConflictError` (base of `BoardRevisionConflictError`, `BoardLabelConflictError`, and `BoardNewerMessageError`), and `BoardResponseError`. `BoardNotFoundError` is also a `LookupError`, and `BoardPermissionError` is also a `PermissionError`.
