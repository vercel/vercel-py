# Sandbox

`vercel.sandbox` creates Vercel Sandboxes, runs commands, reads and writes
files, streams logs, and manages snapshots.

## Credentials

Sandbox APIs resolve credentials from the request/OIDC context or
`VERCEL_OIDC_TOKEN`, falling back to `VERCEL_TOKEN`, `VERCEL_PROJECT_ID`, and
`VERCEL_TEAM_ID`. Pass `token=` to individual calls and `project_id=` to
create/list calls when explicit credentials or scope are needed. Tokens are not
stored on returned sandbox handles.

## Async Sandbox

```python
from vercel.sandbox import AsyncSandbox


async def main() -> None:
    async with await AsyncSandbox.create(timeout=60_000) as sandbox:
        command = await sandbox.run_command("python", ["--version"])
        print(await command.stdout())
```

## Commands And Logs

```python
from vercel.sandbox import AsyncSandbox


async def main() -> None:
    sandbox = await AsyncSandbox.create(timeout=60_000)
    command = await sandbox.run_command_detached(
        "python",
        ["-c", "print('ready')"],
    )
    await command.wait()

    async for line in command.logs():
        print(line.data)

    await sandbox.stop()
```

## Snapshots

```python
from vercel.sandbox import AsyncSandbox


async def main() -> None:
    sandbox = await AsyncSandbox.create(timeout=60_000)
    snapshot = await sandbox.snapshot()
    print(snapshot.snapshot_id)
    await sandbox.stop()
```

Use `Sandbox` for synchronous code.
