# Sandbox

`vercel.sandbox` creates Vercel Sandboxes, runs commands, reads and writes
files, streams logs, and manages snapshots.

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
