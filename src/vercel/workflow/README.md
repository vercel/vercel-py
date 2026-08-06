# Workflow

`vercel.workflow` provides Vercel Workflows primitives: `Workflows`, workflow
registration, step registration, durable sleeps, hooks, and `start`.

## Basic Workflow

```python
from vercel.workflow import Workflows, sleep, start

app = Workflows()


@app.step
async def charge_customer(customer_id: str) -> None:
    ...


@app.workflow
async def renew_subscription(customer_id: str) -> None:
    await sleep("1h")
    await charge_customer(customer_id)


async def main() -> None:
    run = await start(renew_subscription, "cus_123")
```

`app.workflow` registers async workflow functions. `app.step` registers async
steps that can be called only from inside a workflow. `sleep()` creates a
durable wait in a workflow run.

## Queue namespaces

Pass a namespace to a workflow registry to isolate its workflow and step
messages on dedicated queue topics:

```python
workflows = Workflows(namespace="billing")
```

## Hooks

```python
from dataclasses import dataclass
from vercel.workflow import BaseHook, Workflows

app = Workflows()


@dataclass
class Approval(BaseHook):
    approved: bool


@app.workflow
async def wait_for_approval() -> bool:
    approval = await Approval.wait()
    return bool(approval and approval.approved)
```

`BaseHook` supports dataclasses and Pydantic models for external resume events.

## Streaming

Every run has a stream a step can write to while it runs, so a client sees
progress without waiting for the run to finish:

```python
from vercel.workflow import Workflows, get_writable

app = Workflows()


@app.step
async def summarize(*, document: str) -> str:
    writable = get_writable()
    summary = []
    async for token in llm.stream(document):
        await writable.write(token)
        summary.append(token)
    return "".join(summary)


@app.step
async def done() -> None:
    await get_writable().close()


@app.workflow
async def analyze(*, document: str) -> str:
    summary = await summarize(document=document)
    await done()
    return summary
```

`get_writable()` works inside a step only. Where the TypeScript SDK lets a
workflow body take a handle and pass it into a step, here it cannot yet, so each
step that streams calls `get_writable()` itself.

Chunks are values, not just bytes: anything the payload format carries (see
below) can be written, and a reader gets it back. A `bytes` chunk arrives on the
TypeScript side as a `Uint8Array`, so a consumer there can pipe the stream
straight into a `Response`.

Three things are worth knowing:

- **Nothing closes a stream for you.** Not the end of a step, not the end of the
  run — the stream spans steps, and a closed stream cannot be reopened. A reader
  of a stream nobody closes waits until the run expires, so close it from the
  last step that has something to say.
- **A step is not complete until its chunks are.** `write()` returns once the
  chunk is buffered, and the step handler flushes before recording the step, so
  "the step finished" implies "everything it streamed is readable". Call
  `await writable.drain()` if you need that guarantee earlier.
- **Retries re-stream.** A step that fails halfway has already written what it
  wrote, and the retry writes it again. Keep chunks idempotent, or stream from a
  step you are willing to see repeated.

`await writable.write_from(source)` forwards an async iterable in one call, and
`get_writable(namespace="logs")` gives the run a second, independent stream.

There is no public reader yet. A stream is consumed today by the TypeScript SDK
(`run.readable`), the dashboard, or `workflow inspect stream <id> --run=<run-id>`.
Python can read one through the world (`streams_get()` yields transport bytes,
`FrameDecoder` turns them into payloads), but those live under
`vercel._internal` and have no auto-reconnect, so a long read will not survive
the server's stream timeout.

## Serializing your own types

Workflow inputs, step results and hook payloads travel in the devalue format
`@workflow/core` uses, which carries `datetime`, `bytes`, `set` and repeated
references natively. `Decimal`, `UUID`, `date`, `time`, `timedelta` and `Path`
are registered on top of that; anything else needs a registration:

```python
import enum

from vercel.workflow import serializable


@serializable
class Point:
    def __init__(self, x: int, y: int) -> None:
        self.x, self.y = x, y

    def _workflow_serialize(self) -> dict[str, int]:
        return {"x": self.x, "y": self.y}

    @classmethod
    def _workflow_deserialize(cls, data: dict[str, int]) -> "Point":
        return cls(**data)


@serializable          # an Enum needs no methods
class Tier(enum.Enum):
    PRO = "pro"
```

`register_serializable()` is the function form, for classes you cannot
decorate.
