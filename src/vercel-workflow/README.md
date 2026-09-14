# Workflow

`vercel.workflow` provides Vercel Workflows primitives: `Workflows`, workflow
registration, step registration, durable sleeps, hooks, and `start`.

```sh
pip install vercel-workflow
```

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

## Step retries

A step that raises is retried up to `max_retries` times. Two errors change that
from inside the body:

```python
from vercel.workflow import FatalError, RetryableError


@app.step(max_retries=5)
async def charge_customer(customer_id: str) -> None:
    if not customer_id:
        # Retrying cannot help, so fail the step on this attempt.
        raise FatalError("no customer")
    response = await http.post(...)
    if response.status_code == 429:
        # Retry, but not before the API says we may.
        raise RetryableError("rate limited", retry_after="10s")
```

`retry_after` accepts the same values `sleep()` accepts — `"10s"`, a number of
seconds, a `datetime.timedelta`, or an absolute timezone-aware `datetime`
— and defaults to one second. It changes when the next attempt runs, not how
many attempts there are:
a step that has used up its retries fails whichever error it raised.

Inside a step body, `get_step_metadata()` returns the run and step ids, the
current `attempt`, and `step_started_at`. That last one is when the *first*
attempt began, so a step can tell how long it has been trying in total:

```python
from datetime import datetime, timezone

from vercel.workflow import get_step_metadata


@app.step
async def charge_customer(customer_id: str) -> None:
    info = get_step_metadata()
    elapsed = datetime.now(timezone.utc) - info.step_started_at
```

## Run attributes

`set_attributes()` puts plaintext key/value metadata on the run. Callable from
a workflow body or a step:

```python
from vercel.workflow import remove_attributes, set_attributes

@app.workflow
async def renew_subscription(customer_id: str) -> None:
    await set_attributes(customer=customer_id, phase="charging")
    await charge_customer(customer_id)
    await set_attributes(phase="done")
    await remove_attributes("customer")
```

Attributes are never encrypted, so they shouldn't carry sensitive information;
keys are capped at 256 characters, values at 256 bytes when encoded, and a run
at 64 attributes.

`run.attributes()` reads them back:

```python
run = await start(renew_subscription, "cus_123")
await run.return_value()

assert await run.attributes() == {"phase": "done"}
```

## Queue namespaces

Pass a namespace to a workflow registry to isolate its messages on a dedicated
queue topic:

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
    return approval.approved
```

`BaseHook` supports dataclasses and Pydantic models for external resume events.

Pass `metadata` to record data on the hook itself, for whoever resumes it:

```python
@app.workflow
async def wait_for_approval(order_id: str) -> bool:
    approval = await Approval.wait(token=f"order:{order_id}", metadata={"order": order_id})
    return approval.approved
```

Metadata is written once, when the hook is registered, and is not part of the
payload. The resumer reads it back with `get_hook_by_token()`, already decoded,
which is how a run tells it what it is waiting for:

```python
from vercel.workflow import get_hook_by_token

hook = await get_hook_by_token(f"order:{order_id}")
if hook.metadata["order"] == order_id:
    await Approval(approved=True).resume(hook)
```

That `Hook` carries the token, hook and run ids, when it was created, and the
decoded metadata. Pass it back to `resume()` rather than the token to reuse the
lookup. `get_hook_by_token()` raises `HookNotFoundError` when no live hook holds
the token.

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

A workflow body can call `get_writable()` too and pass the result to its steps,
which take it as a `WorkflowWritable` and write to it:

```python
@app.step
async def summarize(*, document: str, out: WorkflowWritable) -> str:
    await out.write("starting")
    ...


@app.workflow
async def analyze(*, document: str) -> str:
    out = get_writable()
    return await summarize(document=document, out=out)
```

Only a step can write. Calling `write()` on what the workflow body holds raises.

Chunks are values, not just bytes: anything the payload format carries (see
below) can be written, and a reader gets it back. A `bytes` chunk arrives on the
TypeScript side as a `Uint8Array`, so a consumer there can pipe the stream
straight into a `Response`.

A stream can carry a type. `get_writable(type=Token)` returns a
`WorkflowWritable[Token]`, and each write is dumped through pydantic the way a
typed step argument is:

```python
class Token(pydantic.BaseModel):
    text: str
    index: int


@app.step
async def summarize(*, document: str) -> str:
    writable = get_writable(type=Token)
    index = 0
    async for text in llm.stream(document):
        await writable.write(Token(text=text, index=index))
        index += 1
    ...
```

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

`async with get_writable() as writable:` closes the stream on the way out — on
the clean path only, so a step that raises leaves the stream open for its retry.

### Reading it back

`run.readable()` yields the values as they are written, and ends when a step
closes the stream:

```python
run = await start(analyze, document=text)

async for chunk in run.readable():
    print(chunk)
```

`run.readable(type=Token)` validates each chunk against the type on the way back, so
what a step wrote as a model is a model again, and a chunk that does not match
raises `TypeValidationError`:

```python
async for token in run.readable(type=Token):
    print(token.text)
```

`run.readable_bytes()` is the same thing narrowed to `bytes`, which is what an
HTTP body wants — hand it to a streaming response as-is.

Pass `start_index` to resume: a positive index picks up exactly where a client
left off, a negative one reads that many chunks back from the end. Only the
positive form survives a dropped connection, because a negative index resolves
against wherever the tail happened to be when it connected.

```python
async for chunk in run.readable(start_index=last_seen + 1):
    ...
```

A read reconnects on its own when the transport drops, which it will: the
server ends a long read at its own time limit. Resuming is exact, so nothing is
duplicated or skipped.

`run.stream_info()` gives the last chunk index and whether the stream is closed
(`tail_index` is `-1` before anything is written), `run.list_streams()` lists
every stream the run has, and `read_stream(run_id, name)` reads one by name,
taking a type the same way `run.readable()` does.

The same stream is readable from the TypeScript SDK (`run.readable`), the
dashboard, and `workflow inspect stream <id> --run=<run-id>`.

## Serializing your own types

Workflow inputs, step results and hook payloads travel in the devalue format
`@workflow/core` uses, which carries `datetime`, `bytes`, `set` and repeated
references natively. `Decimal`, `UUID`, `date`, `time`, `timedelta` and `Path`
are registered on top of that.

Pydantic models and dataclasses will be automatically serialized and
validated using Pydantic.

```python
class Order(pydantic.BaseModel):
    sku: str
    quantity: int


@app.step
async def fulfil(order: Order) -> Receipt:
    ...


@app.step
async def fulfil_many(orders: list[Order] | None) -> None:
    ...
```

A value that does not match its annotation raises
`TypeValidationError`, which is a fatal error for a step.

Classes other than models and dataclasses can have custom serializers
written for them:

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
