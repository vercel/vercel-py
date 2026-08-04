# Workflow

`vercel.workflow` provides Vercel Workflows primitives: `Workflows`, workflow
registration, step registration, durable sleeps, hooks, and `start`.

## Basic Workflow

```python
from vercel.workflow import Workflows, sleep, start

app = Workflows()


@app.step
async def charge_customer(*, customer_id: str) -> None:
    ...


@app.workflow
async def renew_subscription(*, customer_id: str) -> None:
    await sleep("1h")
    await charge_customer(customer_id=customer_id)


async def main() -> None:
    run = await start(renew_subscription, customer_id="cus_123")
```

`app.workflow` registers async workflow functions. `app.step` registers async
steps that can be called only from inside a workflow. `sleep()` creates a
durable wait in a workflow run.

Workflows and steps take **keyword arguments only**, so their parameters have
to be declared keyword-only (after a bare `*`); a positional parameter is
rejected when the function is registered.

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
