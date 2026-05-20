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
