import asyncio
import contextvars
from collections.abc import Coroutine
from typing import Any, Generic, Literal, ParamSpec, TypeVar

from . import core

P = ParamSpec("P")
T = TypeVar("T")
CTX: contextvars.ContextVar["WorkflowContext"] = contextvars.ContextVar("WorkflowContext")


def get_workflow_context() -> "WorkflowContext":
    return CTX.get()


class WorkflowContext:
    def __init__(self, wf: core.Workflow[Any, Any]):
        self.workflow = wf

    async def run_step(self, step: core.Step[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        # TODO: implement step tracking, replaying, etc.
        return await step.func(*args, **kwargs)


class Run(Generic[T]):
    def __init__(self, workflow: core.Workflow[Any, T], coroutine: Coroutine[Any, Any, T]):
        with CTX.set(WorkflowContext(workflow)):
            self.fut = asyncio.ensure_future(coroutine)

    async def get_status(self) -> Literal["completed", "failed", "running"]:
        if self.fut.done():
            if self.fut.exception() is None:
                return "completed"
            else:
                return "failed"
        else:
            return "running"

    async def get_return_value(self) -> T:
        return await self.fut


async def start(wf: core.Workflow[P, T], *args: P.args, **kwargs: P.kwargs) -> Run[T]:
    return Run(wf, wf.func(*args, **kwargs))
