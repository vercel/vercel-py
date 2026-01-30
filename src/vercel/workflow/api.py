import asyncio
import contextvars
import dataclasses
import json
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
        self.ulid_counter = 0
        self.suspensions: dict[str, Suspension[Any]] = {}
        self.resume_handle: asyncio.Handle | None = None
        self.event_log: list[WorkflowEvent] = []
        self.replay_index = 0

    async def run_step(self, step: core.Step[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        args_json = json.dumps((args, kwargs), sort_keys=True)
        sus = Suspension(ulid=str(self.ulid_counter), step=step, args_json=args_json)
        self.suspensions[sus.ulid] = sus
        self.ulid_counter += 1
        if self.resume_handle is None:
            self.resume_handle = asyncio.get_running_loop().call_soon(self.resume)
        return await sus.future

    def resume(self) -> None:
        self.resume_handle = None
        while self.replay_index < len(self.event_log) and self.suspensions:
            event = self.event_log[self.replay_index]
            self.replay_index += 1

            sus = self.suspensions.pop(event.ulid)
            if event.name == sus.step.name and event.args_json == sus.args_json:
                sus.future.set_result(json.loads(event.result_json))
            else:
                raise RuntimeError("Workflow event log does not match execution")

        if self.suspensions:
            loop = asyncio.get_running_loop()
            for sus in self.suspensions.values():
                loop.create_task(sus.resume())
            self.suspensions.clear()


@dataclasses.dataclass(frozen=True)
class WorkflowEvent:
    ulid: str
    name: str
    args_json: str
    result_json: str


@dataclasses.dataclass(frozen=True)
class Suspension(Generic[T]):
    ulid: str
    step: core.Step[Any, T]
    args_json: str
    future: asyncio.Future[T] = dataclasses.field(default_factory=asyncio.Future)

    async def resume(self) -> None:
        try:
            args, kwargs = json.loads(self.args_json)
            result = await self.step.func(*args, **kwargs)
            self.future.set_result(result)
        except Exception as e:
            self.future.set_exception(e)
        except asyncio.CancelledError:
            self.future.cancel()
        else:
            CTX.get().event_log.append(
                WorkflowEvent(
                    ulid=self.ulid,
                    name=self.step.name,
                    args_json=self.args_json,
                    result_json=json.dumps(result, sort_keys=True),
                )
            )


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
