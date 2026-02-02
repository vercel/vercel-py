import contextvars
import dataclasses
from typing import Any, ClassVar, Self

from . import world


@dataclasses.dataclass
class WorkflowContext:
    world: world.World = dataclasses.field(default_factory=world.get_world)
    _ctx_tokens: list[contextvars.Token[Self]] = dataclasses.field(init=False, default_factory=list)
    _ctx: ClassVar[contextvars.ContextVar[Self]] = contextvars.ContextVar("WorkflowContext")

    @classmethod
    def current(cls) -> Self:
        return cls._ctx.get()

    def __enter__(self) -> None:
        self._ctx_tokens.append(self._ctx.set(self))

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._ctx.reset(self._ctx_tokens.pop())


async def workflow_handler(
    message: Any,
    *,
    attempt: int,
    queue_name: str,
    message_id: str,
) -> float | None:
    raise NotImplementedError()


async def step_handler(
    message: Any,
    *,
    attempt: int,
    queue_name: str,
    message_id: str,
) -> float | None:
    raise NotImplementedError()


def workflow_entrypoint() -> world.HTTPHandler:
    return world.get_world().create_queue_handler(
        "__wkf_workflow_",
        workflow_handler,
    )


def step_entrypoint() -> world.HTTPHandler:
    return world.get_world().create_queue_handler(
        "__wkf_step_",
        step_handler,
    )
