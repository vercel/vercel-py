from __future__ import annotations

import contextlib
import contextvars
import dataclasses
import datetime
import json
import sys
from collections.abc import AsyncIterator, Callable, Coroutine, Generator
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import pydantic

if TYPE_CHECKING:
    from . import world as w

P = ParamSpec("P")
T = TypeVar("T")

# Global (default) registries — used when no sandbox is active.
_global_workflows: dict[str, Workflow[Any, Any]] = {}
_global_steps: dict[str, Step[Any, Any]] = {}

# When a sandbox sets these, decorators and lookups use the
# sandbox-local dicts instead of the globals above.
_cv_workflows: contextvars.ContextVar[dict[str, Workflow[Any, Any]] | None] = (
    contextvars.ContextVar("_cv_workflows", default=None)
)
_cv_steps: contextvars.ContextVar[dict[str, Step[Any, Any]] | None] = contextvars.ContextVar(
    "_cv_steps", default=None
)


def _get_workflows() -> dict[str, Workflow[Any, Any]]:
    rv = _cv_workflows.get()
    return _global_workflows if rv is None else rv


def _get_steps() -> dict[str, Step[Any, Any]]:
    rv = _cv_steps.get()
    return _global_steps if rv is None else rv


@contextlib.contextmanager
def clean_registry():
    wf_token = _cv_workflows.set({})
    st_token = _cv_steps.set({})
    try:
        yield
    finally:
        _cv_steps.reset(st_token)
        _cv_workflows.reset(wf_token)


class Workflow(Generic[P, T]):
    def __init__(self, func: Callable[P, Coroutine[Any, Any, T]]):
        self.func = func
        self.module = getattr(func, "__module__", "<unknown module>")
        self.workflow_id = f"workflow//{self.module}//{func.__qualname__}"
        registry = _get_workflows()
        assert self.workflow_id not in registry, f"Duplicate workflow ID: {self.workflow_id}"
        registry[self.workflow_id] = self


def workflow(func: Callable[P, Coroutine[Any, Any, T]]) -> Workflow[P, T]:
    return Workflow(func)


def get_workflow(workflow_id: str) -> Workflow[Any, Any]:
    return _get_workflows()[workflow_id]


class Step(Generic[P, T]):
    max_retries: int = 3

    def __init__(self, func: Callable[P, Coroutine[Any, Any, T]]):
        self.func = func
        module = getattr(func, "__module__", "<unknown module>")
        self.name = f"step//{module}//{func.__qualname__}"
        registry = _get_steps()
        assert self.name not in registry, f"Duplicate step name: {self.name}"
        registry[self.name] = self

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        from . import runtime

        try:
            ctx = runtime.WorkflowOrchestratorContext.current()
        except LookupError:
            raise RuntimeError(
                "cannot call step outside workflow; use a wrapper function instead"
            ) from None

        return await ctx.run_step(self, *args, **kwargs)


def step(func: Callable[P, Coroutine[Any, Any, T]]) -> Step[P, T]:
    return Step(func)


def get_step(step_name: str) -> Step[Any, Any]:
    return _get_steps()[step_name]


async def sleep(param: int | float | datetime.datetime | str) -> None:
    from . import runtime

    try:
        ctx = runtime.WorkflowOrchestratorContext.current()
    except LookupError:
        raise RuntimeError("cannot call sleep outside workflow") from None

    await ctx.run_wait(param)


class HookEvent(Generic[T]):
    def __init__(self, *, correlation_id: str, token: str) -> None:
        self._correlation_id = correlation_id
        self._token = token
        self._disposed = False

    def __await__(self) -> Generator[Any, None, T | None]:
        async def next_or_none() -> T | None:
            try:
                return await self.__anext__()
            except StopAsyncIteration:
                return None

        return next_or_none().__await__()

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        from . import runtime

        try:
            ctx = runtime.WorkflowOrchestratorContext.current()
        except LookupError:
            raise RuntimeError("cannot iterate HookEvent outside workflow") from None

        return await ctx.run_hook(correlation_id=self._correlation_id)

    def dispose(self) -> None:
        if self._disposed:
            return

        from . import runtime

        try:
            ctx = runtime.WorkflowOrchestratorContext.current()
        except LookupError:
            raise RuntimeError("cannot call dispose() outside workflow") from None

        self._disposed = True
        ctx.dispose_hook(correlation_id=self._correlation_id)


class HookMixin:
    @classmethod
    def wait(cls, *, token: str | None = None) -> HookEvent[Self]:
        from . import runtime

        try:
            ctx = runtime.WorkflowOrchestratorContext.current()
        except LookupError:
            raise RuntimeError("cannot call wait() outside workflow") from None
        else:
            return ctx.create_hook(token, cls)

    async def resume(self, token_or_hook: str | w.Hook, **kwargs) -> w.Hook:
        from . import runtime

        try:
            runtime.WorkflowOrchestratorContext.current()
        except LookupError:
            pass
        else:
            raise RuntimeError("cannot call resume() inside workflow")

        if isinstance(self, pydantic.BaseModel):
            json_str = self.model_dump_json(**kwargs)
        elif dataclasses.is_dataclass(self):
            obj = dataclasses.asdict(self, dict_factory=kwargs.pop("dict_factory", dict))
            json_str = json.dumps(obj, **kwargs)
        else:
            raise TypeError("resume only supports pydantic models or dataclasses")

        return await runtime.resume_hook(token_or_hook, json_str)
