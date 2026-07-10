from __future__ import annotations

import dataclasses
import datetime
import functools
import json
from collections.abc import AsyncIterator, Callable, Coroutine, Generator
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar, overload

import pydantic

from vercel._internal.polyfills import Self

from . import py_sandbox
from .world import validate_queue_namespace

if TYPE_CHECKING:
    from . import world as w


P = ParamSpec("P")
T = TypeVar("T")

DEFAULT_MAX_RETRIES = 3


class Workflow(Generic[P, T]):
    def __init__(
        self,
        func: Callable[P, Coroutine[Any, Any, T]],
        *,
        registry: Workflows,
    ):
        self.func = func
        self._registry = registry
        self.module = func.__module__
        self.qualname = func.__qualname__
        self.workflow_id = f"workflow//{self.module}.{self.qualname}"

    def _resolve_queue_namespace(self) -> str | None:
        return self._registry.namespace


class Step(Generic[P, T]):
    def __init__(
        self, func: Callable[P, Coroutine[Any, Any, T]], *, max_retries: int = DEFAULT_MAX_RETRIES
    ):
        self.func = func
        self.name = f"step//{func.__module__}.{func.__qualname__}"
        self.max_retries = max_retries
        functools.update_wrapper(self, func)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        from . import runtime

        try:
            ctx = runtime.WorkflowOrchestratorContext.current()
        except LookupError:
            raise RuntimeError(
                "cannot call step outside workflow; use a wrapper function instead"
            ) from None

        return await ctx.run_step(self, *args, **kwargs)


async def sleep(param: int | float | datetime.datetime | str) -> None:
    from . import runtime

    try:
        ctx = runtime.WorkflowOrchestratorContext.current()
    except LookupError:
        raise RuntimeError("cannot call sleep outside workflow") from None

    await ctx.run_wait(param)


def now() -> datetime.datetime:
    from . import runtime

    try:
        ctx = runtime.WorkflowOrchestratorContext.current()
    except LookupError:
        raise RuntimeError("cannot call now() outside workflow") from None

    return ctx.now()


def time_ns() -> int:
    from . import runtime

    try:
        ctx = runtime.WorkflowOrchestratorContext.current()
    except LookupError:
        raise RuntimeError("cannot call time_ns() outside workflow") from None

    return ctx.time_ns()


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


class BaseHook:
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


class Workflows:
    def __init__(
        self,
        *,
        as_vercel_job: bool = True,
        namespace: str | None = None,
        sandbox_policy: py_sandbox.SandboxPolicy | None = None,
    ):
        validate_queue_namespace(namespace)

        self._namespace = namespace
        self._workflows: dict[str, Workflow] = {}
        self._steps: dict[str, Step] = {}
        if sandbox_policy is None:
            sandbox_policy = py_sandbox.SandboxPolicy()
        self._sandbox_policy = sandbox_policy
        if as_vercel_job and not py_sandbox.in_sandbox():
            from . import runtime

            runtime.workflow_entrypoint(self)
            runtime.step_entrypoint(self)

    @property
    def namespace(self) -> str | None:
        """The immutable queue namespace for this registry."""
        return self._namespace

    def workflow(self, func: Callable[P, Coroutine[Any, Any, T]]) -> Workflow[P, T]:
        rv = Workflow(func, registry=self)
        assert rv.workflow_id not in self._workflows, f"Duplicate workflow ID: {rv.workflow_id}"
        self._workflows[rv.workflow_id] = rv
        return rv

    def _get_workflow(self, workflow_id: str) -> Workflow[Any, Any]:
        return self._workflows[workflow_id]

    @overload
    def step(self, func: Callable[P, Coroutine[Any, Any, T]]) -> Step[P, T]: ...

    @overload
    def step(
        self, *, max_retries: int = ...
    ) -> Callable[[Callable[P, Coroutine[Any, Any, T]]], Step[P, T]]: ...

    def step(
        self,
        func: Callable[P, Coroutine[Any, Any, T]] | None = None,
        *,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> Step[P, T] | Callable[[Callable[P, Coroutine[Any, Any, T]]], Step[P, T]]:
        def register(f: Callable[P, Coroutine[Any, Any, T]]) -> Step[P, T]:
            rv = Step(f, max_retries=max_retries)
            assert rv.name not in self._steps, f"Duplicate step name: {rv.name}"
            self._steps[rv.name] = rv
            return rv

        if func is None:
            return register
        return register(func)

    def _get_step(self, step_name: str) -> Step[Any, Any]:
        return self._steps[step_name]
