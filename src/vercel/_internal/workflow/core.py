from __future__ import annotations

import dataclasses
import datetime
import functools
import inspect
import random as _random
from collections.abc import AsyncIterator, Callable, Coroutine, Generator
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar, overload

import pydantic

from vercel._internal.core.polyfills import Self

from . import py_sandbox
from .world import validate_queue_namespace

if TYPE_CHECKING:
    from . import world as w


P = ParamSpec("P")
T = TypeVar("T")

DEFAULT_MAX_RETRIES = 3


def _bind_arguments(
    signature: inspect.Signature,
    qualname: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Split a call by what the callee's parameters can be *named*.

    A value whose parameter has a usable name is recorded by name; only a
    parameter that cannot be named -- positional-only (before a `/`) or
    `*args` -- is recorded by position. So against
    `async def charge(amount=1, currency="usd", *, tier="basic")`:

        charge(21)                  -> [{"amount": 21}]
        charge(amount=21)           -> [{"amount": 21}]
        charge(21, "eur")           -> [{"amount": 21, "currency": "eur"}]
        charge(21, currency="eur")  -> [{"amount": 21, "currency": "eur"}]
        charge(currency="eur")      -> [{"currency": "eur"}]

    ...and against `async def notify(total, /, *rest)`:

        notify(42)                  -> [42]
        notify(42, "now")           -> [42, "now"]

    So the same values record the same bytes however the call was spelled, which
    matters because the step-input determinism check compares recorded bytes
    against freshly encoded ones: rewriting `charge(21)` as `charge(amount=21)`
    stays cosmetic instead of failing an in-flight run.

    Recording by name also survives a reorder -- swapping two parameters cannot
    silently swap the values of a run already in flight, and a rename fails
    loudly. Recording by position gives that up in exchange for the shape
    TypeScript writes, which is why it takes a `/` to ask for.

    Defaults are deliberately not applied, so an omitted default stays off the
    wire and adding a parameter with one remains replay-safe.

    `bind` is here to resolve values to parameter names, and for its arity check,
    which reports a bad call as an ordinary `TypeError` here rather than inside
    the decoder or during replay. The split below is recomputed from each
    parameter's kind rather than taken from `BoundArguments.args` / `.kwargs`,
    which split on position instead.
    """
    try:
        bound = signature.bind(*args, **kwargs)
    except TypeError as error:
        # `bind` phrases these as "missing a required argument: 'amount'"; the
        # name is what the caller lacks, since `start(wf, ...)` and a step call
        # both put the traceback frame somewhere other than the definition.
        raise TypeError(f"{qualname}() {error}") from None

    positional: list[Any] = []
    keyword: dict[str, Any] = {}
    # Signature order, so a positional-only parameter keeps its slot ahead of
    # whatever `*args` contributes.
    for name, param in signature.parameters.items():
        if name not in bound.arguments:
            continue
        value = bound.arguments[name]
        if param.kind is param.POSITIONAL_ONLY:
            positional.append(value)
        elif param.kind is param.VAR_POSITIONAL:
            positional.extend(value)
        elif param.kind is param.VAR_KEYWORD:
            keyword.update(value)
        else:
            keyword[name] = value
    return tuple(positional), keyword


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
        self.workflow_id = f"workflow//{self.module}//{self.qualname}"
        self._signature = inspect.signature(func)

    def bind_arguments(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return _bind_arguments(self._signature, self.qualname, args, kwargs)

    def _resolve_queue_namespace(self) -> str | None:
        return self._registry.namespace


class Step(Generic[P, T]):
    def __init__(
        self, func: Callable[P, Coroutine[Any, Any, T]], *, max_retries: int = DEFAULT_MAX_RETRIES
    ):
        self.func = func
        self.name = f"step//{func.__module__}//{func.__qualname__}"
        self.max_retries = max_retries
        self._signature = inspect.signature(func)
        functools.update_wrapper(self, func)

    def bind_arguments(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return _bind_arguments(self._signature, self.func.__qualname__, args, kwargs)

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


def random() -> _random.Random:
    from . import runtime

    try:
        ctx = runtime.WorkflowOrchestratorContext.current()
    except LookupError:
        raise RuntimeError("cannot call random() outside workflow") from None

    return ctx.random()


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
            payload = self.model_dump(**{"mode": "python", **kwargs})
        elif dataclasses.is_dataclass(self):
            payload = dataclasses.asdict(self, **kwargs)
        else:
            raise TypeError("resume only supports pydantic models or dataclasses")

        return await runtime.resume_hook(token_or_hook, payload)


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
