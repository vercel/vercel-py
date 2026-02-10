from collections.abc import Callable, Coroutine
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


class Workflow[**P, T]:
    def __init__(self, func: Callable[P, Coroutine[Any, Any, T]]):
        self.func = func
        module = getattr(func, "__module__", "<unknown module>")
        self.workflow_id = f"workflow//{module}//{func.__qualname__}"


def workflow[**P, T](func: Callable[P, Coroutine[Any, Any, T]]) -> Workflow[P, T]:
    return Workflow(func)


class Step[**P, T]:
    def __init__(self, func: Callable[P, Coroutine[Any, Any, T]]):
        self.func = func
        module = getattr(func, "__module__", None)
        if module is None:
            self.name = func.__qualname__
        else:
            self.name = f"{module}.{func.__qualname__}"

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        from . import api

        try:
            ctx = api.get_workflow_context()
        except LookupError:
            pass
        else:
            return await ctx.run_step(self, *args, **kwargs)

        # @step decorator works like a no-op when called directly
        return await self.func(*args, **kwargs)


def step[**P, T](func: Callable[P, Coroutine[Any, Any, T]]) -> Step[P, T]:
    return Step(func)
