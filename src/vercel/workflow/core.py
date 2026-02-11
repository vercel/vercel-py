from collections.abc import Callable, Coroutine
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")
_workflows: dict[str, "Workflow[Any, Any]"] = {}
_steps: dict[str, "Step[Any, Any]"] = {}


class Workflow[**P, T]:
    def __init__(self, func: Callable[P, Coroutine[Any, Any, T]]):
        self.func = func
        module = getattr(func, "__module__", "<unknown module>")
        self.workflow_id = f"workflow//{module}//{func.__qualname__}"
        assert self.workflow_id not in _workflows, f"Duplicate workflow ID: {self.workflow_id}"
        _workflows[self.workflow_id] = self


def workflow[**P, T](func: Callable[P, Coroutine[Any, Any, T]]) -> Workflow[P, T]:
    return Workflow(func)


def get_workflow(workflow_id: str) -> Workflow[Any, Any]:
    return _workflows[workflow_id]


class Step[**P, T]:
    max_retries: int = 3

    def __init__(self, func: Callable[P, Coroutine[Any, Any, T]]):
        self.func = func
        module = getattr(func, "__module__", "<unknown module>")
        self.name = f"step//{module}//{func.__qualname__}"
        assert self.name not in _steps, f"Duplicate step name: {self.name}"
        _steps[self.name] = self

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        from . import runtime

        try:
            ctx = runtime.WorkflowOrchestratorContext.current()
        except LookupError:
            pass
        else:
            return await ctx.run_step(self, *args, **kwargs)

        # @step decorator works like a no-op when called directly
        return await self.func(*args, **kwargs)


def step[**P, T](func: Callable[P, Coroutine[Any, Any, T]]) -> Step[P, T]:
    return Step(func)


def get_step(step_name: str) -> Step[Any, Any]:
    return _steps[step_name]
