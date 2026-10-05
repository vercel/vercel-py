from collections.abc import Iterator
from contextlib import contextmanager

from vercel.workflow._internal.runtime import WorkflowOrchestratorContext


@contextmanager
def bind_context(context: WorkflowOrchestratorContext) -> Iterator[None]:
    """Bind the workflow context when testing replay outside run_workflow()."""
    token = context._ctx.set(context)
    try:
        yield
    finally:
        context._ctx.reset(token)
