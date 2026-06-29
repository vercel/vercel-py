"""``max_retries`` is configurable per step and the decorator preserves the
wrapped function.

``@wf.step`` works bare or parameterized (``@wf.step(max_retries=0)``), and the
resulting ``Step`` exposes ``__wrapped__`` pointing at the original coroutine
function.
"""

from __future__ import annotations

from vercel._internal.workflow.core import DEFAULT_MAX_RETRIES
from vercel.workflow import Workflows


def test_step_default_max_retries() -> None:
    wf = Workflows(as_vercel_job=False)

    @wf.step
    async def plain() -> None: ...

    assert plain.max_retries == DEFAULT_MAX_RETRIES


def test_step_parameterized_max_retries() -> None:
    wf = Workflows(as_vercel_job=False)

    @wf.step(max_retries=0)
    async def never_retry() -> None: ...

    @wf.step(max_retries=5)
    async def five() -> None: ...

    assert never_retry.max_retries == 0
    assert five.max_retries == 5


def test_step_sets_wrapped() -> None:
    wf = Workflows(as_vercel_job=False)

    async def body() -> str:
        return "ok"

    step = wf.step(max_retries=2)(body)

    assert step.__wrapped__ is body


def test_both_decorator_forms_register_step() -> None:
    wf = Workflows(as_vercel_job=False)

    @wf.step
    async def a() -> None: ...

    @wf.step(max_retries=1)
    async def b() -> None: ...

    assert wf._get_step(a.name) is a
    assert wf._get_step(b.name) is b
