"""Write one test for the async and the sync public API.

The two surfaces differ only in how results are delivered: awaited or
returned, `async with` or `with`, async or plain iterators. `Mode` absorbs
that difference so a parametrized test reads as one flow:

    async def test_create(mode: Mode) -> None:
        async with mode.session():
            async with mode.enter(mode.sandbox.create_sandbox(name="box")) as box:
                result = await mode(box.run_process("true"))
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Literal

from fake_sandbox import FakeSandboxApi

from vercel import sandbox
from vercel.sandbox import sync as sandbox_sync


@dataclass
class Mode:
    name: Literal["async", "sync"]
    fake: FakeSandboxApi

    @property
    def sync(self) -> bool:
        return self.name == "sync"

    @property
    def sandbox(self) -> ModuleType:
        """`vercel.sandbox` or `vercel.sandbox.sync`."""
        return sandbox_sync if self.sync else sandbox

    def session(self) -> AbstractAsyncContextManager[Any]:
        """A session of this mode bound to the fake API."""
        return self.enter(self.fake.session(sync=self.sync))

    async def __call__(self, value: Any) -> Any:
        """Await ``value`` in async mode; return it as it is in sync mode."""
        return await value if inspect.isawaitable(value) else value

    def enter(self, manager: Any) -> AbstractAsyncContextManager[Any]:
        """Enter a context manager the way this mode's code would.

        A manager that supports both protocols, like `vercel.api.session`, is
        entered with the protocol matching the mode.
        """
        use_sync = hasattr(manager, "__enter__") and (
            self.sync or not hasattr(manager, "__aenter__")
        )

        @asynccontextmanager
        async def entered() -> AsyncIterator[Any]:
            if use_sync:
                with manager as value:
                    yield value
            else:
                async with manager as value:
                    yield value

        return entered()

    async def collect(self, items: Any) -> list[Any]:
        """Drain an async or sync iterable."""
        if hasattr(items, "__aiter__"):
            return [item async for item in items]
        return list(items)

    async def first(self, items: Any) -> Any:
        """Take the first item of an async or sync iterator, then stop."""
        if hasattr(items, "__aiter__"):
            async for item in items:
                return item
            raise AssertionError("iterator was empty")
        for item in items:
            return item
        raise AssertionError("iterator was empty")

    def handle_types(self, *names: str) -> tuple[type, ...]:
        """Resolve public handle classes by their async names, e.g. ``"Sandbox"``."""
        prefix = "Sync" if self.sync else ""
        return tuple(getattr(self.sandbox, prefix + name) for name in names)


__all__ = ["Mode"]
