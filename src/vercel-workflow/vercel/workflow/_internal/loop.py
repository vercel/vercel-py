import asyncio
import contextvars
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if sys.version_info >= (3, 11):
    from typing import TypeVarTuple, Unpack
else:
    from typing_extensions import TypeVarTuple, Unpack

if TYPE_CHECKING:
    import collections

_Ts = TypeVarTuple("_Ts")


class WorkflowLoop(asyncio.BaseEventLoop):
    if TYPE_CHECKING:
        _ready: collections.deque[Any]
        _stopping: bool

    def __init__(self, *, idle_hook: Callable[[], None] | None = None) -> None:
        super().__init__()
        self.idle_hook: Callable[[], None] | None = idle_hook

    def _run_once(self) -> None:
        while self._ready and not self._stopping:
            handle = self._ready.popleft()
            if handle._cancelled:
                continue
            handle._run()
        handle = None  # Needed to break cycles when an exception occurs.

        if self.idle_hook and not self._stopping:
            self.idle_hook()

    def _write_to_self(self) -> None:
        # The loop has no way to suspend so we don't need to do
        # anything to wake it up.
        pass

    def call_later(
        self,
        delay: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        raise NotImplementedError

    def call_at(
        self,
        when: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        raise NotImplementedError

    def time(self) -> float:
        raise NotImplementedError
