import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import collections


class WorkflowLoop(asyncio.BaseEventLoop):
    idle_hook = None

    if TYPE_CHECKING:
        _ready: collections.deque[Any]
        _stopping: bool

    def __init__(self, *, idle_hook: Callable[[], None] | None = None) -> None:
        super().__init__()
        self.idle_hook = idle_hook

    def _run_once(self):
        while self._ready and not self._stopping:
            handle = self._ready.popleft()
            if handle._cancelled:
                continue
            handle._run()
        handle = None  # Needed to break cycles when an exception occurs.

        if self.idle_hook and not self._stopping:
            self.idle_hook()

    def _write_to_self(self):
        # The loop has no way to suspend so we don't need to do
        # anything to wake it up.
        pass

    def call_later(self, delay, callback, *args, context=None):
        raise NotImplementedError

    def call_at(self, when, callback, *args, context=None):
        raise NotImplementedError

    def time(self):
        raise NotImplementedError
