import asyncio
import contextvars
import datetime
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from vercel._internal.core.polyfills import UTC

if sys.version_info >= (3, 11):
    from typing import TypeVarTuple, Unpack
else:
    from typing_extensions import TypeVarTuple, Unpack

if TYPE_CHECKING:
    import collections

_Ts = TypeVarTuple("_Ts")


class Workflow(Protocol):
    def resume(self) -> None: ...

    def time(self) -> float: ...

    def check_suspended(self) -> None: ...

    def run_wait(self, param: datetime.datetime | datetime.timedelta) -> asyncio.Future[None]: ...


class WorkflowLoop(asyncio.BaseEventLoop):
    if TYPE_CHECKING:
        _ready: collections.deque[Any]
        _stopping: bool

    def __init__(self, *, workflow: Workflow) -> None:
        super().__init__()
        self.workflow = workflow
        self._timers: dict[asyncio.TimerHandle, asyncio.Future[None]] = {}

    def _run_once(self) -> None:
        while self._ready and not self._stopping:
            handle = self._ready.popleft()
            if handle._cancelled:
                continue
            handle._run()
        handle = None  # Needed to break cycles when an exception occurs.

        if not self._stopping:
            self.workflow.resume()

    def _write_to_self(self) -> None:
        # The loop has no way to suspend so we don't need to do
        # anything to wake it up.
        pass

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        if handle in self._timers:
            self._timers[handle].cancel()
        super()._timer_handle_cancelled(handle)  # type: ignore[misc]

    def _call_sleep(
        self,
        sleep_spec: datetime.datetime | datetime.timedelta,
        timer: asyncio.TimerHandle,
    ) -> asyncio.TimerHandle:
        def cb(fut: asyncio.Future[None]) -> None:
            self._timers.pop(timer, None)
            if fut.cancelled():
                pass
            elif fut.exception():
                self.call_exception_handler(
                    {
                        "message": "an error occurred waiting for a workflow sleep",
                        "exception": fut.exception(),
                    }
                )
                # Signal anyway, to avoid some weird hangs.
                # TODO: More decisive failures on this case.
                self._ready.append(timer)
            else:
                self._ready.append(timer)

        self.workflow.check_suspended()
        wait_fut = self.workflow.run_wait(sleep_spec)
        wait_fut.add_done_callback(cb)
        self._timers[timer] = wait_fut
        timer._scheduled = True  # type: ignore[attr-defined]

        return timer

    def call_later(
        self,
        delay: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        # Note: the "when" on the TimerHandle is kind of a fib. We
        # calculate it based on now(), which is the latest time from
        # the workflow, but we arrange so that workflow sleep will
        # call it using the current time.
        delta = datetime.timedelta(seconds=max(delay, 0))
        timer = asyncio.TimerHandle(self.time() + delay, callback, args, self, context)
        return self._call_sleep(delta, timer)

    def call_at(
        self,
        when: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        timestamp = datetime.datetime.fromtimestamp(when, tz=UTC)
        timer = asyncio.TimerHandle(when, callback, args, self, context)
        return self._call_sleep(timestamp, timer)

    def time(self) -> float:
        return self.workflow.time()
