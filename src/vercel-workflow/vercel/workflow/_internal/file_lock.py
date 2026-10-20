"""Directory leases interoperable with proper-lockfile's realpath=false mode.

Like proper-lockfile, this is a cooperative lease, not a fencing mechanism:
participants must use the same stale threshold and allow heartbeat tasks to run.
"""

from __future__ import annotations

import contextlib
import math
import os
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import anyio


class FileLockError(RuntimeError):
    """A directory lease could not be acquired, maintained, or released."""


class FileLockTimeoutError(FileLockError, TimeoutError):
    """Another owner held the lease beyond the acquisition timeout."""


class FileLockCompromisedError(FileLockError):
    """The lease disappeared or another writer changed its ownership marker."""


def _identity(stat: os.stat_result) -> tuple[int, int, int]:
    return stat.st_dev, stat.st_ino, stat.st_mtime_ns


def _timestamp_ns(*, seconds: bool = False) -> int:
    milliseconds = time.time_ns() // 1_000_000
    if seconds:
        milliseconds = ((milliseconds + 999) // 1000) * 1000
    return milliseconds * 1_000_000


@dataclass
class FileLock:
    """An acquired lease; check it before publishing work after an await."""

    path: Path
    _stat: os.stat_result
    _seconds: bool
    _error: FileLockCompromisedError | None = None

    def check(self) -> None:
        """Raise if this directory or its heartbeat no longer belongs to us."""
        if self._error is not None:
            raise self._error
        try:
            current = self.path.stat()
        except FileNotFoundError as exc:
            self._error = FileLockCompromisedError(f"Lock disappeared: {self.path}")
            raise self._error from exc
        if _identity(current) != _identity(self._stat):
            self._error = FileLockCompromisedError(f"Lock ownership changed: {self.path}")
            raise self._error

    def _refresh(self) -> None:
        self.check()
        timestamp = _timestamp_ns(seconds=self._seconds)
        os.utime(self.path, ns=(timestamp, timestamp))
        updated = self.path.stat()
        if (updated.st_dev, updated.st_ino) != (self._stat.st_dev, self._stat.st_ino):
            self._error = FileLockCompromisedError(f"Lock ownership changed: {self.path}")
            raise self._error
        self._stat = updated

    async def _heartbeat(self, scope: anyio.CancelScope, update: float, stale: float) -> None:
        last_update = anyio.current_time()
        delay = update
        while True:
            await anyio.sleep(delay)
            try:
                self._refresh()
            except FileLockCompromisedError:
                scope.cancel()
                return
            except OSError as exc:
                # proper-lockfile retries transient I/O failures every second,
                # but aborts once the last successful heartbeat is too old.
                if (
                    isinstance(exc, FileNotFoundError)
                    or anyio.current_time() - last_update >= stale
                ):
                    self._error = FileLockCompromisedError(f"Cannot update lock: {self.path}")
                    self._error.__cause__ = exc
                    scope.cancel()
                    return
                delay = 1
            else:
                last_update = anyio.current_time()
                delay = update

    def _release(self) -> None:
        # A compromised owner must never unlink its successor's directory.
        self.check()
        self.path.rmdir()


def _try_acquire(path: Path, stale: float) -> FileLock | None:
    try:
        path.mkdir()
    except FileExistsError:
        try:
            observed = path.stat()
            if observed.st_mtime >= time.time() - stale:
                return None
            # Revalidate after observing expiry. Another waiter may already
            # have replaced the stale directory, or its owner renewed it.
            if _identity(path.stat()) != _identity(observed):
                return None
            path.rmdir()
        except FileNotFoundError:
            return None
        try:
            path.mkdir()
        except FileExistsError:
            return None

    # Match proper-lockfile's precision probe: ceil to seconds plus 5ms.
    # Filesystems that round to seconds need rounded heartbeat timestamps too.
    acquired = path.stat()
    try:
        timestamp = ((time.time_ns() // 1_000_000 + 999) // 1000 * 1000 + 5) * 1_000_000
        os.utime(path, ns=(timestamp, timestamp))
        current = path.stat()
        if (current.st_dev, current.st_ino) != (acquired.st_dev, acquired.st_ino):
            raise FileLockCompromisedError(f"Lock ownership changed: {path}")
        return FileLock(path, current, current.st_mtime_ns % 1_000_000_000 == 0)
    except BaseException:
        # Do not remove a replacement if acquisition lost ownership.
        try:
            current = path.stat()
            if (current.st_dev, current.st_ino) == (acquired.st_dev, acquired.st_ino):
                path.rmdir()
        except FileNotFoundError:
            pass
        raise


@contextlib.asynccontextmanager
async def file_lock(
    path: Path,
    *,
    stale: float = 30,
    update: float = 1,
    timeout: float = 35,
    retry_interval: float = 0.1,
) -> AsyncIterator[FileLock]:
    """Lease ``<path>.lock`` without requiring the target file to exist.

    Matches world-local's proper-lockfile options. Stale is at least two
    seconds; heartbeats are between one second and half the stale period.
    The parent directory must exist. A lost lease cancels the body and raises
    FileLockCompromisedError, without deleting another owner's directory.
    """
    if not all(math.isfinite(value) for value in (stale, update, timeout, retry_interval)):
        raise ValueError("File lock timings must be finite")
    if stale <= 0 or update <= 0 or timeout < 0 or retry_interval <= 0:
        raise ValueError("File lock timings must be positive (timeout may be zero)")
    stale = max(stale, 2)
    update = max(min(update, stale / 2), 1)
    lock_path = Path(f"{os.path.abspath(path)}.lock")
    deadline = anyio.current_time() + timeout
    while True:
        try:
            lease = _try_acquire(lock_path, stale)
        except OSError as exc:
            raise FileLockError(f"Cannot acquire lock: {lock_path}") from exc
        if lease is not None:
            break
        remaining = deadline - anyio.current_time()
        if remaining <= 0:
            raise FileLockTimeoutError(f"Timed out acquiring lock: {lock_path}")
        await anyio.sleep(min(retry_interval, remaining))

    error: BaseException | None = None
    try:
        async with anyio.create_task_group() as group:
            with anyio.CancelScope() as scope:
                group.start_soon(lease._heartbeat, scope, update, stale)
                try:
                    yield lease
                except BaseException as exc:
                    # Propagate the original exception outside the task group,
                    # rather than wrapping the caller's error in ExceptionGroup.
                    error = exc
                finally:
                    group.cancel_scope.cancel()
    finally:
        try:
            lease._release()
        except FileLockCompromisedError as exc:
            if error is None or isinstance(error, anyio.get_cancelled_exc_class()):
                error = exc
        except OSError as exc:
            if error is None:
                error = FileLockError(f"Cannot release lock: {lock_path}")
                error.__cause__ = exc
    if lease._error is not None and (
        error is None or isinstance(error, anyio.get_cancelled_exc_class())
    ):
        raise lease._error
    if error is not None:
        raise error
