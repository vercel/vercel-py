from __future__ import annotations

from typing import Any


class FatalError(Exception):
    """A step failure that will not be retried."""

    fatal = True
    """Read by the TypeScript SDK's ``FatalError.is()``, which accepts either
    this attribute or the class name -- so an error that crosses the event log
    stays fatal even where the class itself cannot be rebuilt."""


class RemoteError(Exception):
    """An error a peer raised whose class this side does not have.

    JavaScript's ``URIError`` has no Python counterpart, and neither does an
    ``Error`` subclass an app declares only in TypeScript. Rather than flatten
    those to a plain ``Exception`` and lose what they were, they arrive here
    with the class name the writer used, and go back out under that same name
    -- so a value that passes through a Python run reaches the next JavaScript
    reader unchanged.
    """

    def __init__(self, message: str, *, name: str) -> None:
        super().__init__(message)
        self.name = name
        self.message = message
        """The message on its own. ``str()`` prefixes the name it stands in for,
        which is what a traceback should show and not what goes back on the
        wire."""

    def __str__(self) -> str:
        return f"{self.name}: {self.message}" if self.message else self.name


class WorkflowRunFailedError(Exception):
    """A run this process was waiting on failed.

    ``__cause__`` is what the run actually raised, decoded from the
    ``run_failed`` event -- so the workflow's own exception, with its cause
    chain, is what a caller inspects. ``error_code`` is the plaintext category
    the run row carries beside it (``USER_ERROR``, ``WORLD_CONTRACT_ERROR``,
    ...), readable without decrypting anything.
    """

    def __init__(self, run_id: str, error: Any, *, error_code: str | None = None) -> None:
        message = str(error) if error is not None else "unknown error"
        super().__init__(f'Workflow run "{run_id}" failed: {message}')
        self.run_id = run_id
        self.error_code = error_code
        if isinstance(error, BaseException):
            self.__cause__ = error
