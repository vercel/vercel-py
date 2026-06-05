"""Sandbox errors for the experimental SDK surface."""

import httpx

from vercel._internal.unstable.errors import VercelError
from vercel._internal.unstable.sandbox.models import SandboxStatus


class SandboxError(VercelError):
    """Base error for unstable Sandbox operations."""


class SandboxInvalidHandleError(SandboxError):
    """Raised when a Sandbox handle is unattached or mode-invalid."""


class SandboxTerminalStateError(SandboxError):
    """Raised when Sandbox creation reaches a terminal state."""

    def __init__(
        self,
        message: str,
        *,
        status: SandboxStatus,
        sandbox: object | None = None,
    ) -> None:
        super().__init__(message)
        self.status: SandboxStatus = status
        self.sandbox = sandbox


class SandboxCleanupError(SandboxError):
    """Raised when context-managed Sandbox resource cleanup fails."""

    def __init__(
        self,
        message: str,
        *,
        resource_type: str,
        resource_id: str,
        cause: BaseException,
    ) -> None:
        super().__init__(message)
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.cause = cause


class SandboxApiError(SandboxError):
    """Raised when the Sandbox v2 API returns an error response."""

    def __init__(
        self,
        response: httpx.Response,
        message: str,
        *,
        data: object | None = None,
    ) -> None:
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code
        self.data = data
        self.code = _extract_api_error_code(data)


class SandboxResponseError(SandboxError):
    """Raised when a successful Sandbox v2 API response is malformed."""

    def __init__(self, message: str, *, data: object | None = None) -> None:
        super().__init__(message)
        self.data = data


class SandboxCredentialsError(SandboxError):
    """Raised when Sandbox credentials cannot be resolved."""


class SandboxStreamError(SandboxError):
    """Raised when a Sandbox log stream reports an in-band failure."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


class SandboxFilesystemError(SandboxError):
    """Base error for filesystem capability operations."""


class SandboxFilesystemCommandError(SandboxFilesystemError):
    """Raised when a command-backed filesystem operation fails."""

    def __init__(
        self,
        operation: str,
        *,
        paths: tuple[str, ...],
        exit_code: int | None,
        stdout: str,
        stderr: str,
    ) -> None:
        super().__init__(f"Sandbox filesystem {operation} failed with exit code {exit_code}")
        self.operation = operation
        self.paths = paths
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr


class SandboxFilesystemWriteError(SandboxFilesystemError):
    """Raised when the native filesystem write endpoint rejects a batch."""

    def __init__(
        self,
        *,
        paths: tuple[str, ...],
        cwd: str,
        cause: SandboxApiError,
    ) -> None:
        super().__init__(f"Sandbox filesystem write failed for {len(paths)} path(s)")
        self.paths = paths
        self.cwd = cwd
        self.cause = cause


class SandboxPathNotFoundError(SandboxFilesystemError):
    """Raised when a native filesystem operation proves a missing path."""

    def __init__(
        self,
        path: str,
        *,
        operation: str,
        cwd: str | None,
        cause: SandboxApiError,
    ) -> None:
        super().__init__(f"Sandbox filesystem path not found: {path!r}")
        self.path = path
        self.operation = operation
        self.cwd = cwd
        self.cause = cause


def _extract_api_error_code(data: object | None) -> str | None:
    if not isinstance(data, dict):
        return None
    error = data.get("error")
    if not isinstance(error, dict):
        return None
    code = error.get("code")
    if not isinstance(code, str):
        return None
    return code
