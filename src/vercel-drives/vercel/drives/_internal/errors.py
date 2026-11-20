"""Drive errors for the SDK surface."""

import httpx2 as httpx

from vercel._internal.core.errors import VercelError


class DriveError(VercelError):
    """Base error for Drive operations."""


class DriveApiError(DriveError):
    """Raised when the Drives API returns an error response.

    Attributes:
        response: The raw httpx2 response.
        status_code: HTTP status code, such as 404 for a missing Drive.
        code: Machine-readable error code from the API, when present.
        data: The parsed error body, when it was JSON.
    """

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


class DriveResponseError(DriveError):
    """Raised when a successful Drives API response is malformed."""

    def __init__(self, message: str, *, data: object | None = None) -> None:
        super().__init__(message)
        self.data = data


class DriveCredentialsError(DriveError):
    """Raised when Drive credentials cannot be resolved."""


def _extract_api_error_code(data: object | None) -> str | None:
    if not isinstance(data, dict):
        return None
    error = data.get("error")
    if not isinstance(error, dict):
        return None
    code = error.get("code")
    return code if isinstance(code, str) else None


__all__ = ["DriveApiError", "DriveCredentialsError", "DriveError", "DriveResponseError"]
