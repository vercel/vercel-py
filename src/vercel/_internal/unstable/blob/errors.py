"""Errors for the experimental Blob SDK surface."""

from collections.abc import Iterable

from vercel._internal.blob.errors import (
    BlobAccessError,
    BlobClientTokenExpiredError,
    BlobContentTypeNotAllowedError,
    BlobError,
    BlobFileTooLargeError,
    BlobInvalidResponseJSONError,
    BlobNotFoundError,
    BlobNoTokenProvidedError,
    BlobPathnameMismatchError,
    BlobRequestAbortedError,
    BlobServiceNotAvailable,
    BlobServiceRateLimited,
    BlobStoreNotFoundError,
    BlobStoreSuspendedError,
    BlobUnexpectedResponseContentTypeError,
    BlobUnknownError,
)


class BlobPreconditionFailedError(BlobError):
    """Raised when an ETag precondition fails."""


class BlobAlreadyExistsError(BlobError):
    """Raised when exclusive creation finds an existing pathname."""


class BlobCredentialsError(BlobError):
    """Raised when Blob credentials are missing or malformed."""


class BlobStreamError(BlobError):
    """Raised when a Blob delivery response is invalid or truncated."""


class BlobRecursiveDeleteError(BlobError):
    """Raised after a recursive delete partially or completely fails."""

    def __init__(
        self,
        prefix: str,
        *,
        attempted: int,
        successful: int,
        failures: Iterable[BaseException],
    ) -> None:
        normalized_failures = tuple(failures)
        super().__init__(
            f"Recursive delete of {prefix!r} failed after "
            f"{successful} of {attempted} attempted deletions succeeded"
        )
        self.prefix = prefix
        self.attempted = attempted
        self.successful = successful
        self.failures = normalized_failures


__all__ = (
    "BlobAccessError",
    "BlobAlreadyExistsError",
    "BlobClientTokenExpiredError",
    "BlobContentTypeNotAllowedError",
    "BlobCredentialsError",
    "BlobError",
    "BlobFileTooLargeError",
    "BlobInvalidResponseJSONError",
    "BlobNoTokenProvidedError",
    "BlobNotFoundError",
    "BlobPathnameMismatchError",
    "BlobPreconditionFailedError",
    "BlobRecursiveDeleteError",
    "BlobRequestAbortedError",
    "BlobServiceNotAvailable",
    "BlobServiceRateLimited",
    "BlobStoreNotFoundError",
    "BlobStoreSuspendedError",
    "BlobStreamError",
    "BlobUnexpectedResponseContentTypeError",
    "BlobUnknownError",
)
