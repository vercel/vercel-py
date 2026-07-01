from vercel._internal.blob.errors import BlobAccessError
from vercel._internal.unstable.blob.errors import (
    BlobAlreadyExistsError,
    BlobError,
    BlobPreconditionFailedError,
    BlobRecursiveDeleteError,
    BlobStreamError,
)


def test_unstable_blob_errors_share_existing_blob_hierarchy() -> None:
    assert issubclass(BlobAccessError, BlobError)
    assert issubclass(BlobPreconditionFailedError, BlobError)
    assert issubclass(BlobAlreadyExistsError, BlobError)
    assert issubclass(BlobStreamError, BlobError)


def test_recursive_delete_error_records_partial_completion() -> None:
    failures = (RuntimeError("batch failed"), ValueError("listing failed"))

    error = BlobRecursiveDeleteError(
        "reports/",
        attempted=8,
        successful=5,
        failures=failures,
    )

    assert error.prefix == "reports/"
    assert error.attempted == 8
    assert error.successful == 5
    assert error.failures == failures
