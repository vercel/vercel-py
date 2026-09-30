"""Public package import and namespace ownership tests."""


def test_blob_packages_import() -> None:
    import vercel.blob as blob
    import vercel.blob.sync as sync_blob

    assert blob.sync is sync_blob
    assert blob.__package__ == "vercel.blob"
    assert sync_blob.__package__ == "vercel.blob.sync"

    expected_common = {
        "put",
        "get",
        "head",
        "delete",
        "PutResult",
        "HeadResult",
        "DownloadMetadata",
        "BlobCredentials",
        "BlobCredentialsFactory",
        "CredentialKind",
        "BlobError",
        "BlobAccessError",
        "BlobContentTypeNotAllowedError",
        "BlobCredentialsError",
        "BlobFileTooLargeError",
        "BlobNotFoundError",
        "BlobPathnameMismatchError",
        "BlobPreconditionFailedError",
        "BlobServiceNotAvailable",
        "BlobServiceRateLimited",
        "BlobStoreNotFoundError",
        "BlobStoreSuspendedError",
        "BlobStreamError",
        "BlobUnknownError",
    }

    for module in (blob, sync_blob):
        for name in expected_common:
            assert hasattr(module, name), f"{name} missing from {module.__name__}"

    assert hasattr(blob, "BlobServiceOptions")
    assert hasattr(sync_blob, "SyncBlobServiceOptions")
    assert expected_common.issubset(set(blob.__all__))
    assert "BlobServiceOptions" in blob.__all__
    assert "sync" in blob.__all__
    assert "AsyncDownloadContext" not in blob.__all__

    assert expected_common.issubset(set(sync_blob.__all__))
    assert "SyncBlobServiceOptions" in sync_blob.__all__
    assert "SyncDownloadContext" not in sync_blob.__all__
