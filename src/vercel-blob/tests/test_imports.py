"""Public package import tests."""


def test_blob_packages_import() -> None:
    import vercel.blob as blob
    import vercel.blob.sync as sync_blob

    assert blob.sync is sync_blob
    assert blob.__all__ == ["sync"]
    assert sync_blob.__all__ == []
