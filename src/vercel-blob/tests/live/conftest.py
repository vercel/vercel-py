"""Credential gate shared by live Blob tests and examples."""

import os

import pytest


@pytest.fixture(autouse=True)
def require_blob_credentials() -> None:
    if not (
        os.getenv("BLOB_READ_WRITE_TOKEN")
        or os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN")
        or (os.getenv("VERCEL_OIDC_TOKEN") and os.getenv("BLOB_STORE_ID"))
    ):
        pytest.skip("requires BLOB_READ_WRITE_TOKEN or VERCEL_OIDC_TOKEN with BLOB_STORE_ID")
    if os.getenv("BLOB_TEST_ACCESS", "public") not in ("public", "private"):
        pytest.fail("BLOB_TEST_ACCESS must be public or private")
