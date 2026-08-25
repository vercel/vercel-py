"""Live basic Blob lifecycle verification."""

from __future__ import annotations

import os
from uuid import uuid4

import pytest

from vercel import blob
from vercel.api import session

pytestmark = [pytest.mark.asyncio, pytest.mark.live]


def _require_credentials() -> None:
    has_read_write_token = bool(
        os.getenv("BLOB_READ_WRITE_TOKEN") or os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN")
    )
    has_oidc = bool(os.getenv("VERCEL_OIDC_TOKEN") and os.getenv("BLOB_STORE_ID"))
    if not (has_read_write_token or has_oidc):
        pytest.skip("requires BLOB_READ_WRITE_TOKEN, or VERCEL_OIDC_TOKEN with BLOB_STORE_ID")


async def test_basic_text_and_binary_lifecycle() -> None:
    _require_credentials()
    prefix = f"vercel-py-live/blob-basic/{uuid4().hex}"
    text_path = f"{prefix}/note.txt"
    binary_path = f"{prefix}/payload.bin"

    async with session():
        failure: BaseException | None = None
        try:
            async with blob.open(text_path, "w", content_type="text/plain") as writer:
                await writer.write("hello, Blob!\n")
            async with blob.open(binary_path, "wb") as writer:
                await writer.write(bytes(range(64)))

            assert (await blob.stat(text_path)).size == len(b"hello, Blob!\n")
            assert (await blob.stat(binary_path)).size == 64
            async with blob.open(text_path) as reader:
                assert await reader.read() == "hello, Blob!\n"
            async with blob.open(binary_path, "rb") as reader:
                assert await reader.read() == bytes(range(64))
        except BaseException as error:
            failure = error
            raise
        finally:
            cleanup_errors: list[BaseException] = []
            for pathname in (text_path, binary_path):
                try:
                    await blob.remove(pathname, missing_ok=True)
                except BaseException as error:
                    cleanup_errors.append(error)
            if failure is None and cleanup_errors:
                raise cleanup_errors[0]
