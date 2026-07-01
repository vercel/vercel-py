"""Live verification for the pathname-oriented unstable Blob API.

Run with::

    VERCEL_OIDC_TOKEN=$(vercel project token) ./scripts/test.sh -q \
        tests/live/test_unstable_blob_live.py

OIDC requires ``BLOB_STORE_ID``. A ``BLOB_READ_WRITE_TOKEN`` may be used
instead. Every test operates below a fresh prefix and removes that prefix in a
``finally`` block.
"""

import os
from collections.abc import AsyncGenerator
from typing import Literal
from uuid import uuid4

import httpx
import pytest

from vercel import unstable as vercel
from vercel._internal.blob import extract_store_id_from_token
from vercel._internal.unstable.blob.errors import (
    BlobAlreadyExistsError,
    BlobError,
    BlobNotFoundError,
    BlobPreconditionFailedError,
)
from vercel.oidc import VercelOidcTokenError, get_vercel_oidc_token_sync
from vercel.unstable import blob
from vercel.unstable.blob import (
    PresignedOperation,
    ScandirMode,
)

pytestmark = [pytest.mark.live, pytest.mark.asyncio]

_PRIVATE_STORE_UNSUPPORTED = (
    "Vercel Blob: Cannot use private access on a public store. "
    "The store must be configured with private access."
)
_PRESIGNED_ROLLOUT_DISABLED = "Presigned URL auth is not enabled for this owner"
_SECRET_EXPOSURE_MESSAGE = "configured credential exposed in presigned request"


@pytest.fixture(autouse=True)
async def _isolated_sdk_session() -> AsyncGenerator[None, None]:
    """Keep each asyncio test's transport on that test's event loop."""
    async with vercel.session():
        yield


def _oidc_token() -> str | None:
    try:
        return get_vercel_oidc_token_sync()
    except VercelOidcTokenError:
        return None


def _require_blob_credentials() -> None:
    has_read_write_token = bool(
        os.getenv("BLOB_READ_WRITE_TOKEN") or os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN")
    )
    has_oidc = bool(_oidc_token() and os.getenv("BLOB_STORE_ID"))
    if not (has_read_write_token or has_oidc):
        pytest.skip(
            "requires BLOB_READ_WRITE_TOKEN, or VERCEL_OIDC_TOKEN paired with BLOB_STORE_ID"
        )


def _prefix() -> str:
    return f"vercel-py-live/unstable-blob/{uuid4().hex}/"


def _expected_store_id() -> str:
    store_id = os.getenv("BLOB_STORE_ID")
    if _oidc_token() and store_id:
        return store_id.removeprefix("store_")

    token = os.getenv("BLOB_READ_WRITE_TOKEN") or os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN")
    assert token is not None
    return extract_store_id_from_token(token).removeprefix("store_")


def _configured_credential_values() -> set[str]:
    values = {
        os.getenv("BLOB_READ_WRITE_TOKEN"),
        os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN"),
        os.getenv("VERCEL_OIDC_TOKEN"),
        _oidc_token(),
    }
    return {value for value in values if value}


def _assert_credentials_not_exposed(request: httpx.Request) -> None:
    if "authorization" in request.headers:
        raise AssertionError(_SECRET_EXPOSURE_MESSAGE) from None
    exposed_text = [
        str(request.url),
        *request.headers.keys(),
        *request.headers.values(),
    ]
    for credential in _configured_credential_values():
        if any(credential in value for value in exposed_text):
            raise AssertionError(_SECRET_EXPOSURE_MESSAGE) from None
        if credential.encode() in request.content:
            raise AssertionError(_SECRET_EXPOSURE_MESSAGE) from None


def _is_private_store_unsupported(error: BlobError) -> bool:
    return type(error) is BlobError and str(error) == _PRIVATE_STORE_UNSUPPORTED


def _is_presigned_rollout_disabled(response: httpx.Response) -> bool:
    if response.status_code != 403:
        return False
    try:
        body = response.json()
    except ValueError:
        return False
    return body == {
        "error": {
            "code": "forbidden",
            "message": _PRESIGNED_ROLLOUT_DISABLED,
        }
    }


async def _write(
    pathname: str,
    data: str | bytes,
    *,
    access: Literal["public", "private"] = "public",
) -> None:
    if isinstance(data, bytes):
        async with blob.open(pathname, "wb", access=access) as target:
            await target.write(data)
    else:
        async with blob.open(pathname, "w", access=access) as target:
            await target.write(data)


async def _read_text(
    pathname: str,
    *,
    access: Literal["public", "private"] = "public",
) -> str:
    async with blob.open(pathname, access=access) as source:
        return await source.read()


@pytest.mark.parametrize("access", ["public"])
async def test_unstable_blob_file_and_tree_contract(access: Literal["public"]) -> None:
    _require_blob_credentials()
    root = _prefix()
    text_path = f"{root}note.txt"
    binary_path = f"{root}range.bin"
    nested_path = f"{root}nested/child.txt"
    deep_path = f"{root}nested/deep/value.txt"
    marker_child = f"{root}materialized/child.txt"
    marker_path = f"{root}materialized/"
    exclusive_path = f"{root}exclusive.txt"
    abort_path = f"{root}abort.txt"
    update_path = f"{root}update.txt"
    conflict_path = f"{root}conflict.txt"
    removed_tree = f"{root}removed/"

    failure: BaseException | None = None
    try:
        await _write(text_path, "hello, Blob!\n", access=access)
        assert await _read_text(text_path) == "hello, Blob!\n"
        text_stat = await blob.stat(text_path)
        assert text_stat.pathname == text_path
        assert text_stat.size == len(b"hello, Blob!\n")

        binary = bytes(range(64))
        await _write(binary_path, binary, access=access)
        async with blob.open(binary_path, "rb") as source:
            assert await source.read(7) == binary[:7]
            assert await source.seek(40) == 40
            assert await source.read(8) == binary[40:48]
            assert await source.seek(-5, 1) == 43
            assert await source.read(4) == binary[43:47]
            assert await source.seek(3) == 3
            assert await source.read(5) == binary[3:8]

        await _write(nested_path, "nested", access=access)
        await _write(deep_path, "deep", access=access)
        folded = [entry async for entry in blob.scandir(root)]
        assert {entry.path for entry in folded if entry.is_object()} >= {
            text_path,
            binary_path,
        }
        assert {entry.path for entry in folded if entry.is_prefix()} == {f"{root}nested/"}

        expanded = [entry async for entry in blob.scandir(root, mode=ScandirMode.EXPANDED)]
        expanded_paths = {entry.path for entry in expanded}
        assert {text_path, binary_path, nested_path, deep_path} <= expanded_paths
        assert all(entry.is_object() for entry in expanded)

        await _write(marker_child, "already below marker", access=access)
        await blob.mkdir(marker_path, access=access)
        assert (await blob.stat(marker_path)).size == 0
        await blob.mkdir(marker_path, access=access, exist_ok=True)

        await _write(exclusive_path, "original", access=access)
        with pytest.raises(BlobAlreadyExistsError):
            async with blob.open(exclusive_path, "x", access=access) as target:
                await target.write("replacement")
        assert await _read_text(exclusive_path) == "original"

        await _write(abort_path, "before", access=access)
        with pytest.raises(RuntimeError, match="abort publication"):
            async with blob.open(abort_path, "w", access=access) as target:
                await target.write("after")
                raise RuntimeError("abort publication")
        assert await _read_text(abort_path) == "before"

        await _write(update_path, "abcdef", access=access)
        async with blob.open(update_path, "r+", access=access) as target:
            await target.seek(2)
            await target.write("XY")
        assert await _read_text(update_path) == "abXYef"

        await _write(conflict_path, "first", access=access)
        stale = await blob.open(conflict_path, "r+", access=access)
        await stale.seek(0)
        await stale.write("stale")
        await _write(conflict_path, "concurrent", access=access)
        with pytest.raises(BlobPreconditionFailedError):
            await stale.close()
        assert await _read_text(conflict_path) == "concurrent"

        await blob.mkdir(removed_tree, access=access)
        await _write(f"{removed_tree}one.txt", "one", access=access)
        await _write(f"{removed_tree}deeper/two.txt", "two", access=access)
        await blob.rmtree(removed_tree)
        for pathname in (
            removed_tree,
            f"{removed_tree}one.txt",
            f"{removed_tree}deeper/two.txt",
        ):
            with pytest.raises(BlobNotFoundError):
                await blob.stat(pathname)
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            await blob.rmtree(root, missing_ok=True)
        except BlobError:
            if failure is None:
                raise


async def test_default_credentials_target_configured_store() -> None:
    _require_blob_credentials()
    root = _prefix()
    pathname = f"{root}default-credentials.txt"
    failure: BaseException | None = None
    try:
        await _write(pathname, "default credentials")
        result = await blob.stat(pathname)
        assert result.url.startswith(f"https://{_expected_store_id()}.")
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            await blob.rmtree(root, missing_ok=True)
        except BlobError:
            if failure is None:
                raise


async def test_private_bearer_read() -> None:
    _require_blob_credentials()
    root = _prefix()
    private_path = f"{root}private.txt"
    failure: BaseException | None = None
    try:
        try:
            await _write(private_path, "private contents", access="private")
        except BlobError as error:
            if _is_private_store_unsupported(error):
                pytest.skip("configured live Blob store is explicitly public-only")
            raise

        private_stat = await blob.stat(private_path)
        assert ".private.blob.vercel-storage.com/" in private_stat.url
        assert await _read_text(private_path, access="private") == "private contents"
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            await blob.rmtree(root, missing_ok=True)
        except BlobError:
            if failure is None:
                raise


async def test_presigned_private_get() -> None:
    _require_blob_credentials()
    root = _prefix()
    private_path = f"{root}private-presigned.txt"
    failure: BaseException | None = None
    try:
        try:
            await _write(private_path, "private presigned contents", access="private")
        except BlobError as error:
            if _is_private_store_unsupported(error):
                pytest.skip("configured live Blob store is explicitly public-only")
            raise

        assert await _read_text(private_path, access="private") == "private presigned contents"
        get_url = await blob.presign(
            private_path,
            operation=PresignedOperation.GET,
            access="private",
        )
        async with httpx.AsyncClient() as client:
            get_request = client.build_request(
                "GET",
                get_url.url,
                headers=dict(get_url.required_headers),
            )
            _assert_credentials_not_exposed(get_request)
            get_response = await client.send(get_request)
            if _is_presigned_rollout_disabled(get_response):
                pytest.skip("presigned URL auth rollout is explicitly disabled for this owner")
            get_response.raise_for_status()
            assert get_response.text == "private presigned contents"
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            await blob.rmtree(root, missing_ok=True)
        except BlobError:
            if failure is None:
                raise


async def test_presigned_put() -> None:
    _require_blob_credentials()
    root = _prefix()
    uploaded_path = f"{root}presigned-upload.txt"
    failure: BaseException | None = None
    try:
        put_url = await blob.presign(
            uploaded_path,
            operation=PresignedOperation.PUT,
            access="public",
            allowed_content_types=("text/plain",),
            maximum_size=64,
            allow_overwrite=False,
        )

        async with httpx.AsyncClient() as client:
            body = b"uploaded without SDK credentials"
            put_headers = {
                **put_url.required_headers,
                "x-content-type": "text/plain",
            }
            put_request = client.build_request(
                "PUT",
                put_url.url,
                content=body,
                headers=put_headers,
            )
            _assert_credentials_not_exposed(put_request)
            put_response = await client.send(put_request)
            if _is_presigned_rollout_disabled(put_response):
                pytest.skip("presigned URL auth rollout is explicitly disabled for this owner")
            put_response.raise_for_status()

        assert await _read_text(uploaded_path) == "uploaded without SDK credentials"
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            await blob.rmtree(root, missing_ok=True)
        except BlobError:
            if failure is None:
                raise
