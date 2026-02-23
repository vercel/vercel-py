"""Integration tests for multipart auto upload using respx."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest
import respx

from vercel.blob.multipart import (
    AsyncMultipartUploader,
    MultipartUploader,
    auto_multipart_upload,
    auto_multipart_upload_async,
    complete_multipart_upload,
    complete_multipart_upload_async,
    create_multipart_upload,
    create_multipart_upload_async,
    create_multipart_uploader,
    create_multipart_uploader_async,
    upload_part,
    upload_part_async,
)
from vercel.blob.multipart.uploader import MIN_PART_SIZE
from vercel.blob.utils import UploadProgressEvent

BLOB_API_BASE = "https://vercel.com/api/blob"


def _build_complete_response(pathname: str) -> dict[str, str]:
    return {
        "url": f"https://blob.vercel-storage.com/test-abc123/{pathname}",
        "downloadUrl": f"https://blob.vercel-storage.com/test-abc123/{pathname}?download=1",
        "pathname": pathname,
        "contentType": "application/octet-stream",
        "contentDisposition": 'inline; filename="file.bin"',
    }


def _manual_mpu_handler(
    pathname: str,
) -> tuple[Callable[[httpx.Request], httpx.Response], dict[str, Any]]:
    upload_part_numbers: list[int] = []
    completed_parts: list[dict[str, str | int]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        action = request.headers["x-mpu-action"]

        if action == "create":
            assert request.url.params["pathname"] == pathname
            return httpx.Response(200, json={"uploadId": "upload-id", "key": "blob-key"})

        if action == "upload":
            part_number = int(request.headers["x-mpu-part-number"])
            upload_part_numbers.append(part_number)
            assert request.headers["x-mpu-upload-id"] == "upload-id"
            assert request.headers["x-mpu-key"] == "blob-key"
            return httpx.Response(200, json={"etag": f"etag-{part_number}"})

        if action == "complete":
            completed_parts.extend(json.loads(request.content.decode()))
            return httpx.Response(200, json=_build_complete_response(pathname))

        raise AssertionError(f"unexpected multipart action: {action}")

    state = {
        "upload_part_numbers": upload_part_numbers,
        "completed_parts": completed_parts,
    }
    return handler, state


@respx.mock
def test_auto_multipart_upload_sync_uses_blob_api_flow(mock_env_clear) -> None:
    upload_part_numbers: list[int] = []
    upload_part_lengths: list[int] = []
    completed_parts: list[dict[str, str | int]] = []
    progress_events: list[UploadProgressEvent] = []

    def mpu_handler(request: httpx.Request) -> httpx.Response:
        action = request.headers["x-mpu-action"]

        if action == "create":
            assert request.url.params["pathname"] == "folder/file.bin"
            return httpx.Response(200, json={"uploadId": "upload-id", "key": "blob-key"})

        if action == "upload":
            part_number = int(request.headers["x-mpu-part-number"])
            upload_part_numbers.append(part_number)
            upload_part_lengths.append(len(request.content))
            assert request.headers["x-mpu-upload-id"] == "upload-id"
            assert request.headers["x-mpu-key"] == "blob-key"
            return httpx.Response(200, json={"etag": f"etag-{part_number}"})

        if action == "complete":
            completed_parts.extend(json.loads(request.content.decode()))
            return httpx.Response(200, json=_build_complete_response("folder/file.bin"))

        raise AssertionError(f"unexpected multipart action: {action}")

    route = respx.post(f"{BLOB_API_BASE}/mpu").mock(side_effect=mpu_handler)

    body = (b"a" * MIN_PART_SIZE) + b"b"
    result = auto_multipart_upload(
        "folder/file.bin",
        body,
        token="test_token",
        part_size=MIN_PART_SIZE,
        on_upload_progress=progress_events.append,
    )

    assert route.call_count == 4
    assert sorted(upload_part_numbers) == [1, 2]
    assert sorted(upload_part_lengths) == [1, MIN_PART_SIZE]
    assert [part["partNumber"] for part in completed_parts] == [1, 2]
    assert result["pathname"] == "folder/file.bin"
    assert isinstance(progress_events[-1], UploadProgressEvent)
    assert progress_events[-1] == UploadProgressEvent(
        loaded=len(body),
        total=len(body),
        percentage=100.0,
    )


@respx.mock
def test_manual_multipart_sync_uses_blob_api_flow(mock_env_clear) -> None:
    handler, state = _manual_mpu_handler("folder/manual.bin")
    route = respx.post(f"{BLOB_API_BASE}/mpu").mock(side_effect=handler)

    created = create_multipart_upload("folder/manual.bin", token="test_token")
    part = upload_part(
        "folder/manual.bin",
        b"chunk",
        token="test_token",
        upload_id=created.upload_id,
        key=created.key,
        part_number=1,
    )
    result = complete_multipart_upload(
        "folder/manual.bin",
        [part],
        token="test_token",
        upload_id=created.upload_id,
        key=created.key,
    )

    assert route.call_count == 3
    assert state["upload_part_numbers"] == [1]
    assert [part["partNumber"] for part in state["completed_parts"]] == [1]
    assert result.pathname == "folder/manual.bin"


@respx.mock
@pytest.mark.asyncio
async def test_auto_multipart_upload_async_uses_blob_api_flow(mock_env_clear) -> None:
    upload_part_numbers: list[int] = []
    upload_part_lengths: list[int] = []
    completed_parts: list[dict[str, str | int]] = []
    progress_events: list[UploadProgressEvent] = []

    def mpu_handler(request: httpx.Request) -> httpx.Response:
        action = request.headers["x-mpu-action"]

        if action == "create":
            assert request.url.params["pathname"] == "folder/file.bin"
            return httpx.Response(200, json={"uploadId": "upload-id", "key": "blob-key"})

        if action == "upload":
            part_number = int(request.headers["x-mpu-part-number"])
            upload_part_numbers.append(part_number)
            upload_part_lengths.append(len(request.content))
            assert request.headers["x-mpu-upload-id"] == "upload-id"
            assert request.headers["x-mpu-key"] == "blob-key"
            return httpx.Response(200, json={"etag": f"etag-{part_number}"})

        if action == "complete":
            completed_parts.extend(json.loads(request.content.decode()))
            return httpx.Response(200, json=_build_complete_response("folder/file.bin"))

        raise AssertionError(f"unexpected multipart action: {action}")

    route = respx.post(f"{BLOB_API_BASE}/mpu").mock(side_effect=mpu_handler)

    body = (b"a" * MIN_PART_SIZE) + b"b"

    async def on_progress(event: UploadProgressEvent) -> None:
        progress_events.append(event)

    result = await auto_multipart_upload_async(
        "folder/file.bin",
        body,
        token="test_token",
        part_size=MIN_PART_SIZE,
        on_upload_progress=on_progress,
    )

    assert route.call_count == 4
    assert sorted(upload_part_numbers) == [1, 2]
    assert sorted(upload_part_lengths) == [1, MIN_PART_SIZE]
    assert [part["partNumber"] for part in completed_parts] == [1, 2]
    assert result["pathname"] == "folder/file.bin"
    assert isinstance(progress_events[-1], UploadProgressEvent)
    assert progress_events[-1] == UploadProgressEvent(
        loaded=len(body),
        total=len(body),
        percentage=100.0,
    )


@respx.mock
@pytest.mark.asyncio
async def test_auto_multipart_upload_async_unknown_total_reports_loaded_bytes(
    mock_env_clear,
) -> None:
    upload_part_numbers: list[int] = []
    progress_events: list[UploadProgressEvent] = []

    def mpu_handler(request: httpx.Request) -> httpx.Response:
        action = request.headers["x-mpu-action"]

        if action == "create":
            assert request.url.params["pathname"] == "folder/unknown-total.bin"
            return httpx.Response(200, json={"uploadId": "upload-id", "key": "blob-key"})

        if action == "upload":
            part_number = int(request.headers["x-mpu-part-number"])
            upload_part_numbers.append(part_number)
            return httpx.Response(200, json={"etag": f"etag-{part_number}"})

        if action == "complete":
            return httpx.Response(200, json=_build_complete_response("folder/unknown-total.bin"))

        raise AssertionError(f"unexpected multipart action: {action}")

    route = respx.post(f"{BLOB_API_BASE}/mpu").mock(side_effect=mpu_handler)

    chunk_one = b"a" * (MIN_PART_SIZE // 2)
    chunk_two = b"b"

    async def async_chunks():
        yield chunk_one
        yield chunk_two

    async def on_progress(event: UploadProgressEvent) -> None:
        progress_events.append(event)

    result = await auto_multipart_upload_async(
        "folder/unknown-total.bin",
        async_chunks(),
        token="test_token",
        part_size=MIN_PART_SIZE,
        on_upload_progress=on_progress,
    )

    assert route.call_count == 3
    assert upload_part_numbers == [1]
    assert result["pathname"] == "folder/unknown-total.bin"
    assert progress_events[-1] == UploadProgressEvent(
        loaded=len(chunk_one) + len(chunk_two),
        total=0,
        percentage=100.0,
    )


@respx.mock
@pytest.mark.asyncio
async def test_manual_multipart_async_uses_blob_api_flow(mock_env_clear) -> None:
    handler, state = _manual_mpu_handler("folder/manual-async.bin")
    route = respx.post(f"{BLOB_API_BASE}/mpu").mock(side_effect=handler)

    created = await create_multipart_upload_async("folder/manual-async.bin", token="test_token")
    part = await upload_part_async(
        "folder/manual-async.bin",
        b"chunk",
        token="test_token",
        upload_id=created.upload_id,
        key=created.key,
        part_number=1,
    )
    result = await complete_multipart_upload_async(
        "folder/manual-async.bin",
        [part],
        token="test_token",
        upload_id=created.upload_id,
        key=created.key,
    )

    assert route.call_count == 3
    assert state["upload_part_numbers"] == [1]
    assert [part["partNumber"] for part in state["completed_parts"]] == [1]
    assert result.pathname == "folder/manual-async.bin"


@respx.mock
def test_create_multipart_uploader_sync_uses_blob_api_flow(mock_env_clear) -> None:
    handler, state = _manual_mpu_handler("folder/uploader-sync.bin")
    route = respx.post(f"{BLOB_API_BASE}/mpu").mock(side_effect=handler)

    uploader = create_multipart_uploader("folder/uploader-sync.bin", token="test_token")
    assert isinstance(uploader, MultipartUploader)
    assert uploader.upload_id == "upload-id"
    assert uploader.key == "blob-key"

    part = uploader.upload_part(1, b"chunk")
    result = uploader.complete([part])

    assert route.call_count == 3
    assert state["upload_part_numbers"] == [1]
    assert [part["partNumber"] for part in state["completed_parts"]] == [1]
    assert result.pathname == "folder/uploader-sync.bin"


@respx.mock
@pytest.mark.asyncio
async def test_create_multipart_uploader_async_uses_blob_api_flow(mock_env_clear) -> None:
    handler, state = _manual_mpu_handler("folder/uploader-async.bin")
    route = respx.post(f"{BLOB_API_BASE}/mpu").mock(side_effect=handler)

    uploader = await create_multipart_uploader_async(
        "folder/uploader-async.bin", token="test_token"
    )
    assert isinstance(uploader, AsyncMultipartUploader)
    assert uploader.upload_id == "upload-id"
    assert uploader.key == "blob-key"

    part = await uploader.upload_part(1, b"chunk")
    result = await uploader.complete([part])

    assert route.call_count == 3
    assert state["upload_part_numbers"] == [1]
    assert [part["partNumber"] for part in state["completed_parts"]] == [1]
    assert result.pathname == "folder/uploader-async.bin"
