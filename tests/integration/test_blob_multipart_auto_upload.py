"""Integration tests for multipart auto upload using respx."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from vercel.blob.multipart import auto_multipart_upload, auto_multipart_upload_async
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
