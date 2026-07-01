import os
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from vercel._internal.blob.types import Access
from vercel._internal.byte_stream import StagingFileRuntime
from vercel._internal.http import AsyncTransport
from vercel._internal.unstable.blob.api_client import BlobApiClient
from vercel._internal.unstable.blob.errors import (
    BlobNotFoundError,
    BlobRecursiveDeleteError,
    BlobStreamError,
)
from vercel._internal.unstable.blob.models import (
    BlobListItemState,
    BlobPageState,
    BlobPrefixState,
    BlobStatResult,
    PresignedOperation,
    PresignedUrl,
    ScandirMode,
)
from vercel._internal.unstable.blob.options import BlobCredentials, BlobServiceOptions
from vercel._internal.unstable.blob.runtime_common import (
    BlobObjectEntry,
)
from vercel._internal.unstable.blob.service import BlobService

UTC = timezone.utc
NOW = datetime(2026, 1, 2, tzinfo=UTC)


def _item(pathname: str, *, access: Access = "public") -> BlobListItemState:
    return BlobListItemState(
        pathname=pathname,
        url=f"https://store_123.{access}.blob.vercel-storage.com/{pathname}",
        download_url=f"https://store_123.{access}.blob.vercel-storage.com/{pathname}?download=1",
        size=12,
        etag='"etag"',
        uploaded_at=NOW,
    )


def _stat(pathname: str) -> BlobStatResult:
    item = _item(pathname)
    return BlobStatResult(
        pathname=item.pathname,
        url=item.url,
        download_url=item.download_url,
        size=item.size,
        etag=item.etag,
        uploaded_at=item.uploaded_at,
        content_type="text/plain",
        content_disposition="inline",
        cache_control="public, max-age=60",
    )


class FakeApiClient:
    def __init__(self, events: list[str] | None = None) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.pages: list[BlobPageState | BaseException] = []
        self.delete_failures: set[tuple[str, ...]] = set()
        self.stat_result = _stat("fallback.txt")
        self.events = events

    def _request(self, name: str) -> None:
        if self.events is not None:
            self.events.append(name)

    async def stat(self, pathname: str) -> BlobStatResult:
        self._request("stat")
        self.calls.append(("stat", pathname))
        if pathname == "missing":
            raise BlobNotFoundError()
        return BlobStatResult(
            pathname=pathname,
            url=self.stat_result.url,
            download_url=self.stat_result.download_url,
            size=self.stat_result.size,
            etag=self.stat_result.etag,
            uploaded_at=self.stat_result.uploaded_at,
            content_type=self.stat_result.content_type,
            content_disposition=self.stat_result.content_disposition,
            cache_control=self.stat_result.cache_control,
        )

    async def list_page(
        self,
        *,
        prefix: str,
        mode: ScandirMode,
        page_size: int | None,
        cursor: str | None,
    ) -> BlobPageState:
        self._request("list")
        self.calls.append(("list", prefix, mode, page_size, cursor))
        page = self.pages.pop(0)
        if isinstance(page, BaseException):
            raise page
        return page

    async def delete(self, pathname: str, *, if_match: str | None = None) -> None:
        self._request("delete")
        self.calls.append(("delete", pathname, if_match))
        if pathname == "missing":
            raise BlobNotFoundError()

    async def delete_batch(self, pathnames: Sequence[str]) -> None:
        self._request("delete_batch")
        batch = tuple(pathnames)
        self.calls.append(("delete_batch", batch))
        if batch in self.delete_failures:
            raise RuntimeError(f"failed {batch!r}")

    async def create_marker(
        self, pathname: str, *, access: Access, exist_ok: bool
    ) -> BlobStatResult:
        self._request("create_marker")
        self.calls.append(("create_marker", pathname, access, exist_ok))
        return _stat(pathname)

    async def presign(
        self,
        pathname: str,
        *,
        operation: PresignedOperation,
        access: Access,
        expires_at: datetime,
        maximum_size: int | None,
        allowed_content_types: Sequence[str] | None,
        allow_overwrite: bool | None,
        cache_control_max_age: timedelta | None,
        if_match: str | None,
    ) -> PresignedUrl:
        self._request("presign")
        self.calls.append(
            (
                "presign",
                pathname,
                operation,
                access,
                expires_at,
                maximum_size,
                allowed_content_types,
                allow_overwrite,
                cache_control_max_age,
                if_match,
            )
        )
        return PresignedUrl(
            url="https://signed.test",
            operation=operation,
            expires_at=expires_at,
            required_headers={},
        )


def _service(client: FakeApiClient, events: list[str] | None = None) -> BlobService:
    observed = events if events is not None else []
    return BlobService(
        api_client=cast(Any, client),
        options=BlobServiceOptions(),
        ensure_open=lambda: observed.append("open"),
        staging_file_runtime=cast(StagingFileRuntime, object()),
    )


@pytest.mark.anyio
async def test_normalizes_object_paths_once_at_service_boundary() -> None:
    client = FakeApiClient()
    service = _service(client)

    await service.stat(Path("/folder\\object.txt"))
    await service.remove("folder/", missing_ok=False)

    assert client.calls == [
        ("stat", "folder/object.txt"),
        ("delete", "folder/", None),
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "pathname",
    ["", "/", "//", "//object", "https://blob.test/object", "x" * 951],
)
async def test_rejects_invalid_object_paths(pathname: str) -> None:
    client = FakeApiClient()
    service = _service(client)

    with pytest.raises(ValueError):
        await service.stat(pathname)

    assert client.calls == []


@pytest.mark.anyio
async def test_scandir_iterates_pages_without_materializing_or_sorting() -> None:
    client = FakeApiClient()
    client.pages = [
        BlobPageState((_item("z.txt"), BlobPrefixState("folder/")), "next", True),
        BlobPageState((_item("a.txt"),), None, False),
    ]
    service = _service(client)

    iterator = service.scandir(prefix="", mode=ScandirMode.FOLDED, page_size=2)
    first = await anext(iterator)
    calls_after_first = list(client.calls)
    remaining = [entry async for entry in iterator]

    assert first.pathname == "z.txt"
    assert calls_after_first == [("list", "", ScandirMode.FOLDED, 2, None)]
    assert [entry.pathname for entry in remaining] == ["folder/", "a.txt"]
    assert client.calls[-1] == ("list", "", ScandirMode.FOLDED, 2, "next")


@pytest.mark.anyio
async def test_expanded_scandir_rejects_folded_folders_from_api() -> None:
    async def credentials() -> BlobCredentials:
        return BlobCredentials("oidc-token", "store", "oidc")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"blobs": [], "folders": ["folder/"], "hasMore": False},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        api_client = BlobApiClient(
            base_url="https://blob-service.test",
            credentials_factory=credentials,
            transport=AsyncTransport(http),
        )
        service = BlobService(
            api_client=api_client,
            options=BlobServiceOptions(credentials_factory=credentials),
            ensure_open=lambda: None,
            staging_file_runtime=cast(StagingFileRuntime, object()),
        )

        with pytest.raises(BlobStreamError, match="malformed list metadata"):
            async for _ in service.scandir(mode=ScandirMode.EXPANDED):
                pass


@pytest.mark.anyio
@pytest.mark.parametrize("invalid_cursor", [None, ""])
async def test_scandir_rejects_missing_continuation_cursor(
    invalid_cursor: str | None,
) -> None:
    client = FakeApiClient()
    client.pages = [BlobPageState((_item("folder/one"),), invalid_cursor, True)]
    service = _service(client)
    observed = []

    with pytest.raises(BlobStreamError, match="continuation cursor"):
        async for entry in service.scandir(mode=ScandirMode.EXPANDED):
            observed.append(entry)

    assert observed == []
    assert len(client.calls) == 1


@pytest.mark.anyio
async def test_scandir_rejects_repeated_cursor_before_yielding_duplicate_page() -> None:
    client = FakeApiClient()
    client.pages = [
        BlobPageState((_item("folder/one"),), "next", True),
        BlobPageState((_item("folder/one"),), "next", True),
    ]
    service = _service(client)
    observed = []

    with pytest.raises(BlobStreamError, match="continuation cursor"):
        async for entry in service.scandir(mode=ScandirMode.EXPANDED):
            observed.append(entry.pathname)

    assert observed == ["folder/one"]
    assert len(client.calls) == 2


@pytest.mark.anyio
async def test_rmtree_traverses_once_and_continues_after_batch_errors() -> None:
    client = FakeApiClient()
    first_entries = tuple(_item(f"tree/{index}") for index in range(1001))
    client.pages = [
        BlobPageState(first_entries, "next", True),
        BlobPageState((_item("tree/final"),), None, False),
    ]
    failed_batch = tuple(f"tree/{index}" for index in range(1000))
    client.delete_failures.add(failed_batch)
    service = _service(client)

    with pytest.raises(BlobRecursiveDeleteError) as caught:
        await service.rmtree("tree", missing_ok=False)

    error = caught.value
    assert error.prefix == "tree/"
    assert error.attempted == 1002
    assert error.successful == 2
    assert len(error.failures) == 1
    assert [call[0] for call in client.calls] == [
        "list",
        "delete_batch",
        "delete_batch",
        "list",
        "delete_batch",
    ]


@pytest.mark.anyio
async def test_rmtree_reports_listing_failure_with_completed_batches() -> None:
    client = FakeApiClient()
    client.pages = [
        BlobPageState((_item("tree/marker"),), "next", True),
        RuntimeError("listing failed"),
    ]
    service = _service(client)

    with pytest.raises(BlobRecursiveDeleteError) as caught:
        await service.rmtree("tree/", missing_ok=True)

    assert caught.value.attempted == 1
    assert caught.value.successful == 1
    assert [str(failure) for failure in caught.value.failures] == ["listing failed"]


@pytest.mark.anyio
async def test_rmtree_deletes_marker_and_descendants_with_prefix_boundary() -> None:
    client = FakeApiClient()
    client.pages = [
        BlobPageState((_item("tree/"), _item("tree/child")), "next", True),
        RuntimeError("listing failed"),
    ]
    service = _service(client)

    with pytest.raises(BlobRecursiveDeleteError) as caught:
        await service.rmtree("tree", missing_ok=False)

    assert caught.value.attempted == 2
    assert caught.value.successful == 2
    assert client.calls == [
        ("list", "tree/", ScandirMode.EXPANDED, 1000, None),
        ("delete_batch", ("tree/", "tree/child")),
        ("list", "tree/", ScandirMode.EXPANDED, 1000, "next"),
    ]


@pytest.mark.anyio
async def test_prefix_normalization_validates_final_marker_length() -> None:
    client = FakeApiClient()
    service = _service(client)
    unslashed_949 = "a" * 949
    slashed_950 = f"{'b' * 949}/"

    await service.mkdir(unslashed_949, access="public", exist_ok=False)
    await service.mkdir(slashed_950, access="public", exist_ok=False)
    with pytest.raises(ValueError, match="950"):
        await service.mkdir("c" * 950, access="public", exist_ok=False)

    client.pages = [BlobPageState((), None, False), BlobPageState((), None, False)]
    await service.rmtree(unslashed_949, missing_ok=True)
    await service.rmtree(slashed_950, missing_ok=True)
    with pytest.raises(ValueError, match="950"):
        await service.rmtree("c" * 950, missing_ok=True)

    assert [call[1] for call in client.calls] == [
        f"{unslashed_949}/",
        slashed_950,
        f"{unslashed_949}/",
        slashed_950,
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("pathname", ["", "/", "//"])
async def test_prefix_operations_preserve_empty_and_root_rejection(pathname: str) -> None:
    client = FakeApiClient()
    service = _service(client)

    with pytest.raises(ValueError):
        await service.mkdir(pathname, access="public", exist_ok=False)
    with pytest.raises(ValueError):
        await service.rmtree(pathname, missing_ok=True)

    assert client.calls == []


@pytest.mark.anyio
async def test_presign_validates_expiry_and_delegates_absolute_utc_time() -> None:
    client = FakeApiClient()
    service = _service(client)

    with pytest.raises(ValueError, match="positive"):
        await service.presign(
            "object", operation=PresignedOperation.GET, access="public", expires_in=timedelta(0)
        )

    before = datetime.now(UTC) + timedelta(minutes=4, seconds=59)
    result = await service.presign(
        "object",
        operation=PresignedOperation.PUT,
        access="private",
        expires_in=timedelta(minutes=5),
        maximum_size=10,
        allowed_content_types=["text/plain"],
        allow_overwrite=False,
    )
    after = datetime.now(UTC) + timedelta(minutes=5, seconds=1)

    call = client.calls[-1]
    assert call[:4] == ("presign", "object", PresignedOperation.PUT, "private")
    assert before < call[4] < after
    assert call[4].tzinfo is UTC
    assert result.expires_at == call[4]


@pytest.mark.anyio
@pytest.mark.parametrize("expires_in", [timedelta(days=8), timedelta.max])
async def test_presign_rejects_expiry_beyond_supported_maximum(
    expires_in: timedelta,
) -> None:
    client = FakeApiClient()
    service = _service(client)

    with pytest.raises(ValueError, match="seven days"):
        await service.presign(
            "object",
            operation=PresignedOperation.GET,
            access="public",
            expires_in=expires_in,
        )

    assert client.calls == []


@pytest.mark.anyio
async def test_presign_rejects_non_timedelta_expiry_before_request() -> None:
    client = FakeApiClient()
    service = _service(client)

    with pytest.raises(TypeError, match="timedelta"):
        await service.presign(
            "object",
            operation=PresignedOperation.GET,
            access="public",
            expires_in=cast(Any, 60),
        )

    assert client.calls == []


@pytest.mark.anyio
async def test_async_object_entry_exposes_metadata_and_caches_stat() -> None:
    client = FakeApiClient()
    service = _service(client)
    entry = BlobObjectEntry(_item("folder/object.txt", access="private"), service)

    first = await entry.stat()
    second = await entry.stat()
    signed = await entry.presign(operation=PresignedOperation.HEAD, expires_in=timedelta(minutes=1))

    assert entry.name == "object.txt"
    assert entry.path == "folder/object.txt"
    assert entry.size == 12
    assert entry.is_object() is True
    assert entry.is_prefix() is False
    assert first is second
    assert [call[0] for call in client.calls] == ["stat", "presign"]
    assert client.calls[-1][3] == "private"
    assert signed.operation is PresignedOperation.HEAD
    with pytest.raises(ValueError, match="get and head"):
        await entry.presign(operation=PresignedOperation.PUT)


def test_pathlike_returning_bytes_is_rejected() -> None:
    class BytesPath(os.PathLike[bytes]):
        def __fspath__(self) -> bytes:
            return b"object"

    service = _service(FakeApiClient())

    with pytest.raises(TypeError, match="string path"):
        service.normalize_path(cast(Any, BytesPath()))
