"""Collection contracts exercised through both public runtimes."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx2 as httpx
import pytest
from pydantic import ValidationError
from test_object_ops import _async_session, _credentials, _object_json, _sync_session

from vercel import blob
from vercel.blob import BlobCredentials, BlobError, CredentialKind
from vercel.blob.sync import copy_blob, delete_blob

pytestmark = pytest.mark.anyio


@asynccontextmanager
async def runtime(
    sync: bool,
    handler: Callable[[httpx.Request], httpx.Response],
    credentials: Callable[[], BlobCredentials] | None = None,
) -> AsyncIterator[Callable[..., Any]]:
    credentials = credentials or _credentials

    async def call(name: str, *args: Any, **options: Any) -> Any:
        if name in ("query_blobs", "query_blob_entries"):
            if sync:
                return list(getattr(blob.sync, name)(*args, **options))
            return [item async for item in getattr(blob, name)(*args, **options)]
        if sync:
            return getattr(blob.sync, name)(*args, **options)
        return await getattr(blob, name)(*args, **options)

    if sync:
        with _sync_session(handler, credentials_factory=credentials):
            yield call
    else:
        async with _async_session(handler, credentials_factory=credentials):
            yield call


@pytest.fixture(params=[False, True], ids=["async", "sync"])
def sync(request: pytest.FixtureRequest) -> bool:
    return request.param


def entry(path: str) -> dict[str, Any]:
    data = _object_json(path, size=7)
    return {k: data[k] for k in ("url", "downloadUrl", "pathname", "size", "uploadedAt", "etag")}


async def test_query_blobs_pagination_and_metadata(sync: bool) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.headers["authorization"] == "Bearer oidc-test"
        assert request.headers["x-vercel-blob-store-id"] == "teststore123"
        assert request.headers["x-api-version"] == "12"
        assert request.url.params["prefix"] == "folder/雪 &"
        assert request.url.params["limit"] == "1"
        assert "mode" not in request.url.params
        if len(requests) == 1:
            assert "cursor" not in request.url.params
            return httpx.Response(
                200, json={"blobs": [entry("folder/a")], "hasMore": True, "cursor": "opaque+/=&雪"}
            )
        assert request.url.params["cursor"] == "opaque+/=&雪"
        return httpx.Response(200, json={"blobs": [], "hasMore": False})

    async with runtime(
        sync,
        handler,
        lambda: BlobCredentials(
            token="oidc-test", store_id="teststore123", kind=CredentialKind.OIDC
        ),
    ) as call:
        items = await call("query_blobs", prefix="folder/雪 &", page_size=1)
        assert len(items) == 1
        item = items[0]
        assert isinstance(item, blob.ListBlob)
        assert item.size == 7 and item.etag == "etag"
        assert item.uploaded_at.isoformat() == "2026-09-29T12:00:00+00:00"
    assert len(requests) == 2


async def test_copy_metadata_options_and_single_request(sync: bool) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "PUT"
        assert dict(request.url.params) == {"pathname": "copies/雪 &.bin", "fromUrl": "source.bin"}
        assert request.content == b""
        assert request.headers["x-if-match"] == '"source-etag"'
        assert request.headers["x-vercel-blob-access"] == "private"
        assert request.headers["x-allow-overwrite"] == "1"
        assert request.headers["x-add-random-suffix"] == "0"
        assert request.headers["x-content-type"] == "application/octet-stream"
        assert request.headers["x-cache-control-max-age"] == "300"
        return httpx.Response(200, json=_object_json("copies/雪 &.bin"))

    async with runtime(sync, handler) as call:
        result = await call(
            "copy_blob",
            "source.bin",
            "copies/雪 &.bin",
            access="private",
            content_type="application/octet-stream",
            cache_control_max_age=300,
            allow_overwrite=True,
            if_match='"source-etag"',
        )
    assert isinstance(result, blob.CopyResult)
    assert result.pathname == "copies/雪 &.bin" and result.etag == "etag"
    assert result.content_type == "text/plain" and result.content_disposition == "inline"
    assert result.download_url == result.url + "?download=1"
    assert len(requests) == 1


@pytest.mark.parametrize("targets", ["a.bin", ["a.bin"], ("a.bin", "b.bin"), ["a.bin"] * 1000])
async def test_delete_batch_in_one_request(sync: bool, targets: Any) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST" and request.url.path == "/api/blob/delete"
        assert json.loads(request.content) == {
            "urls": [targets] if isinstance(targets, str) else list(targets)
        }
        assert "x-if-match" not in request.headers
        return httpx.Response(200, json=None)

    async with runtime(sync, handler) as call:
        assert await call("delete_blob", targets) is None
    assert len(requests) == 1


async def test_conditional_delete(sync: bool) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-if-match"] == '"etag"'
        return httpx.Response(412, json={"error": {"code": "precondition_failed"}})

    async with runtime(sync, handler) as call:
        with pytest.raises(blob.BlobPreconditionFailedError):
            await call("delete_blob", ["a.bin"], if_match='"etag"')


@pytest.mark.parametrize(
    ("operation", "args", "options"),
    [
        ("query_blobs", (), {"page_size": 0}),
        ("query_blobs", (), {"page_size": 1001}),
        ("query_blobs", (), {"page_size": True}),
        ("query_blobs", (), {"page_size": 1.5}),
        ("query_blobs", (), {"cursor": ""}),
        ("query_blobs", (), {"cursor": "a\nb"}),
        ("query_blobs", (), {"prefix": "\ud800"}),
        ("delete_blob", ([],), {}),
        ("delete_blob", (["a"] * 1001,), {}),
        ("delete_blob", (["a", ""],), {}),
        ("delete_blob", (b"a",), {}),
        ("delete_blob", (["a", "b"],), {"if_match": '"etag"'}),
        ("delete_blob", ("a",), {"if_match": ""}),
        ("copy_blob", ("https://evil.example/a", "b"), {"access": "public"}),
        ("copy_blob", ("a", "../b"), {"access": "public"}),
        ("copy_blob", ("a", "b"), {"access": "invalid"}),
        ("copy_blob", ("a", "b"), {"access": "public", "if_match": "a\rb"}),
        ("copy_blob", ("a", "b"), {"access": "public", "allow_overwrite": 1}),
    ],
)
async def test_validation_before_credentials_and_http(
    sync: bool, operation: str, args: tuple[Any, ...], options: dict[str, Any]
) -> None:
    def credentials() -> BlobCredentials:
        pytest.fail("invalid request resolved credentials")

    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid request reached HTTP")

    async with runtime(sync, handler, credentials) as call:
        with pytest.raises((BlobError, ValidationError, TypeError, ValueError)):
            await call(operation, *args, **options)


@pytest.mark.parametrize(
    ("operation", "args", "options"),
    [
        ("query_blobs", (), {}),
        ("copy_blob", ("a", "b"), {"access": "public"}),
        ("delete_blob", (["a", "b"],), {}),
    ],
)
@pytest.mark.parametrize(
    ("status", "error"),
    [
        (403, blob.BlobAccessError),
        (404, blob.BlobNotFoundError),
        (412, blob.BlobPreconditionFailedError),
        (429, blob.BlobServiceRateLimited),
    ],
)
async def test_service_errors_no_retry(
    sync: bool,
    operation: str,
    args: tuple[Any, ...],
    options: dict[str, Any],
    status: int,
    error: type[Exception],
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            status, json={"error": {"message": "failed"}}, headers={"retry-after": "5"}
        )

    async with runtime(sync, handler) as call:
        with pytest.raises(error):
            await call(operation, *args, **options)
    assert len(requests) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"blobs": [], "hasMore": "true"},
        {"blobs": [], "hasMore": True},
        {"blobs": [{**entry("a"), "size": -1}], "hasMore": False},
        {"blobs": [{**entry("a"), "uploadedAt": "bad-date"}], "hasMore": False},
        {"blobs": [{**entry("a"), "etag": None}], "hasMore": False},
    ],
)
async def test_malformed_list_response(sync: bool, payload: Any) -> None:
    async with runtime(sync, lambda r: httpx.Response(200, json=payload)) as call:
        with pytest.raises(blob.BlobStreamError):
            await call("query_blobs")


async def test_malformed_copy_response(sync: bool) -> None:
    async with runtime(sync, lambda r: httpx.Response(200, json={"pathname": "b"})) as call:
        with pytest.raises(blob.BlobStreamError):
            await call("copy_blob", "a", "b", access="public")


async def test_copy_defaults_and_url_source(sync: bool) -> None:
    source = _object_json("a")["url"] + "?download=1#fragment"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["fromUrl"] == source.split("#")[0]
        assert request.url.params["pathname"] == "b"
        assert request.headers["x-allow-overwrite"] == "0"
        assert request.headers["x-add-random-suffix"] == "0"
        assert "x-if-match" not in request.headers
        assert "x-content-type" not in request.headers
        assert "x-cache-control-max-age" not in request.headers
        return httpx.Response(200, json=_object_json("b"))

    async with runtime(sync, handler) as call:
        await call("copy_blob", source, "/b", access="public")


async def test_successful_conditional_delete(sync: bool) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-if-match"] == '"etag"'
        assert json.loads(request.content) == {"urls": ["a"]}
        return httpx.Response(200, json=None)

    async with runtime(sync, handler) as call:
        assert await call("delete_blob", "a", if_match='"etag"') is None


@pytest.mark.parametrize(
    ("operation", "args", "options"),
    [
        ("query_blobs", (), {}),
        ("copy_blob", ("a", "b"), {"access": "public"}),
    ],
)
async def test_non_json_response(
    sync: bool, operation: str, args: tuple[Any, ...], options: dict[str, Any]
) -> None:
    async with runtime(sync, lambda r: httpx.Response(200, content=b"invalid-json")) as call:
        with pytest.raises(blob.BlobStreamError, match="not valid JSON"):
            await call(operation, *args, **options)


def test_sync_collections_without_event_loop() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"blobs": [], "hasMore": False})
        if request.method == "PUT":
            return httpx.Response(200, json=_object_json("b"))
        return httpx.Response(200, json=None)

    with _sync_session(handler):
        assert list(blob.sync.query_blobs()) == []
        assert copy_blob("a", "b", access="public").pathname == "b"
        delete_blob(["a", "b"])


@pytest.mark.parametrize("query_name", ["query_blobs", "query_blob_entries"])
async def test_queries_are_lazy_and_fetch_pages_on_demand(sync: bool, query_name: str) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.params["prefix"] == "folder/"
        assert request.url.params["limit"] == "2"
        if query_name == "query_blob_entries":
            assert request.url.params["mode"] == "folded"
        else:
            assert "mode" not in request.url.params
        if len(requests) == 1:
            assert request.url.params["cursor"] == "start+/="
            return httpx.Response(
                200,
                json={
                    "blobs": [entry("folder/a"), entry("folder/b")],
                    "hasMore": True,
                    "cursor": "next+/=",
                },
            )
        assert request.url.params["cursor"] == "next+/="
        return httpx.Response(200, json={"blobs": [entry("folder/c")], "hasMore": False})

    async with runtime(sync, handler):
        api = blob.sync if sync else blob
        iterator: Any = getattr(api, query_name)(prefix="folder/", page_size=2, cursor="start+/=")
        assert not requests

        async def advance() -> Any:
            return next(iterator) if sync else await anext(iterator)

        assert (await advance()).pathname == "folder/a"
        assert len(requests) == 1
        assert (await advance()).pathname == "folder/b"
        assert len(requests) == 1
        assert (await advance()).pathname == "folder/c"
        assert len(requests) == 2
        with pytest.raises(StopIteration if sync else StopAsyncIteration):
            if sync:
                next(iterator)
            else:
                await anext(iterator)
        assert len(requests) == 2


async def test_query_blobs_early_exit_fetches_no_more_pages(sync: bool) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "blobs": [entry("a")],
                "hasMore": True,
                "cursor": "next",
            },
        )

    async with runtime(sync, handler):
        if sync:
            for item in blob.sync.query_blobs():
                assert item.pathname == "a"
                break
        else:
            async for item in blob.query_blobs():
                assert item.pathname == "a"
                break
    assert len(requests) == 1


async def test_query_blobs_empty_page_stops_like_sandbox(sync: bool) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"blobs": [], "hasMore": True, "cursor": "next"})

    async with runtime(sync, handler):
        if sync:
            assert list(blob.sync.query_blobs()) == []
        else:
            assert [item async for item in blob.query_blobs()] == []
    assert len(requests) == 1


async def test_query_blobs_later_page_error_propagates_without_retry(sync: bool) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(
                200,
                json={
                    "blobs": [entry("a")],
                    "hasMore": True,
                    "cursor": "next",
                },
            )
        return httpx.Response(403)

    seen: list[str] = []
    async with runtime(sync, handler):
        with pytest.raises(blob.BlobAccessError):
            if sync:
                for item in blob.sync.query_blobs():
                    seen.append(item.pathname)
            else:
                async for item in blob.query_blobs():
                    seen.append(item.pathname)
    assert seen == ["a"]
    assert len(requests) == 2


async def test_folded_entries_mixed_and_folder_only_pages(sync: bool) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.params["mode"] == "folded"
        assert request.url.params["prefix"] == "reports/雪 &/"
        assert request.url.params["limit"] == "2"
        if len(requests) == 1:
            assert request.url.params["cursor"] == "start"
            return httpx.Response(
                200,
                json={
                    "blobs": [entry("reports/雪 &/a.bin")],
                    "folders": ["reports/雪 &/nested/"],
                    "hasMore": True,
                    "cursor": "next+/=&",
                },
            )
        if len(requests) == 2:
            assert request.url.params["cursor"] == "next+/=&"
            return httpx.Response(
                200,
                json={
                    "blobs": [],
                    "folders": ["reports/雪 &/other/"],
                    "hasMore": True,
                    "cursor": "last",
                },
            )
        assert request.url.params["cursor"] == "last"
        return httpx.Response(
            200,
            json={
                "blobs": [entry("reports/雪 &/z.bin")],
                "hasMore": False,
            },
        )

    async with runtime(sync, handler) as call:
        items = await call(
            "query_blob_entries", prefix="reports/雪 &/", page_size=2, cursor="start"
        )
    assert len(requests) == 3
    assert [type(item) for item in items] == [
        blob.ListBlob,
        blob.BlobPrefix,
        blob.BlobPrefix,
        blob.ListBlob,
    ]
    assert items[0].pathname == "reports/雪 &/a.bin"
    assert items[1] == blob.BlobPrefix(prefix="reports/雪 &/nested/")
    assert items[2] == blob.BlobPrefix(prefix="reports/雪 &/other/")
    assert items[3].pathname == "reports/雪 &/z.bin"


@pytest.mark.parametrize("prefix", [None, ""])
async def test_folded_entries_root(sync: bool, prefix: str | None) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 2:
            assert request.url.params["prefix"] == "reports/"
            return httpx.Response(200, json={"blobs": [], "hasMore": False})
        if prefix is None:
            assert "prefix" not in request.url.params
        else:
            assert request.url.params["prefix"] == ""
        assert request.url.params["mode"] == "folded"
        return httpx.Response(
            200,
            json={
                "blobs": [entry("root.bin")],
                "folders": ["reports/"],
                "hasMore": False,
            },
        )

    async with runtime(sync, handler) as call:
        items = await call("query_blob_entries", prefix=prefix)
        assert items[1] == blob.BlobPrefix(prefix="reports/")
        # Folder prefixes can be passed directly to another directory query.
        await call("query_blob_entries", prefix=items[1].prefix)


@pytest.mark.parametrize(
    "options",
    [
        {"prefix": "reports"},
        {"prefix": "reports-old"},
        {"prefix": "a\n/"},
        {"prefix": "\ud800/"},
        {"page_size": 0},
        {"page_size": 1001},
        {"page_size": True},
        {"cursor": ""},
    ],
)
async def test_entries_validation_before_credentials(
    sync: bool,
    options: dict[str, Any],
) -> None:
    def credentials() -> BlobCredentials:
        pytest.fail("invalid query resolved credentials")

    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid query reached HTTP")

    async with runtime(sync, handler, credentials) as call:
        with pytest.raises((BlobError, ValidationError)):
            await call("query_blob_entries", **options)


@pytest.mark.parametrize("folders", [[1], None, "reports/", [""], ["reports"]])
async def test_malformed_folder_response(sync: bool, folders: Any) -> None:
    async with runtime(
        sync,
        lambda r: httpx.Response(
            200,
            json={
                "blobs": [],
                "folders": folders,
                "hasMore": False,
            },
        ),
    ) as call:
        with pytest.raises(blob.BlobStreamError):
            await call("query_blob_entries")


async def test_entries_early_exit_does_not_fetch_next_page(sync: bool) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "blobs": [],
                "folders": ["reports/"],
                "hasMore": True,
                "cursor": "next",
            },
        )

    async with runtime(sync, handler):
        if sync:
            for item in blob.sync.query_blob_entries():
                assert item == blob.BlobPrefix(prefix="reports/")
                break
        else:
            async for item in blob.query_blob_entries():
                assert item == blob.BlobPrefix(prefix="reports/")
                break
    assert len(requests) == 1


async def test_entries_later_page_error_propagates(sync: bool) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(
                200,
                json={
                    "blobs": [],
                    "folders": ["reports/"],
                    "hasMore": True,
                    "cursor": "next",
                },
            )
        return httpx.Response(403)

    seen: list[blob.ListBlob | blob.BlobPrefix] = []
    async with runtime(sync, handler):
        with pytest.raises(blob.BlobAccessError):
            if sync:
                for item in blob.sync.query_blob_entries():
                    seen.append(item)
            else:
                async for item in blob.query_blob_entries():
                    seen.append(item)
    assert seen == [blob.BlobPrefix(prefix="reports/")]
    assert len(requests) == 2


def test_sync_entries_without_event_loop() -> None:
    with _sync_session(
        lambda r: httpx.Response(
            200,
            json={
                "blobs": [entry("a")],
                "folders": ["b/"],
                "hasMore": False,
            },
        )
    ):
        entries = list(blob.sync.query_blob_entries())
    assert isinstance(entries[0], blob.ListBlob)
    assert entries[1] == blob.BlobPrefix(prefix="b/")
