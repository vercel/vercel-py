import base64
import csv
import gc
import io
import json
import shutil
import time
import warnings
from datetime import datetime, timezone
from typing import Any, cast

import anyio
import httpx
import pytest

import vercel.oidc as oidc
import vercel.oidc.aio as oidc_aio
from vercel import unstable as vercel
from vercel._internal.unstable.blob.errors import (
    BlobCredentialsError,
    BlobRecursiveDeleteError,
)
from vercel._internal.unstable.blob.models import (
    BlobListItemState,
    BlobPrefixState,
    PresignedOperation,
    ScandirMode,
)
from vercel._internal.unstable.blob.service import BlobService
from vercel._internal.unstable.errors import VercelSessionClosedError
from vercel._internal.unstable.session import (
    SdkSession,
    SyncSdkSession,
    get_active_session,
    get_active_sync_session,
)
from vercel.unstable.blob import BlobCredentials, BlobServiceOptions

BASE_URL = "https://blob-public-flow.test"
UTC = timezone.utc


def _stat_payload(pathname: str, *, size: int = 4, etag: str = '"etag"') -> dict[str, object]:
    return {
        "pathname": pathname,
        "url": f"https://store.public.blob.vercel-storage.com/{pathname}",
        "downloadUrl": f"https://store.public.blob.vercel-storage.com/{pathname}?download=1",
        "size": size,
        "etag": etag,
        "uploadedAt": "2026-06-11T12:00:00Z",
        "contentType": "application/octet-stream",
        "contentDisposition": "inline",
        "cacheControl": "public, max-age=60",
    }


def _client_factory(
    handler: Any,
) -> Any:
    return lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _sync_client_factory(handler: Any) -> Any:
    return lambda: httpx.Client(transport=httpx.MockTransport(handler))


def _disable_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    async def raise_async() -> str:
        raise oidc.VercelOidcTokenError("no oidc")

    def raise_sync() -> str:
        raise oidc.VercelOidcTokenError("no oidc")

    monkeypatch.setattr(oidc_aio, "get_vercel_oidc_token", raise_async)
    monkeypatch.setattr(oidc, "get_vercel_oidc_token_sync", raise_sync)


def _set_oidc(monkeypatch: pytest.MonkeyPatch, token: str) -> None:
    async def get_async() -> str:
        return token

    monkeypatch.setattr(oidc_aio, "get_vercel_oidc_token", get_async)
    monkeypatch.setattr(oidc, "get_vercel_oidc_token_sync", lambda: token)


def _options(
    *,
    base_url: str = BASE_URL,
    token: str = "oidc-token",
    store_id: str = "store",
    default_access: str = "public",
) -> BlobServiceOptions:
    async def credentials() -> BlobCredentials:
        return BlobCredentials(token=token, store_id=store_id, kind="oidc")

    def sync_credentials() -> BlobCredentials:
        return BlobCredentials(token=token, store_id=store_id, kind="oidc")

    return BlobServiceOptions(
        base_url=base_url,
        credentials_factory=credentials,
        sync_credentials_factory=sync_credentials,
        default_access=cast(Any, default_access),
    )


class _Service:
    def __init__(self, *, default_access: str = "public") -> None:
        self.options = type("Options", (), {"default_access": default_access})()
        self.calls: list[tuple[Any, ...]] = []

    async def stat(self, pathname: object) -> str:
        self.calls.append(("stat", pathname))
        return "stat"

    async def scandir(self, **kwargs: Any):
        self.calls.append(("scandir", kwargs))
        yield BlobListItemState(
            "folder/file.txt",
            "https://store.public.blob.vercel-storage.com/folder/file.txt",
            "https://store.public.blob.vercel-storage.com/folder/file.txt?download=1",
            1,
            '"etag"',
            datetime.now(UTC),
        )
        yield BlobPrefixState("folder/nested/")

    async def remove(self, pathname: object, **kwargs: Any) -> None:
        self.calls.append(("remove", pathname, kwargs))

    async def rmtree(self, pathname: object, **kwargs: Any) -> None:
        self.calls.append(("rmtree", pathname, kwargs))

    async def mkdir(self, pathname: object, **kwargs: Any) -> None:
        self.calls.append(("mkdir", pathname, kwargs))

    async def presign(self, pathname: object, **kwargs: Any) -> str:
        self.calls.append(("presign", pathname, kwargs))
        return "signed"


def _install(service: _Service) -> None:
    get_active_session()._service_cache[BlobService] = cast(Any, service)


def _install_sync(service: _Service) -> None:
    get_active_sync_session()._service_cache[BlobService] = cast(Any, service)


@pytest.mark.anyio
async def test_open_validates_mode_specific_arguments_before_service_resolution() -> None:
    from vercel.unstable import blob

    with pytest.raises(ValueError, match="binary mode"):
        blob.open("file", cast(Any, "rb"), encoding="utf-8")
    with pytest.raises(ValueError, match="read mode"):
        blob.open("file", "r", content_type="text/plain")


@pytest.mark.anyio
async def test_closed_owned_stream_rejects_io_after_session_exit() -> None:
    from vercel.unstable import blob

    operation = None
    async with vercel.session():
        service = _Service()
        _install(service)
        operation = blob.open("file", "rb")
    with pytest.raises(VercelSessionClosedError, match="closed"):
        await operation


@pytest.mark.anyio
async def test_real_session_routes_stat_listing_and_mutations_with_credentials() -> None:
    from vercel.unstable import blob

    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET" and "prefix" in request.url.params:
            return httpx.Response(
                200,
                json={
                    "blobs": [_stat_payload("folder/file")],
                    "folders": ["folder/nested/"],
                    "hasMore": False,
                },
            )
        if request.method == "POST" and request.url.path.endswith("/delete"):
            return httpx.Response(200, json={})
        if request.method == "PUT":
            return httpx.Response(200, json={"etag": '"etag"'})
        return httpx.Response(200, json=_stat_payload(request.url.params.get("url", "folder/")))

    async with vercel.session(
        service_options=[_options(default_access="private")],
        httpx_client_factory=_client_factory(handler),
    ):
        result = await blob.stat("object")
        entries = [entry async for entry in blob.scandir("folder/", mode=ScandirMode.FOLDED)]
        await blob.remove("object")
        await blob.mkdir("folder", exist_ok=True)

    assert result.pathname == "object"
    assert [entry.path for entry in entries] == ["folder/file", "folder/nested/"]
    assert all(request.headers["authorization"] == "Bearer oidc-token" for request in requests)
    assert all(request.headers["x-vercel-blob-store-id"] == "store" for request in requests)
    marker = next(request for request in requests if request.method == "PUT")
    assert marker.headers["x-vercel-blob-access"] == "private"


@pytest.mark.anyio
async def test_real_default_session_fallback_and_closed_behavior() -> None:
    from vercel.unstable import blob

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_stat_payload(request.url.params["url"]))

    previous = SdkSession._default
    default = SdkSession(
        service_options={BlobServiceOptions: _options()},
        httpx_client_factory=_client_factory(handler),
    )
    SdkSession._default = default
    try:
        assert (await blob.stat("default")).pathname == "default"
        await default.aclose()
        with pytest.raises(VercelSessionClosedError):
            await blob.stat("closed")
    finally:
        SdkSession._default = previous

    assert len(requests) == 1


def test_real_sync_default_session_fallback_and_closed_behavior() -> None:
    from vercel.unstable.blob import sync as blob

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_stat_payload(request.url.params["url"]))

    previous = SyncSdkSession._default
    default = SyncSdkSession(
        service_options={BlobServiceOptions: _options()},
        httpx_client_factory=_sync_client_factory(handler),
    )
    SyncSdkSession._default = default
    try:
        assert blob.stat("default").pathname == "default"
        default.close()
        with pytest.raises(VercelSessionClosedError):
            blob.stat("closed")
    finally:
        SyncSdkSession._default = previous

    assert len(requests) == 1


@pytest.mark.anyio
async def test_real_session_default_read_write_credentials(monkeypatch) -> None:
    from vercel.unstable import blob

    monkeypatch.setenv("BLOB_READ_WRITE_TOKEN", "vercel_blob_rw_store_secret")
    monkeypatch.delenv("VERCEL_BLOB_READ_WRITE_TOKEN", raising=False)
    monkeypatch.delenv("BLOB_STORE_ID", raising=False)
    _disable_oidc(monkeypatch)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_stat_payload("object"))

    async with vercel.session(
        service_options=[BlobServiceOptions(base_url=BASE_URL)],
        httpx_client_factory=_client_factory(handler),
    ):
        await blob.stat("object")

    assert requests[0].headers["authorization"] == "Bearer vercel_blob_rw_store_secret"
    assert "x-vercel-blob-store-id" not in requests[0].headers


@pytest.mark.anyio
async def test_async_custom_credentials_factory_may_suspend() -> None:
    from vercel.unstable import blob

    calls = 0

    async def credentials() -> BlobCredentials:
        nonlocal calls
        calls += 1
        await anyio.sleep(0)
        return BlobCredentials(token="oidc-token", store_id="async-store", kind="oidc")

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_stat_payload(request.url.params["url"]))

    async with vercel.session(
        service_options=[BlobServiceOptions(base_url=BASE_URL, credentials_factory=credentials)],
        httpx_client_factory=_client_factory(handler),
    ):
        assert (await blob.stat("object")).pathname == "object"

    assert calls == 1
    assert requests[0].headers["x-vercel-blob-store-id"] == "async-store"


def test_sync_rejects_suspending_credentials_factory_before_transport() -> None:
    from vercel.unstable.blob import sync as blob

    credential_calls = 0

    async def credentials() -> BlobCredentials:
        nonlocal credential_calls
        credential_calls += 1
        await anyio.sleep(0)
        return BlobCredentials(token="oidc-token", store_id="sync-store", kind="oidc")

    transport_calls = 0
    transport_factory_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal transport_calls
        transport_calls += 1
        return httpx.Response(500)

    def client_factory() -> httpx.Client:
        nonlocal transport_factory_calls
        transport_factory_calls += 1
        return httpx.Client(transport=httpx.MockTransport(handler))

    with vercel.session(
        service_options=[BlobServiceOptions(base_url=BASE_URL, credentials_factory=credentials)],
        httpx_client_factory=client_factory,
    ):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(BlobCredentialsError, match="sync_credentials_factory"):
                blob.stat("object")
            gc.collect()

    assert transport_calls == 0
    assert transport_factory_calls == 0
    assert credential_calls == 0
    assert not [warning for warning in caught if "never awaited" in str(warning.message)]


@pytest.mark.anyio
async def test_real_session_incomplete_oidc_configuration_errors(monkeypatch) -> None:
    from vercel.unstable import blob

    monkeypatch.delenv("BLOB_READ_WRITE_TOKEN", raising=False)
    monkeypatch.delenv("VERCEL_BLOB_READ_WRITE_TOKEN", raising=False)
    monkeypatch.delenv("BLOB_STORE_ID", raising=False)
    _set_oidc(monkeypatch, "oidc-token")

    async with vercel.session(
        service_options=[BlobServiceOptions(base_url=BASE_URL)],
        httpx_client_factory=_client_factory(lambda request: httpx.Response(500)),
    ):
        with pytest.raises(BlobCredentialsError, match="BLOB_STORE_ID"):
            await blob.stat("object")


@pytest.mark.anyio
async def test_real_session_default_oidc_credentials_include_store(monkeypatch) -> None:
    from vercel.unstable import blob

    monkeypatch.delenv("BLOB_READ_WRITE_TOKEN", raising=False)
    monkeypatch.delenv("VERCEL_BLOB_READ_WRITE_TOKEN", raising=False)
    monkeypatch.setenv("BLOB_STORE_ID", "store_oidc")
    _set_oidc(monkeypatch, "oidc-token")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_stat_payload("object"))

    async with vercel.session(
        service_options=[BlobServiceOptions(base_url=BASE_URL)],
        httpx_client_factory=_client_factory(handler),
    ):
        await blob.stat("object")

    assert requests[0].headers["authorization"] == "Bearer oidc-token"
    assert requests[0].headers["x-vercel-blob-store-id"] == "oidc"


@pytest.mark.anyio
async def test_real_session_open_read_write_update_and_abort() -> None:
    from vercel.unstable import blob

    publications: list[tuple[str, bytes]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "store.public.blob.vercel-storage.com":
            return httpx.Response(
                206,
                content=b"data",
                headers={"content-range": "bytes 0-3/4", "etag": '"etag"'},
            )
        if request.method == "PUT":
            publications.append((request.url.params["pathname"], await request.aread()))
            return httpx.Response(200, json={"etag": '"etag"'})
        return httpx.Response(200, json=_stat_payload(request.url.params["url"]))

    async with vercel.session(
        service_options=[_options()],
        httpx_client_factory=_client_factory(handler),
    ):
        async with blob.open("source", "rb") as reader:
            assert await reader.read() == b"data"
        async with blob.open("created", "wb") as writer:
            assert await writer.write(b"new") == 3
        async with blob.open("source", "r+b") as updating:
            assert await updating.read(2) == b"da"
            await updating.seek(0)
            assert await updating.write(b"EDIT") == 4
        with pytest.raises(RuntimeError, match="abort"):
            async with blob.open("aborted", "wb") as aborted:
                await aborted.write(b"discard")
                raise RuntimeError("abort")

    assert publications == [("created", b"new"), ("source", b"EDIT")]


def test_sync_open_integrates_with_stdlib_readers_and_copying() -> None:
    from vercel.unstable.blob import sync as blob

    contents = {
        "rows.csv": b"name,value\none,1\ntwo,2\n",
        "value.json": b'{"answer": 42}',
        "source.bin": b"binary-data",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "store.public.blob.vercel-storage.com":
            pathname = request.url.path.lstrip("/")
            content = contents[pathname]
            range_value = request.headers["range"].removeprefix("bytes=")
            start_text, end_text = range_value.split("-", 1)
            start = int(start_text)
            end = len(content) - 1 if not end_text else min(int(end_text), len(content) - 1)
            part = content[start : end + 1]
            return httpx.Response(
                206,
                content=part,
                headers={
                    "content-range": f"bytes {start}-{end}/{len(content)}",
                    "etag": '"etag"',
                },
            )
        pathname = request.url.params["url"]
        return httpx.Response(200, json=_stat_payload(pathname, size=len(contents[pathname])))

    with vercel.session(
        service_options=[_options()],
        httpx_client_factory=_sync_client_factory(handler),
    ):
        with blob.open("rows.csv", "r", newline="") as source:
            assert list(csv.reader(source)) == [["name", "value"], ["one", "1"], ["two", "2"]]
        with blob.open("value.json", "r") as source:
            assert json.load(source) == {"answer": 42}
        with blob.open("source.bin", "rb") as binary_source:
            assert isinstance(binary_source, io.BufferedIOBase)
            target = io.BytesIO()
            shutil.copyfileobj(binary_source, target, length=3)
            assert target.getvalue() == b"binary-data"


def test_sync_writer_context_publishes_or_aborts_with_explicit_ownership() -> None:
    from vercel.unstable.blob import sync as blob

    publications: list[tuple[str, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            publications.append((request.url.params["pathname"], request.read()))
            return httpx.Response(200, json={"etag": '"etag"'})
        return httpx.Response(200, json=_stat_payload(request.url.params["url"]))

    with vercel.session(
        service_options=[_options()],
        httpx_client_factory=_sync_client_factory(handler),
    ):
        with blob.open("created", "wb") as binary_writer:
            assert binary_writer.write(b"new") == 3
        text_writer = blob.open("owned", "w")
        assert text_writer.write("text") == 4
        assert publications == [("created", b"new")]
        text_writer.close()
        with pytest.raises(RuntimeError, match="abort"):
            with blob.open("aborted", "wb") as aborted:
                aborted.write(b"discard")
                raise RuntimeError("abort")

    assert publications == [("created", b"new"), ("owned", b"text")]


async def _public_stream_typing_probe() -> None:
    """Checked by mypy; never executed by pytest."""
    from vercel.unstable import blob

    binary_writer = await blob.open("binary", "wb")
    await binary_writer.write(b"data")
    await binary_writer.flush()
    await binary_writer.truncate(2)
    await binary_writer.write("invalid")  # type: ignore[arg-type]

    updating = await blob.open("binary", "r+b")
    await updating.read()
    await updating.write(b"data")
    await updating.truncate()

    text_writer = await blob.open("text", "w")
    await text_writer.write("data")
    await text_writer.flush()
    await text_writer.truncate(2)
    await text_writer.write(b"invalid")  # type: ignore[arg-type]


def _sync_public_stream_typing_probe() -> None:
    """Checked by mypy; never executed by pytest."""
    from vercel.unstable.blob import sync as blob

    binary_writer = blob.open("binary", "wb")
    binary_writer.write(b"data")
    binary_writer.flush()
    binary_writer.truncate(2)
    binary_writer.write("invalid")  # type: ignore[arg-type]

    updating = blob.open("binary", "r+b")
    updating.read()
    updating.write(b"data")
    updating.truncate()

    text_writer = blob.open("text", "w")
    text_writer.write("data")
    text_writer.flush()
    text_writer.truncate(2)
    text_writer.write(b"invalid")  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_reader_writer_only_methods_raise_unsupported() -> None:
    from vercel.unstable import blob

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_stat_payload(request.url.params["url"]))

    async with vercel.session(
        service_options=[_options()],
        httpx_client_factory=_client_factory(handler),
    ):
        binary_reader = await blob.open("binary", "rb")
        with pytest.raises(OSError, match="not writable"):
            await binary_reader.flush()
        with pytest.raises(OSError, match="not writable"):
            await binary_reader.truncate()
        await binary_reader.close()

        text_reader = await blob.open("text", "r")
        with pytest.raises(OSError, match="not writable"):
            await text_reader.flush()
        with pytest.raises(OSError, match="not writable"):
            await text_reader.truncate()
        await text_reader.close()


@pytest.mark.anyio
async def test_real_session_presigns_nonexistent_and_list_entry_without_stat() -> None:
    from vercel.unstable import blob

    stat_requests = 0
    issued_pathnames: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal stat_requests
        if request.method == "GET" and "prefix" in request.url.params:
            return httpx.Response(
                200,
                json={"blobs": [_stat_payload("listed")], "folders": [], "hasMore": False},
            )
        if request.method == "GET":
            stat_requests += 1
            return httpx.Response(404)
        body = json.loads(await request.aread())
        pathname = body["pathname"]
        issued_pathnames.append(pathname)
        valid_until = body["validUntil"]
        payload = {
            "storeId": "store",
            "ownerId": "owner",
            "pathname": pathname,
            "operations": body["operations"],
            "validUntil": valid_until,
            "iat": int(time.time() * 1000),
        }
        encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
        return httpx.Response(
            200,
            json={
                "delegationToken": f"{encoded}.signature",
                "clientSigningToken": "signing-token",
                "validUntil": valid_until,
            },
        )

    async with vercel.session(
        service_options=[_options()],
        httpx_client_factory=_client_factory(handler),
    ):
        direct = await blob.presign("does-not-exist", operation=PresignedOperation.PUT)
        entry = [entry async for entry in blob.scandir()][0]
        get_url = await cast(Any, entry).presign()
        head_url = await cast(Any, entry).presign(operation=PresignedOperation.HEAD)

    assert direct.operation is PresignedOperation.PUT
    assert get_url.operation is PresignedOperation.GET
    assert head_url.operation is PresignedOperation.HEAD
    assert issued_pathnames == ["does-not-exist", "listed", "listed"]
    assert stat_requests == 0


def test_real_sync_marker_rmtree_presign_and_store_flows() -> None:
    from vercel.unstable.blob import sync as blob

    issued: list[tuple[str, list[str]]] = []
    deleted: list[list[str]] = []
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            if "url" in request.url.params:
                return httpx.Response(200, json=_stat_payload(request.url.params["url"], size=0))
            prefix = request.url.params.get("prefix")
            if prefix == "tree/":
                return httpx.Response(
                    200,
                    json={
                        "blobs": [_stat_payload("tree/"), _stat_payload("tree/child")],
                        "folders": [],
                        "hasMore": False,
                    },
                )
            return httpx.Response(
                200,
                json={"blobs": [_stat_payload("listed")], "folders": [], "hasMore": False},
            )
        if request.method == "PUT":
            return httpx.Response(200, json={"etag": '"etag"'})
        if request.url.path.endswith("/delete"):
            deleted.append(json.loads(request.read())["urls"])
            return httpx.Response(200, json={})

        body = json.loads(request.read())
        issued.append((body["pathname"], body["operations"]))
        payload = {
            "storeId": "sync-store",
            "ownerId": "owner",
            "pathname": body["pathname"],
            "operations": body["operations"],
            "validUntil": body["validUntil"],
            "iat": int(time.time() * 1000),
        }
        encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
        return httpx.Response(
            200,
            json={
                "delegationToken": f"{encoded}.signature",
                "clientSigningToken": "signing-token",
                "validUntil": body["validUntil"],
            },
        )

    with vercel.session(
        service_options=[_options(store_id="sync-store", default_access="private")],
        httpx_client_factory=_sync_client_factory(handler),
    ):
        blob.mkdir("folder", exist_ok=True)
        entry = cast(Any, next(blob.scandir()))
        assert entry.presign().operation is PresignedOperation.GET
        assert entry.presign(operation=PresignedOperation.HEAD).operation is PresignedOperation.HEAD
        assert (
            blob.presign("new", operation=PresignedOperation.PUT).operation
            is PresignedOperation.PUT
        )
        blob.rmtree("tree")

    assert issued == [
        ("listed", ["get"]),
        ("listed", ["head"]),
        ("new", ["put"]),
    ]
    assert deleted == [["tree/", "tree/child"]]
    assert all(request.headers["authorization"] == "Bearer oidc-token" for request in requests)
    assert all(request.headers["x-vercel-blob-store-id"] == "sync-store" for request in requests)


@pytest.mark.anyio
async def test_real_session_mkdir_descendants_and_exact_marker_conflicts() -> None:
    from vercel.unstable import blob

    marker_exists = False
    put_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal marker_exists, put_count
        if request.method == "PUT":
            put_count += 1
            if marker_exists:
                return httpx.Response(409, json={"error": {"code": "blob_already_exists"}})
            marker_exists = True
            return httpx.Response(200, json={"etag": '"etag"'})
        return httpx.Response(200, json=_stat_payload("folder/", size=0))

    async with vercel.session(
        service_options=[_options()],
        httpx_client_factory=_client_factory(handler),
    ):
        # A descendant may already exist; only the exact marker controls mkdir.
        await blob.mkdir("folder")
        with pytest.raises(FileExistsError):
            await blob.mkdir("folder")
        await blob.mkdir("folder", exist_ok=True)

    assert put_count == 3


@pytest.mark.anyio
async def test_real_session_rmtree_boundaries_missing_and_root_rejection() -> None:
    from vercel.unstable import blob

    deleted: list[list[str]] = []
    requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if request.method == "GET":
            assert request.url.params["prefix"] == "tree/"
            return httpx.Response(
                200,
                json={
                    "blobs": [_stat_payload("tree/"), _stat_payload("tree/child")],
                    "folders": [],
                    "hasMore": False,
                },
            )
        deleted.append(json.loads(await request.aread())["urls"])
        return httpx.Response(200, json={})

    async with vercel.session(
        service_options=[_options()],
        httpx_client_factory=_client_factory(handler),
    ):
        with pytest.raises(ValueError):
            await blob.rmtree("")
        with pytest.raises(ValueError):
            await blob.rmtree("/")
        assert requests == 0
        await blob.rmtree("tree")

    assert deleted == [["tree/", "tree/child"]]
    assert all(not path.startswith("treehouse/") for batch in deleted for path in batch)

    def empty_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"blobs": [], "folders": [], "hasMore": False})

    async with vercel.session(
        service_options=[_options()],
        httpx_client_factory=_client_factory(empty_handler),
    ):
        with pytest.raises(FileNotFoundError):
            await blob.rmtree("missing")
        await blob.rmtree("missing", missing_ok=True)


@pytest.mark.anyio
async def test_real_session_rmtree_partial_failure_reports_counts() -> None:
    from vercel.unstable import blob

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "blobs": [_stat_payload("tree/a"), _stat_payload("tree/b")],
                    "folders": [],
                    "hasMore": False,
                },
            )
        return httpx.Response(500, json={"error": {"code": "unknown"}})

    async with vercel.session(
        service_options=[_options()],
        httpx_client_factory=_client_factory(handler),
    ):
        with pytest.raises(BlobRecursiveDeleteError) as raised:
            await blob.rmtree("tree")

    assert raised.value.attempted == 2
    assert raised.value.successful == 0
    assert len(raised.value.failures) == 1
