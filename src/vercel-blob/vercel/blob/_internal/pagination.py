"""Lazy Blob catalog iterators matching Sandbox query APIs."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.blob.models import BlobPrefix, ListBlob

if TYPE_CHECKING:
    from .service import BlobService

_Item = TypeVar("_Item")


@dataclass(frozen=True, slots=True)
class QueryPage(Generic[_Item]):
    items: tuple[_Item, ...]
    next_cursor: str | None


async def _iterate(
    fetch_page: Callable[[str | None], Coroutine[Any, Any, QueryPage[_Item]]],
    cursor: str | None,
) -> AsyncIterator[_Item]:
    while True:
        page = await fetch_page(cursor)
        for item in page.items:
            yield item
        if page.next_cursor is None or not page.items:
            return
        cursor = page.next_cursor


def _iterate_sync(
    fetch_page: Callable[[str | None], Coroutine[Any, Any, QueryPage[_Item]]],
    cursor: str | None,
) -> Iterator[_Item]:
    while True:
        page = iter_coroutine(fetch_page(cursor))
        yield from page.items
        if page.next_cursor is None or not page.items:
            return
        cursor = page.next_cursor


def query_blobs(
    service: BlobService,
    *,
    prefix: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[ListBlob]:
    return _iterate(
        lambda next_cursor: service.query_blobs_page(
            prefix=prefix, page_size=page_size, cursor=next_cursor
        ),
        cursor,
    )


def query_blobs_sync(
    service: BlobService,
    *,
    prefix: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> Iterator[ListBlob]:
    return _iterate_sync(
        lambda next_cursor: service.query_blobs_page(
            prefix=prefix, page_size=page_size, cursor=next_cursor
        ),
        cursor,
    )


def query_blob_entries(
    service: BlobService,
    *,
    prefix: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[ListBlob | BlobPrefix]:
    return _iterate(
        lambda next_cursor: service.query_blob_entries_page(
            prefix=prefix, page_size=page_size, cursor=next_cursor
        ),
        cursor,
    )


def query_blob_entries_sync(
    service: BlobService,
    *,
    prefix: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> Iterator[ListBlob | BlobPrefix]:
    return _iterate_sync(
        lambda next_cursor: service.query_blob_entries_page(
            prefix=prefix, page_size=page_size, cursor=next_cursor
        ),
        cursor,
    )
