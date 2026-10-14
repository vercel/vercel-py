"""Lazy Blob catalog iterators matching Sandbox query APIs."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.blob.models import ListBlob

if TYPE_CHECKING:
    from .service import BlobService


@dataclass(frozen=True, slots=True)
class QueryBlobsPage:
    blobs: tuple[ListBlob, ...]
    next_cursor: str | None


def query_blobs(
    service: BlobService,
    *,
    prefix: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[ListBlob]:
    async def iterate() -> AsyncIterator[ListBlob]:
        next_cursor = cursor
        while True:
            page = await service.query_blobs_page(
                page_size=page_size, prefix=prefix, cursor=next_cursor
            )
            for item in page.blobs:
                yield item
            if page.next_cursor is None or not page.blobs:
                return
            next_cursor = page.next_cursor

    return iterate()


def query_blobs_sync(
    service: BlobService,
    *,
    prefix: str | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> Iterator[ListBlob]:
    next_cursor = cursor
    while True:
        page = iter_coroutine(
            service.query_blobs_page(page_size=page_size, prefix=prefix, cursor=next_cursor)
        )
        yield from page.blobs
        if page.next_cursor is None or not page.blobs:
            return
        next_cursor = page.next_cursor
