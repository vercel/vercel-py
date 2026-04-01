from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Generic, TypeVar

from vercel._internal.sandbox.models import Pagination

ItemT = TypeVar("ItemT")


@dataclass(slots=True)
class Page(Generic[ItemT]):
    items: list[ItemT]
    pagination: Pagination

    def __iter__(self) -> Iterator[ItemT]:
        return iter(self.items)


__all__ = ["Page"]
