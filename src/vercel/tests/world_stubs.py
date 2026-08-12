"""Shared stubs for hand-written `World` doubles in the test suite.

`World` declares the full contract a real world owes, streams included, so a
double that only exercises the event-log paths still has to say something about
them. Saying it once here keeps that from being six copied stub methods in
every fake, and keeps "this double has no streams" legible at the class
declaration instead of buried thirty lines down.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence

from vercel._internal.workflow import world as w


class NoStreams:
    """Mix in to declare a `World` double that does not support streams.

    Every method raises, so a test that unexpectedly reaches the stream path
    fails loudly rather than reading back plausible empty results.
    """

    async def streams_write(self, run_id: str, name: str, chunk: bytes) -> None:
        raise NotImplementedError

    async def streams_write_multi(self, run_id: str, name: str, chunks: Sequence[bytes]) -> None:
        raise NotImplementedError

    async def streams_close(self, run_id: str, name: str) -> None:
        raise NotImplementedError

    def streams_get(
        self, run_id: str, name: str, start_index: int | None = None
    ) -> AsyncGenerator[bytes, None]:
        raise NotImplementedError

    async def streams_list(self, run_id: str) -> list[str]:
        raise NotImplementedError

    async def streams_get_chunks(
        self, run_id: str, name: str, *, limit: int | None = None, cursor: str | None = None
    ) -> w.StreamChunksPage:
        raise NotImplementedError

    async def streams_get_info(self, run_id: str, name: str) -> w.StreamInfo:
        raise NotImplementedError
