"""`LocalWorld`'s stream storage, and its live read.

The on-disk shape is shared with the TypeScript `@workflow/world-local`, so the
layout assertions here are about interoperability, not about our own
preferences: one file per chunk under a per-stream directory, an EOF flag byte
in front of each payload, and the run's stream list as TS-readable JSON.
"""

from __future__ import annotations

import asyncio
import contextlib
import json

import pytest

from vercel._internal.workflow import world as w
from vercel._internal.workflow.worlds import local as local_mod

RUN_ID = "wrun_test"
NAME = "strm_test_user"


@pytest.fixture
def world(tmp_path, monkeypatch) -> local_mod.LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return local_mod.LocalWorld()


async def _drain(world: local_mod.LocalWorld, *, start_index: int | None = None) -> list[bytes]:
    """Read a closed stream to its end."""
    out = []
    async for chunk in world.streams_get(RUN_ID, NAME, start_index):
        out.append(chunk)
    return out


class TestLayout:
    async def test_chunks_are_one_file_each_under_a_per_stream_directory(self, world) -> None:
        # Per-stream rather than one flat directory: a live reader re-lists this
        # every 100ms, and that cost has to scale with the stream, not the world.
        await world.streams_write(RUN_ID, NAME, b"one")
        await world.streams_write(RUN_ID, NAME, b"two")

        chunk_dir = world.data_dir / "streams" / "chunks" / NAME
        files = sorted(p.name for p in chunk_dir.iterdir())
        assert len(files) == 2
        assert all(name.startswith("chnk_") and name.endswith(".bin") for name in files)

    async def test_a_chunk_file_is_its_eof_flag_then_the_payload(self, world) -> None:
        await world.streams_write(RUN_ID, NAME, b"payload")
        await world.streams_close(RUN_ID, NAME)

        chunk_dir = world.data_dir / "streams" / "chunks" / NAME
        data, eof = sorted(chunk_dir.iterdir(), key=lambda p: p.name)
        assert data.read_bytes() == b"\x00payload"
        assert eof.read_bytes() == b"\x01"

    async def test_chunk_order_is_file_name_order(self, world) -> None:
        # Names are monotonic ULIDs minted before the write, so lexicographic
        # order is call order even when the writes complete out of order.
        await world.streams_write_multi(RUN_ID, NAME, [str(i).encode() for i in range(20)])
        await world.streams_close(RUN_ID, NAME)
        assert await _drain(world) == [str(i).encode() for i in range(20)]

    async def test_the_run_stream_list_is_ts_readable_json(self, world) -> None:
        await world.streams_write(RUN_ID, NAME, b"x")
        path = world.data_dir / "streams" / "runs" / f"{RUN_ID}.json"
        # Real JSON, two-space indent -- the same `JSON.stringify(…, 2)` shape
        # every other file in the data directory uses.
        assert json.loads(path.read_text()) == {"streams": [NAME]}
        assert path.read_text().startswith('{\n  "streams"')

    async def test_streams_list_reports_every_stream_of_the_run(self, world) -> None:
        await world.streams_write(RUN_ID, NAME, b"x")
        await world.streams_write(RUN_ID, "strm_test_user_bG9ncw", b"y")
        await world.streams_write(RUN_ID, NAME, b"z")  # already registered

        assert await world.streams_list(RUN_ID) == [NAME, "strm_test_user_bG9ncw"]

    async def test_the_registry_is_read_once_per_stream_not_once_per_chunk(
        self, world, monkeypatch
    ) -> None:
        # Registration runs on every append, so without a cache a long stream
        # would re-read and re-parse the registry for every chunk it writes.
        reads = 0
        real_read_json = local_mod.read_json

        def counting_read_json(path, schema):
            nonlocal reads
            if path.parent.name == "runs":
                reads += 1
            return real_read_json(path, schema)

        monkeypatch.setattr(local_mod, "read_json", counting_read_json)

        await world.streams_write_multi(RUN_ID, NAME, [b"a", b"b", b"c"])
        await world.streams_write(RUN_ID, NAME, b"d")
        await world.streams_close(RUN_ID, NAME)

        assert reads == 1
        assert await world.streams_list(RUN_ID) == [NAME]

    async def test_an_unknown_run_has_no_streams(self, world) -> None:
        assert await world.streams_list("wrun_missing") == []

    @pytest.mark.parametrize("name", ["../escape", "a/b", ".hidden", "", "with.dot"])
    async def test_a_stream_name_that_could_escape_the_data_dir_is_refused(
        self, world, name
    ) -> None:
        # The name becomes a directory, and a namespace is caller-chosen text.
        with pytest.raises(local_mod.UnsafeEntityIdError):
            await world.streams_write(RUN_ID, name, b"x")


class TestLiveRead:
    async def test_reads_what_was_already_written_then_ends_at_close(self, world) -> None:
        await world.streams_write(RUN_ID, NAME, b"a")
        await world.streams_write(RUN_ID, NAME, b"b")
        await world.streams_close(RUN_ID, NAME)

        assert await _drain(world) == [b"a", b"b"]

    async def test_waits_for_chunks_that_have_not_been_written_yet(self, world) -> None:
        """The defining property: a read tails, it does not stop at the tail."""
        received: list[bytes] = []

        async def reader() -> None:
            async for chunk in world.streams_get(RUN_ID, NAME):
                received.append(chunk)

        task = asyncio.ensure_future(reader())
        await asyncio.sleep(0.05)
        assert received == [], "nothing written yet"

        await world.streams_write(RUN_ID, NAME, b"late")
        await asyncio.sleep(0.3)
        assert received == [b"late"], "a chunk written after the read started"

        await world.streams_close(RUN_ID, NAME)
        await asyncio.wait_for(task, timeout=5)

    async def test_an_open_stream_keeps_the_reader_waiting(self, world) -> None:
        # Nothing closes a stream implicitly, which is exactly why a workflow
        # that forgets to close leaves its readers hanging.
        await world.streams_write(RUN_ID, NAME, b"a")
        task = asyncio.ensure_future(_drain(world))
        await asyncio.sleep(0.3)
        assert not task.done()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def test_empty_payloads_are_not_delivered(self, world) -> None:
        # An empty transport chunk carries nothing and would look like EOF to a
        # naive consumer, so the read skips it -- while it still holds an index.
        await world.streams_write(RUN_ID, NAME, b"")
        await world.streams_write(RUN_ID, NAME, b"real")
        await world.streams_close(RUN_ID, NAME)

        assert await _drain(world) == [b"real"]

    async def test_start_index_skips_from_the_beginning(self, world) -> None:
        for i in range(5):
            await world.streams_write(RUN_ID, NAME, str(i).encode())
        await world.streams_close(RUN_ID, NAME)

        assert await _drain(world, start_index=2) == [b"2", b"3", b"4"]

    async def test_a_negative_start_index_counts_back_from_the_end(self, world) -> None:
        for i in range(5):
            await world.streams_write(RUN_ID, NAME, str(i).encode())
        await world.streams_close(RUN_ID, NAME)

        # -2 is the last two data chunks: the EOF marker is not one of them, so
        # it must not shift the window.
        assert await _drain(world, start_index=-2) == [b"3", b"4"]

    async def test_a_negative_start_index_past_the_start_is_clamped(self, world) -> None:
        await world.streams_write(RUN_ID, NAME, b"only")
        await world.streams_close(RUN_ID, NAME)

        assert await _drain(world, start_index=-10) == [b"only"]

    async def test_a_start_index_past_the_end_yields_nothing_and_still_ends(self, world) -> None:
        await world.streams_write(RUN_ID, NAME, b"a")
        await world.streams_close(RUN_ID, NAME)

        assert await _drain(world, start_index=50) == []

    async def test_skipped_chunks_are_not_re_delivered_by_the_poll(self, world) -> None:
        """A resumed read must not replay history once it starts polling.

        The poll re-lists the whole directory, so anything the caller asked to
        skip has to be remembered as already handled or it comes back.
        """
        for i in range(3):
            await world.streams_write(RUN_ID, NAME, str(i).encode())

        received: list[bytes] = []

        async def reader() -> None:
            async for chunk in world.streams_get(RUN_ID, NAME, 2):
                received.append(chunk)

        task = asyncio.ensure_future(reader())
        await asyncio.sleep(0.3)
        await world.streams_write(RUN_ID, NAME, b"new")
        await asyncio.sleep(0.3)
        await world.streams_close(RUN_ID, NAME)
        await asyncio.wait_for(task, timeout=5)

        assert received == [b"2", b"new"]

    async def test_abandoning_a_read_stops_its_poll(self, world) -> None:
        await world.streams_write(RUN_ID, NAME, b"a")
        stream = world.streams_get(RUN_ID, NAME)
        async with contextlib.aclosing(stream):
            assert await stream.__anext__() == b"a"
        # Closing the generator is what releases the poll; an abandoned reader
        # would otherwise keep listing the directory for the life of the process.
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()


class TestSnapshot:
    async def test_get_info_reports_the_tail_and_the_closed_flag(self, world) -> None:
        assert await world.streams_get_info(RUN_ID, NAME) == w.StreamInfo(tailIndex=-1, done=False)

        await world.streams_write(RUN_ID, NAME, b"a")
        await world.streams_write(RUN_ID, NAME, b"b")
        assert await world.streams_get_info(RUN_ID, NAME) == w.StreamInfo(tailIndex=1, done=False)

        await world.streams_close(RUN_ID, NAME)
        assert await world.streams_get_info(RUN_ID, NAME) == w.StreamInfo(tailIndex=1, done=True)

    async def test_get_chunks_returns_a_snapshot_not_a_live_read(self, world) -> None:
        for i in range(3):
            await world.streams_write(RUN_ID, NAME, str(i).encode())

        page = await world.streams_get_chunks(RUN_ID, NAME)
        # Returns immediately on an open stream, unlike streams_get.
        assert [(c.index, c.data) for c in page.data] == [(0, b"0"), (1, b"1"), (2, b"2")]
        assert page.has_more is False
        assert page.done is False
        assert page.cursor is None

    async def test_get_chunks_pages_through_with_a_cursor(self, world) -> None:
        for i in range(5):
            await world.streams_write(RUN_ID, NAME, str(i).encode())
        await world.streams_close(RUN_ID, NAME)

        first = await world.streams_get_chunks(RUN_ID, NAME, limit=2)
        assert [c.index for c in first.data] == [0, 1]
        assert first.has_more is True
        assert first.cursor is not None

        second = await world.streams_get_chunks(RUN_ID, NAME, limit=2, cursor=first.cursor)
        assert [c.index for c in second.data] == [2, 3]

        third = await world.streams_get_chunks(RUN_ID, NAME, limit=2, cursor=second.cursor)
        assert [c.index for c in third.data] == [4]
        assert third.has_more is False
        assert third.done is True, "the last page sees the EOF marker"

    async def test_the_cursor_is_the_base64_json_shape_ts_writes(self, world) -> None:
        for i in range(3):
            await world.streams_write(RUN_ID, NAME, str(i).encode())

        page = await world.streams_get_chunks(RUN_ID, NAME, limit=1)
        assert page.cursor is not None
        assert local_mod._decode_chunks_cursor(page.cursor) == 1

    async def test_an_unreadable_cursor_restarts_rather_than_failing(self, world) -> None:
        # A cursor is an opaque token a client round-trips; failing the whole
        # read over a mangled one helps nobody.
        await world.streams_write(RUN_ID, NAME, b"a")
        page = await world.streams_get_chunks(RUN_ID, NAME, cursor="not-base64")
        assert [c.data for c in page.data] == [b"a"]
