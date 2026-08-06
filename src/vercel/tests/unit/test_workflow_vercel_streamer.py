"""`VercelWorld`'s stream transport: endpoints, headers, and what may be retried.

The retry policy is the load-bearing part. A chunk append is not idempotent, so
resending one that may already have landed duplicates it and shifts every index
after it -- which silently breaks resume for every reader. Close is idempotent
and must retry 5xx, because the server's close barrier surfaces transient
reconciliation as a retriable 503.
"""

from __future__ import annotations

import cbor2
import httpx
import pytest
import respx

from vercel._internal.workflow import world as w
from vercel._internal.workflow.worlds.vercel import VercelWorld

RUN_ID = "wrun_test"
NAME = "strm_test_user"
BASE = "https://vercel-workflow.com/api"
STREAM_URL = f"{BASE}/v2/runs/{RUN_ID}/stream/{NAME}"
READ_URL = f"{BASE}/v3/runs/{RUN_ID}/stream/{NAME}"


@pytest.fixture(autouse=True)
def _no_ambient_auth(monkeypatch) -> None:
    # A token short-circuits the OIDC lookup, which would otherwise try the
    # network from a unit test.
    monkeypatch.delenv("VERCEL_OIDC_TOKEN", raising=False)


@pytest.fixture
def world() -> VercelWorld:
    return VercelWorld(token="tok_test")


@pytest.fixture(autouse=True)
def _fast_retries(monkeypatch) -> None:
    """Collapse the retry backoff so the retry tests do not sleep for real."""
    import asyncio

    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)


class TestWrite:
    @respx.mock
    async def test_a_single_chunk_is_put_to_the_v2_stream_endpoint(self, world) -> None:
        route = respx.put(STREAM_URL).respond(200)

        await world.streams_write(RUN_ID, NAME, b"hello")

        assert route.called
        request = route.calls.last.request
        assert request.content == b"hello"
        assert request.headers["Authorization"] == "Bearer tok_test"
        assert "X-Stream-Multi" not in request.headers
        assert "X-Stream-Done" not in request.headers

    @respx.mock
    async def test_a_batch_is_length_prefixed_and_flagged(self, world) -> None:
        route = respx.put(STREAM_URL).respond(200)

        await world.streams_write_multi(RUN_ID, NAME, [b"ab", b"c"])

        request = route.calls.last.request
        assert request.headers["X-Stream-Multi"] == "true"
        # The server has to recover the original boundaries so each chunk keeps
        # its own index -- hence the same framing the stream itself uses.
        assert request.content == b"\x00\x00\x00\x02ab\x00\x00\x00\x01c"

    @respx.mock
    async def test_an_empty_batch_makes_no_request(self, world) -> None:
        route = respx.put(STREAM_URL).respond(200)
        await world.streams_write_multi(RUN_ID, NAME, [])
        assert not route.called

    @respx.mock
    async def test_a_batch_over_the_server_cap_is_split_into_pages(self, world) -> None:
        route = respx.put(STREAM_URL).respond(200)

        await world.streams_write_multi(RUN_ID, NAME, [b"x"] * 1500)

        assert len(route.calls) == 2
        # 1000 then 500, each `[4-byte len][1 byte]`.
        assert len(route.calls[0].request.content) == 1000 * 5
        assert len(route.calls[1].request.content) == 500 * 5

    @respx.mock
    async def test_429_is_retried(self, world) -> None:
        # The request was rejected before being applied, so resending cannot
        # duplicate a chunk.
        route = respx.put(STREAM_URL).mock(
            side_effect=[httpx.Response(429, headers={"retry-after": "0"}), httpx.Response(200)]
        )

        await world.streams_write(RUN_ID, NAME, b"x")

        assert len(route.calls) == 2

    @respx.mock
    async def test_500_is_not_retried_on_a_write(self, world) -> None:
        """The whole point of the narrowed policy.

        A 5xx may mean the append *did* land; resending it would store the chunk
        twice and shift every later index.
        """
        route = respx.put(STREAM_URL).respond(500)

        with pytest.raises(w.WorkflowWorldError, match="Stream write failed: HTTP 500"):
            await world.streams_write(RUN_ID, NAME, b"x")

        assert len(route.calls) == 1

    @respx.mock
    async def test_a_transport_error_is_retried(self, world) -> None:
        # Never reached a server that could have applied it.
        route = respx.put(STREAM_URL).mock(
            side_effect=[httpx.ConnectError("reset"), httpx.Response(200)]
        )

        await world.streams_write(RUN_ID, NAME, b"x")

        assert len(route.calls) == 2

    @respx.mock
    async def test_persistent_throttling_surfaces_as_throttle_error(self, world) -> None:
        respx.put(STREAM_URL).respond(429, headers={"retry-after": "7"})

        with pytest.raises(w.ThrottleError) as error:
            await world.streams_write(RUN_ID, NAME, b"x")

        assert error.value.retry_after == 7

    @respx.mock
    async def test_the_error_carries_the_vercel_request_id(self, world) -> None:
        # Without it a failure inside the platform is unactionable.
        respx.put(STREAM_URL).respond(
            503, headers={"x-vercel-id": "iad1::abc", "x-vercel-error": "FUNCTION_THROTTLED"}
        )

        with pytest.raises(w.WorkflowWorldError, match="x-vercel-id=iad1::abc") as error:
            await world.streams_write(RUN_ID, NAME, b"x")

        assert "x-vercel-error=FUNCTION_THROTTLED" in str(error.value)


class TestClose:
    @respx.mock
    async def test_close_flags_done_and_sends_no_body(self, world) -> None:
        route = respx.put(STREAM_URL).respond(200)

        await world.streams_close(RUN_ID, NAME)

        request = route.calls.last.request
        assert request.headers["X-Stream-Done"] == "true"
        assert request.content == b""

    @respx.mock
    async def test_500_is_retried_on_close(self, world) -> None:
        """Closing twice is harmless, and the server's close barrier expects it:
        a transient reconciliation failure comes back as a retriable 503 with
        the stream left durably closing."""
        route = respx.put(STREAM_URL).mock(side_effect=[httpx.Response(503), httpx.Response(200)])

        await world.streams_close(RUN_ID, NAME)

        assert len(route.calls) == 2


class TestRead:
    @respx.mock
    async def test_the_live_read_uses_v3(self, world) -> None:
        """v3, not v2, and it matters.

        v3 errors the response body when the server's max duration expires
        where v2 closes it cleanly. Only the error distinguishes a timeout from
        the end of the stream, so reading v2 would silently truncate any stream
        that outlives the limit.
        """
        route = respx.get(READ_URL).respond(200, content=b"chunk")

        chunks = [c async for c in world.streams_get(RUN_ID, NAME)]

        assert chunks == [b"chunk"]
        assert route.called
        assert "/v3/" in str(route.calls.last.request.url)

    @respx.mock
    async def test_start_index_is_a_query_parameter(self, world) -> None:
        route = respx.get(READ_URL).respond(200, content=b"")

        [c async for c in world.streams_get(RUN_ID, NAME, 7)]

        assert route.calls.last.request.url.params["startIndex"] == "7"

    @respx.mock
    async def test_a_negative_start_index_is_passed_through(self, world) -> None:
        # Last-N is resolved by the server, which is the only side that knows
        # the current tail.
        route = respx.get(READ_URL).respond(200, content=b"")

        [c async for c in world.streams_get(RUN_ID, NAME, -3)]

        assert route.calls.last.request.url.params["startIndex"] == "-3"

    @respx.mock
    async def test_no_start_index_means_no_parameter(self, world) -> None:
        route = respx.get(READ_URL).respond(200, content=b"")

        [c async for c in world.streams_get(RUN_ID, NAME)]

        assert "startIndex" not in route.calls.last.request.url.params

    @respx.mock
    async def test_the_leading_header_flush_chunk_is_skipped(self, world) -> None:
        # The server writes an empty chunk to commit response headers before any
        # data exists; it is not a chunk anyone wrote.
        respx.get(READ_URL).respond(
            200, stream=httpx.ByteStream(b""), content=b"".join([b"", b"real"])
        )

        chunks = [c async for c in world.streams_get(RUN_ID, NAME)]

        assert b"" not in chunks

    @respx.mock
    async def test_a_failed_read_raises_before_yielding(self, world) -> None:
        respx.get(READ_URL).respond(404)

        with pytest.raises(w.WorkflowWorldError, match="Stream read failed: HTTP 404"):
            [c async for c in world.streams_get(RUN_ID, NAME)]


class TestSnapshot:
    @respx.mock
    async def test_get_info_reads_the_v2_info_endpoint(self, world) -> None:
        respx.get(f"{BASE}/v2/runs/{RUN_ID}/streams/{NAME}/info").respond(
            200, json={"tailIndex": 4, "done": True}
        )

        assert await world.streams_get_info(RUN_ID, NAME) == w.StreamInfo(tailIndex=4, done=True)

    @respx.mock
    async def test_get_chunks_passes_paging_and_parses_the_page(self, world) -> None:
        # CBOR, as the server really answers: chunk payloads arrive as native
        # byte strings, so nothing base64-decodes them on the way in.
        route = respx.get(f"{BASE}/v2/runs/{RUN_ID}/streams/{NAME}/chunks").respond(
            200,
            headers={"Content-Type": "application/cbor"},
            content=cbor2.dumps(
                {
                    "data": [{"index": 0, "data": b"hi"}],
                    "cursor": "next",
                    "hasMore": True,
                    "done": False,
                }
            ),
        )

        page = await world.streams_get_chunks(RUN_ID, NAME, limit=1, cursor="here")

        params = route.calls.last.request.url.params
        assert params["limit"] == "1"
        assert params["cursor"] == "here"
        assert page.has_more is True
        assert page.cursor == "next"
        assert [(c.index, c.data) for c in page.data] == [(0, b"hi")]

    @respx.mock
    async def test_list_returns_the_run_stream_names(self, world) -> None:
        respx.get(f"{BASE}/v2/runs/{RUN_ID}/streams").respond(200, json=[NAME, "strm_other"])

        assert await world.streams_list(RUN_ID) == [NAME, "strm_other"]


class TestNaming:
    @respx.mock
    async def test_path_segments_are_percent_encoded(self, world) -> None:
        # A namespace is base64url, so `-` and `_` show up in real names; the
        # encoding is what keeps a surprising one from reshaping the URL.
        weird = "strm_a b/c"
        route = respx.put(f"{BASE}/v2/runs/{RUN_ID}/stream/strm_a%20b%2Fc").respond(200)

        await world.streams_write(RUN_ID, weird, b"x")

        assert route.called
