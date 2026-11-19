"""Vercel v4 event framing, full-log reads, and interruption recovery."""

from collections.abc import AsyncIterator

import anyio
import cbor2
import httpx2 as httpx
import pytest
from hypothesis import given, strategies as st

import vendor.respx as respx
from vercel.workflow._internal import world as w
from vercel.workflow._internal.worlds import event_frames as frames
from vercel.workflow._internal.worlds.vercel import VercelWorld


def event(index=1, **overrides):
    return {
        "eventType": "run_started",
        "eventId": f"evnt_{index}",
        "runId": "wrun_test",
        "createdAt": "2026-01-01T00:00:00Z",
        "specVersion": 5,
    } | overrides


def end(index=1, *, more=False):
    return frames.encode_frame({"_end": 1, "next": f"eid:evnt_{index}", "hasMore": more})


def response(body):
    return httpx.Response(200, content=body, headers={"content-type": frames.CONTENT_TYPE})


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks, *, error=None):
        self.chunks = chunks
        self.error = error
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.error:
            raise self.error

    async def aclose(self):
        self.closed = True


# @lat: [[vercel-workflow#Worlds#Vercel event streams]]
@given(split=st.integers(min_value=0, max_value=1000))
async def test_arbitrary_chunk_boundaries_preserve_opaque_payloads(split):
    payload = b"zstd\x00\xff\x00opaque"
    wire = (
        frames.encode_frame(
            event(
                eventType="run_created",
                eventData={
                    "deploymentId": "dpl_test",
                    "workflowName": "test",
                },
            ),
            payload,
        )
        + end()
    )
    offset = split % (len(wire) + 1)

    async def chunks():
        for value in [wire[:offset], b"", wire[offset:]]:
            yield value

    events: list[w.Event] = []
    sentinel = await frames.consume_event_stream(chunks(), events)
    assert isinstance(events[0], w.RunCreatedEvent)
    assert events[0].event_data.input == payload
    assert sentinel.next == "eid:evnt_1" and not sentinel.has_more


@respx.mock
@pytest.mark.parametrize("limit", [None, 2])
async def test_omitted_limit_requests_all_and_explicit_limit_requests_one_page(limit):
    world = VercelWorld(token="test")
    route = respx.get(url__startswith=f"{world._base_url}/v4/runs/wrun_test/events").mock(
        return_value=response(frames.encode_frame(event()) + end(more=True))
    )
    result = await world.events_list(
        "wrun_test",
        pagination=w.PaginationOptions(cursor="eid:before", limit=limit, sort_order="asc"),
    )
    params = route.calls[0].request.url.params
    assert params["cursor"] == "eid:before"
    assert params["sortOrder"] == "asc" and params["remoteRefBehavior"] == "resolve"
    assert params.get("returnAll") == ("true" if limit is None else None)
    assert params.get("limit") == (str(limit) if limit else None)
    assert result.has_more and result.cursor == "eid:evnt_1"
    assert route.call_count == 1


@respx.mock
@pytest.mark.parametrize("transport_error", [False, True])
async def test_full_read_resumes_after_last_validated_frame(transport_error):
    world = VercelWorld(token="test")
    prefix = frames.encode_frame(event())
    stream = Chunks(
        [prefix, b"\x00\x00"], error=httpx.ReadError("cut") if transport_error else None
    )
    route = respx.get(url__startswith=f"{world._base_url}/v4/runs/wrun_test/events").mock(
        side_effect=[
            httpx.Response(200, stream=stream, headers={"content-type": frames.CONTENT_TYPE}),
            response(frames.encode_frame(event(2)) + end(2)),
        ]
    )
    result = await world.events_list("wrun_test")
    assert [e.server_props.event_id for e in result.data if e.server_props] == ["evnt_1", "evnt_2"]
    assert result.cursor == "eid:evnt_2" and not result.has_more
    assert route.calls[1].request.url.params["cursor"] == "eid:evnt_1"
    assert stream.closed


@respx.mock
async def test_partial_read_retry_budget_is_finite():
    world = VercelWorld(token="test")
    route = respx.get(url__startswith=f"{world._base_url}/v4/runs/wrun_test/events").mock(
        side_effect=[response(frames.encode_frame(event(i))) for i in range(1, 4)]
    )
    with pytest.raises(frames.IncompleteFrameError):
        await world.events_list("wrun_test")
    assert route.call_count == 3


@respx.mock
async def test_a_nonadvancing_partial_stream_is_not_retried():
    world = VercelWorld(token="test")
    route = respx.get(url__startswith=f"{world._base_url}/v4/runs/wrun_test/events").mock(
        return_value=response(frames.encode_frame(event()))
    )
    with pytest.raises(frames.IncompleteFrameError):
        await world.events_list("wrun_test", pagination=w.PaginationOptions(cursor="eid:evnt_1"))
    assert route.call_count == 1


@respx.mock
async def test_explicit_page_never_silently_resumes_a_truncated_response():
    world = VercelWorld(token="test")
    route = respx.get(url__startswith=f"{world._base_url}/v4/runs/wrun_test/events").mock(
        return_value=response(frames.encode_frame(event()))
    )
    with pytest.raises(frames.IncompleteFrameError):
        await world.events_list("wrun_test", pagination=w.PaginationOptions(limit=2))
    assert route.call_count == 1


@respx.mock
@pytest.mark.parametrize(
    "bad",
    [
        frames.encode_frame({"_error": 1, "code": "payload-missing"}),
        frames.encode_frame({"eventType": "unknown"}),
        frames.encode_frame({"_end": 1, "hasMore": "false"}),
        frames.encode_frame({"_end": 1, "hasMore": True}),
        b"\x00\x00\x00\x01\x80\x00\x00\x00\x00",  # CBOR array, not metadata map
        (frames.MAX_META_SIZE + 1).to_bytes(4, "big"),
        frames.encode_frame({"eventType": "run_started"}, b"forbidden"),
    ],
)
async def test_protocol_errors_after_a_valid_prefix_are_not_retried(bad):
    world = VercelWorld(token="test")
    stream = Chunks([frames.encode_frame(event()), bad])
    route = respx.get(url__startswith=f"{world._base_url}/v4/runs/wrun_test/events").mock(
        return_value=httpx.Response(
            200, stream=stream, headers={"content-type": frames.CONTENT_TYPE}
        )
    )
    with pytest.raises(w.WorkflowWorldError):
        await world.events_list("wrun_test")
    assert route.call_count == 1 and stream.closed


@pytest.mark.parametrize(
    "payload", [b"devl[0]", b"encr\x00bytes", b"gzip\x00bytes", b"zstd\x00bytes"]
)
def test_error_payload_prefixes_remain_opaque(payload):
    decoded = frames.decode_event(
        event(eventType="step_failed", correlationId="step_1", eventData={}), payload
    )
    assert isinstance(decoded, w.StepFailedEvent)
    assert decoded.event_data.error == payload


def test_legacy_structured_errors_are_decoded():
    error = {"message": "old failure", "name": "Error", "stack": "stack"}
    decoded = frames.decode_event(
        event(eventType="step_failed", correlationId="step_1", eventData={}), cbor2.dumps(error)
    )
    assert isinstance(decoded, w.StepFailedEvent)
    assert decoded.event_data.error == error


async def test_cbor_x_date_and_map_wire_fixture():
    # Generated with cbor-x from ~/workflow, using frames.ts's length prefixes.
    wire = bytes.fromhex(
        "00000055b90005696576656e74547970656b72756e5f73746172746564676576656e744964"
        "6665766e745f316572756e4964697772756e5f7465737469637265617465644174c11a6955"
        "b9006b7370656356657273696f6e050000000000000022b90003645f656e6401646e657874"
        "6a6569643a65766e745f31676861734d6f7265f400000000"
    )

    async def chunks():
        for byte in wire:
            yield bytes([byte])

    events: list[w.Event] = []
    sentinel = await frames.consume_event_stream(chunks(), events)
    assert events[0].server_props is not None
    assert events[0].server_props.created_at.isoformat() == "2026-01-01T00:00:00+00:00"
    assert sentinel.next == "eid:evnt_1"


@respx.mock
async def test_wrong_content_type_is_not_treated_as_a_truncated_stream():
    world = VercelWorld(token="test")
    route = respx.get(url__startswith=f"{world._base_url}/v4/runs/wrun_test/events").respond(
        200, content=b"unexpected", headers={"content-type": "application/cbor"}
    )
    with pytest.raises(w.WorkflowWorldError, match="Expected v4"):
        await world.events_list("wrun_test")
    assert route.call_count == 1


@respx.mock
async def test_cancellation_closes_the_response_without_resuming():
    world = VercelWorld(token="test")

    class Waiting(Chunks):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield frames.encode_frame(event())
            await anyio.sleep_forever()

    stream = Waiting([])
    route = respx.get(url__startswith=f"{world._base_url}/v4/runs/wrun_test/events").mock(
        return_value=httpx.Response(
            200, stream=stream, headers={"content-type": frames.CONTENT_TYPE}
        )
    )
    with anyio.move_on_after(0.05) as scope:
        await world.events_list("wrun_test")
    assert scope.cancel_called and stream.closed and route.call_count == 1
