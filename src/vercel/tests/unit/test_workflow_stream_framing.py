"""Frame layout on a run's stream, and the naming of the stream itself.

The framing is a wire format shared with `@workflow/core`, so these tests pin
the bytes rather than a round trip through our own code: a round trip stays
green while both ends drift together.
"""

from __future__ import annotations

import base64

import pytest

from vercel._internal.workflow import serialization as ser, streams


def test_frame_is_a_four_byte_big_endian_length_then_the_payload() -> None:
    assert streams.encode_frame(b"hello") == b"\x00\x00\x00\x05hello"
    assert streams.encode_frame(b"") == b"\x00\x00\x00\x00"


def test_value_frame_carries_a_devl_payload() -> None:
    # The payload is exactly what the run/step payload boundary writes, which
    # is what lets `getDeserializeStream` on the TS side read it.
    frame = streams.encode_value({"hi": 1})
    payload = ser.dehydrate({"hi": 1})
    assert frame == len(payload).to_bytes(4, "big") + payload
    assert payload.startswith(b"devl")


def test_oversized_payload_is_refused_rather_than_framed() -> None:
    with pytest.raises(ser.SerializationError, match="exceeds the maximum frame size"):
        streams.encode_frame(b"x" * (streams.MAX_FRAME_SIZE + 1))


class TestFrameDecoder:
    """The transport picks its own read boundaries; the decoder absorbs that."""

    def test_one_read_holding_several_frames(self) -> None:
        decoder = streams.FrameDecoder()
        wire = streams.encode_frame(b"a") + streams.encode_frame(b"bb")
        assert list(decoder.feed(wire)) == [b"a", b"bb"]
        decoder.finish()

    def test_one_frame_split_across_reads(self) -> None:
        decoder = streams.FrameDecoder()
        wire = streams.encode_frame(b"payload")
        # Split inside the header, then inside the payload.
        assert list(decoder.feed(wire[:2])) == []
        assert list(decoder.feed(wire[2:6])) == []
        assert list(decoder.feed(wire[6:])) == [b"payload"]
        decoder.finish()

    def test_a_frame_straddling_a_read_boundary_is_delivered_whole(self) -> None:
        decoder = streams.FrameDecoder()
        wire = streams.encode_frame(b"one") + streams.encode_frame(b"two")
        cut = 4 + 3 + 2  # first frame plus part of the second's header
        assert list(decoder.feed(wire[:cut])) == [b"one"]
        assert list(decoder.feed(wire[cut:])) == [b"two"]
        decoder.finish()

    def test_empty_frames_survive_the_round_trip(self) -> None:
        # A zero-length frame is a real chunk and holds an index, so it cannot
        # be skipped the way an empty transport read can.
        decoder = streams.FrameDecoder()
        wire = streams.encode_frame(b"") + streams.encode_frame(b"x")
        assert list(decoder.feed(wire)) == [b"", b"x"]

    def test_truncation_mid_frame_is_reported_not_dropped(self) -> None:
        decoder = streams.FrameDecoder()
        list(decoder.feed(streams.encode_frame(b"abcdef")[:-2]))
        assert decoder.pending == 8
        with pytest.raises(ser.SerializationError, match="truncated mid-frame"):
            decoder.finish()

    def test_an_absurd_length_header_is_refused_before_allocating(self) -> None:
        # The likely cause is a raw stream being read as framed, not a 100MB+
        # chunk, so this must not become a giant buffer.
        decoder = streams.FrameDecoder()
        with pytest.raises(ser.SerializationError, match="exceeds the maximum"):
            list(decoder.feed(b"\xff\xff\xff\xff"))


class TestStreamId:
    def test_default_stream_replaces_the_run_prefix(self) -> None:
        assert streams.workflow_run_stream_id("wrun_abc") == "strm_abc_user"

    def test_only_the_first_prefix_occurrence_is_replaced(self) -> None:
        # JS `String.replace` with a string pattern is first-match-only, and the
        # name has to match byte for byte or the two SDKs address different keys.
        assert streams.workflow_run_stream_id("wrun_wrun_x") == "strm_wrun_x_user"

    def test_namespace_is_unpadded_base64url(self) -> None:
        name = streams.workflow_run_stream_id("wrun_abc", "logs")
        encoded = name.rsplit("_", 1)[-1]
        assert name == f"strm_abc_user_{encoded}"
        assert "=" not in encoded
        assert base64.urlsafe_b64decode(encoded + "==") == b"logs"

    def test_namespace_with_characters_a_key_could_not_hold(self) -> None:
        # The point of encoding: a namespace reaches Redis keys on the Vercel
        # world, so slashes and non-ASCII cannot go through raw.
        name = streams.workflow_run_stream_id("wrun_abc", "a/b?ü")
        encoded = name.rsplit("_", 1)[-1]
        assert set(encoded) <= set(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        )
        assert base64.urlsafe_b64decode(encoded + "===")[: len("a/b")] == b"a/b"

    def test_empty_namespace_is_the_default_stream(self) -> None:
        assert streams.workflow_run_stream_id("wrun_abc", "") == "strm_abc_user"
        assert streams.workflow_run_stream_id("wrun_abc", None) == "strm_abc_user"


class TestBytesChunks:
    """A `bytes` chunk goes out as a `Uint8Array`.

    The TypeScript pattern for consuming a run's stream pipes it straight into a
    `Response`, and a body stream takes `Uint8Array` chunks only -- an
    `ArrayBuffer` raises `Received non-Uint8Array chunk`.
    """

    def _wire(self, frame: bytes) -> str:
        return frame[streams.FRAME_HEADER_SIZE + len(ser.DEVALUE_V1) :].decode()

    def test_a_chunk_is_a_view_onto_its_own_buffer(self) -> None:
        assert (
            self._wire(streams.encode_value(b"hi")) == '[["Uint8Array",1],["ArrayBuffer","aGk="]]'
        )

    def test_bytes_nested_in_a_chunk_are_views_too(self) -> None:
        # `run.readable` hands whole chunks to a consumer, so a `bytes` field
        # reaches the same `Response` the top-level one would.
        assert self._wire(streams.encode_value({"blob": b"hi"})) == (
            '[{"blob":1},["Uint8Array",2],["ArrayBuffer","aGk="]]'
        )

    def test_it_reads_back_as_bytes(self) -> None:
        frame = streams.encode_value(b"round trip")
        (payload,) = streams.FrameDecoder().feed(frame)
        assert ser.hydrate(payload, what="chunk") == b"round trip"

    def test_an_empty_chunk_survives(self) -> None:
        frame = streams.encode_value(b"")
        (payload,) = streams.FrameDecoder().feed(frame)
        assert ser.hydrate(payload, what="chunk") == b""

    def test_a_repeated_buffer_is_still_written_once(self) -> None:
        shared = b"xy"
        wire = self._wire(streams.encode_value([shared, shared]))
        assert wire.count("ArrayBuffer") == 1
