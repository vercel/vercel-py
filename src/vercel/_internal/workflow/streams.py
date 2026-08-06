"""Run-scoped streams: the wire framing, and the writer steps use.

A run's stream is an append-only, indexed log of chunks that a reader can tail
live and resume at an index. Steps write to it with
:func:`vercel.workflow.get_writable`; anything holding the run id reads it back
through the world's ``streams_get``.

Each user write becomes exactly one length-prefixed frame, and each frame is
stored under exactly one chunk index::

    [4-byte big-endian length][format-prefixed payload]

That one-write-one-frame-one-index correspondence is what makes
``start_index + frames_consumed`` a correct resume position, so nothing along
this path may coalesce or split frames. The payload is whatever
:mod:`.serialization` writes -- a ``devl``-prefixed devalue payload -- which is
the format `@workflow/core`'s ``getDeserializeStream`` reads, so a stream
written here is readable by the TypeScript SDK, the ``workflow`` CLI and the
dashboard. The length header stays outside the payload so a reader can find
frame boundaries without understanding the payload format at all.
"""

from __future__ import annotations

import base64
from collections.abc import Iterator
from typing import Any

from . import serialization as ser

FRAME_HEADER_SIZE = 4
"""Bytes of big-endian length prefix in front of every frame."""

MAX_FRAME_SIZE = 100_000_000
"""Largest single frame payload, matching `@workflow/core`'s ``MAX_FRAME_SIZE``.

A length header advertising more than this is refused rather than allocated:
past a certain size the far more likely explanation is a misframed wire than a
100 MB chunk.
"""


def workflow_run_stream_id(run_id: str, namespace: str | None = None) -> str:
    """The name of a run's default user stream.

    Mirrors `@workflow/core`'s ``getWorkflowRunStreamId``. A namespace is
    base64url-encoded because it reaches Redis keys on the Vercel world, where
    arbitrary user text is not safe as a key segment.
    """
    # JS `String.replace` with a string pattern replaces the first match only.
    name = run_id.replace("wrun_", "strm_", 1) + "_user"
    if not namespace:
        return name
    encoded = base64.urlsafe_b64encode(namespace.encode()).decode("ascii").rstrip("=")
    return f"{name}_{encoded}"


def encode_frame(payload: bytes) -> bytes:
    """Wrap *payload* in its length header."""
    if len(payload) > MAX_FRAME_SIZE:
        raise ser.SerializationError(
            f"Stream chunk of {len(payload)} bytes exceeds the maximum frame size "
            f"({MAX_FRAME_SIZE}); split the data into smaller chunks before writing"
        )
    return len(payload).to_bytes(FRAME_HEADER_SIZE, "big") + payload


def encode_value(value: Any) -> bytes:
    """The frame a single user write becomes."""
    return encode_frame(ser.dehydrate(value))


class FrameDecoder:
    """Reassembles frames from transport reads of arbitrary size.

    The transport decides where its own read boundaries fall: one frame may
    arrive split across three reads, and three frames may arrive in one. Feed
    every read in and take whatever whole frames come out.
    """

    __slots__ = ("_buffer",)

    def __init__(self) -> None:
        self._buffer = bytearray()

    @property
    def pending(self) -> int:
        """Bytes buffered as part of a frame that has not fully arrived."""
        return len(self._buffer)

    def feed(self, data: bytes) -> Iterator[bytes]:
        """Yield the payload of every frame completed by *data*."""
        self._buffer += data
        while len(self._buffer) >= FRAME_HEADER_SIZE:
            length = int.from_bytes(self._buffer[:FRAME_HEADER_SIZE], "big")
            if length > MAX_FRAME_SIZE:
                raise ser.SerializationError(
                    f"Stream frame length {length} exceeds the maximum ({MAX_FRAME_SIZE}); "
                    f"this usually means a non-framed stream is being read as framed"
                )
            end = FRAME_HEADER_SIZE + length
            if len(self._buffer) < end:
                return
            yield bytes(self._buffer[FRAME_HEADER_SIZE:end])
            del self._buffer[:end]

    def finish(self) -> None:
        """Assert the stream ended on a frame boundary.

        Leftover bytes mean the stream was truncated mid-frame. Reporting that
        beats silently dropping the tail, which would look like a short stream.
        """
        if self._buffer:
            raise ser.SerializationError(
                f"Stream ended with {len(self._buffer)} bytes of incomplete frame data; "
                f"it was truncated mid-frame"
            )
