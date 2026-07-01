"""Runtime-neutral seekable Blob reader engines."""

import base64
import codecs
import hashlib
import hmac
import io
import json
import secrets
import threading
from collections.abc import Callable
from typing import Any

from vercel._internal.blob.types import Access
from vercel._internal.unstable.blob.api_client import BlobApiClient
from vercel._internal.unstable.blob.errors import (
    BlobPreconditionFailedError,
    BlobStreamError,
)
from vercel._internal.unstable.blob.models import BlobRangeResponse, BlobStatResult

_COOKIE_PREFIX = b"VBR2:"
_COOKIE_SIGNATURE_SIZE = hashlib.sha256().digest_size


class BlobReaderState:
    """One ETag-pinned binary cursor shared by the sync and async facades."""

    def __init__(
        self,
        *,
        stat: BlobStatResult,
        api_client: BlobApiClient,
        access: Access,
        read_buffer_size: int,
        ensure_open: Callable[[], None],
    ) -> None:
        self.stat = stat
        self._api_client = api_client
        self._access = access
        self._read_buffer_size = read_buffer_size
        self._ensure_open = ensure_open
        self._position = 0
        self._buffer = b""
        self._buffer_start = 0
        self._closed = False
        self._broken: BaseException | None = None
        self._active_response: BlobRangeResponse | None = None
        self._lifecycle_lock = threading.RLock()

    @property
    def closed(self) -> bool:
        """Whether the reader state has been closed."""
        with self._lifecycle_lock:
            return self._closed

    def tell(self) -> int:
        """Return the current binary byte offset."""
        self._check_io()
        return self._position

    def _check_io(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                raise ValueError("I/O operation on closed Blob reader")
            if self._broken is not None:
                raise self._broken

    def _register_response(self, response: BlobRangeResponse) -> bool:
        with self._lifecycle_lock:
            if self._closed:
                return False
            self._active_response = response
            return True

    def _release_response(self, response: BlobRangeResponse) -> None:
        with self._lifecycle_lock:
            if self._active_response is response:
                self._active_response = None

    def _check_operation(self) -> None:
        self._check_io()
        self._ensure_open()

    async def _read_range_into(self, start: int, length: int, target: bytearray) -> None:
        self._check_io()
        if start >= self.stat.size or length <= 0:
            return
        end = min(self.stat.size, start + length) - 1
        try:
            response = await self._api_client.read_range(
                self.stat,
                access=self._access,
                start=start,
                end=end,
            )
        except BlobPreconditionFailedError as exc:
            with self._lifecycle_lock:
                self._broken = exc
            raise

        if not self._register_response(response):
            await response.aclose()
            raise ValueError("I/O operation on closed Blob reader")

        expected = end - start + 1
        received = 0
        try:
            async for chunk in response:
                if self._closed:
                    raise ValueError("I/O operation on closed Blob reader")
                received += len(chunk)
                if received > expected:
                    raise BlobStreamError("Blob delivery returned more bytes than requested")
                target.extend(chunk)
        finally:
            self._release_response(response)
            await response.aclose()
        self._check_io()
        if received != expected:
            raise BlobStreamError("Blob delivery ended before the requested range was complete")

    async def _fetch_window(self) -> bytes:
        body = bytearray()
        await self._read_range_into(self._position, self._read_buffer_size, body)
        return bytes(body)

    def _buffered(self) -> bytes:
        offset = self._position - self._buffer_start
        if offset < 0 or offset >= len(self._buffer):
            return b""
        return self._buffer[offset:]

    async def _ensure_buffer(self) -> bytes:
        buffered = self._buffered()
        if buffered or self._position >= self.stat.size:
            return buffered
        self._buffer_start = self._position
        self._buffer = await self._fetch_window()
        return self._buffer

    async def read(self, size: int | None = -1) -> bytes:
        """Read bytes from the current ETag-pinned cursor.

        Args:
            size: Maximum number of bytes to read, or ``-1`` for all remaining
                bytes.

        Returns:
            Bytes read from the object.
        """
        self._check_operation()
        return await self._read(size)

    async def _read(self, size: int | None = -1) -> bytes:
        if size is None:
            size = -1
        if not isinstance(size, int):
            raise TypeError("size must be an integer")
        if size < -1:
            size = -1
        if size == 0 or self._position >= self.stat.size:
            return b""
        remaining = self.stat.size - self._position
        wanted = remaining if size < 0 else min(size, remaining)
        result = bytearray()
        while len(result) < wanted:
            need = wanted - len(result)
            buffered = self._buffered()
            if not buffered and need > self._read_buffer_size:
                await self._read_range_into(self._position, need, result)
                self._position += need
                continue
            buffered = await self._ensure_buffer()
            if not buffered:
                break
            take = min(need, len(buffered))
            result.extend(buffered[:take])
            self._position += take
        return bytes(result)

    async def readinto(self, buffer: Any) -> int:
        """Read bytes into a writable bytes-like object.

        Args:
            buffer: Writable buffer to fill.

        Returns:
            Number of bytes copied into the buffer.
        """
        self._check_operation()
        view = memoryview(buffer)
        if view.readonly:
            raise TypeError("readinto() argument must be read-write bytes-like object")
        try:
            target = view.cast("B")
            data = await self._read(len(target))
            target[: len(data)] = data
            return len(data)
        finally:
            view.release()

    async def readline(self, size: int | None = -1) -> bytes:
        """Read one binary line from the current cursor.

        Args:
            size: Maximum number of bytes to read, or ``-1`` for an unbounded
                line.

        Returns:
            The next line, including the newline byte when present.
        """
        self._check_operation()
        if size is None:
            size = -1
        if not isinstance(size, int):
            raise TypeError("size must be an integer")
        if size == 0:
            return b""
        limit = None if size < 0 else size
        result = bytearray()
        while limit is None or len(result) < limit:
            buffered = await self._ensure_buffer()
            if not buffered:
                break
            available = len(buffered)
            if limit is not None:
                available = min(available, limit - len(result))
            newline = buffered.find(b"\n", 0, available)
            take = newline + 1 if newline >= 0 else available
            result.extend(buffered[:take])
            self._position += take
            if newline >= 0:
                break
        return bytes(result)

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move the binary cursor.

        Args:
            offset: Byte offset interpreted relative to ``whence``.
            whence: Standard ``io`` seek mode.

        Returns:
            New absolute byte offset.
        """
        self._check_operation()
        return self._seek(offset, whence)

    def _seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if not isinstance(offset, int):
            raise TypeError("offset must be an integer")
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_CUR:
            position = self._position + offset
        elif whence == io.SEEK_END:
            position = self.stat.size + offset
        else:
            raise ValueError(f"invalid whence ({whence!r})")
        if position < 0:
            raise ValueError("negative seek position")
        self._position = position
        return position

    async def close(self) -> None:
        """Close the reader and release any active range response."""
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            self._buffer = b""
            response = self._active_response
            self._active_response = None
        if response is not None:
            await response.aclose()


class BlobTextReaderState:
    """Decoded cursor with bounded, self-contained integer seek cookies."""

    def __init__(
        self,
        binary: BlobReaderState,
        *,
        encoding: str,
        errors: str,
        newline: str | None,
    ) -> None:
        if newline not in (None, "", "\n", "\r", "\r\n"):
            raise ValueError(f"illegal newline value: {newline!r}")
        self.binary = binary
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self._decoder: Any = self._new_decoder()
        self._text = ""
        self._eof = False
        self._cookie_secret = secrets.token_bytes(32)
        self._max_cookie_bytes = max(4096, binary._read_buffer_size * 12 + 1024)

    def _new_decoder(self) -> Any:
        decoder = codecs.getincrementaldecoder(self.encoding)(errors=self.errors)
        if self.newline in (None, ""):
            return io.IncrementalNewlineDecoder(decoder, translate=self.newline is None)
        return decoder

    @property
    def closed(self) -> bool:
        """Whether the underlying binary reader has been closed."""
        return self.binary.closed

    @property
    def newlines(self) -> str | tuple[str, ...] | None:
        """Newline forms observed by the text decoder."""
        return getattr(self._decoder, "newlines", None)

    def tell(self) -> int:
        """Return an opaque text seek cookie for the current position."""
        self.binary._check_io()
        decoder_state = self._decoder.getstate()
        if (
            self.binary.tell() == 0
            and not self._text
            and not self._eof
            and decoder_state[0] in (b"", "")
        ):
            return 0
        return self._encode_cookie(
            self.binary.tell(),
            decoder_state,
            self._text,
            self._eof,
        )

    def _encode_cookie(
        self,
        position: int,
        decoder_state: tuple[bytes, int],
        text: str,
        eof: bool,
    ) -> int:
        buffered, flags = decoder_state
        if not isinstance(buffered, bytes) or not isinstance(flags, int):
            raise io.UnsupportedOperation("decoder does not support seekable state")
        payload = json.dumps(
            [
                position,
                base64.b64encode(buffered).decode("ascii"),
                flags,
                text,
                eof,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        signature = hmac.digest(self._cookie_secret, payload, hashlib.sha256)
        return int.from_bytes(_COOKIE_PREFIX + signature + payload, "big")

    def _decode_cookie(self, cookie: int) -> tuple[int, tuple[bytes, int], str, bool]:
        try:
            if cookie.bit_length() > self._max_cookie_bytes * 8:
                raise ValueError
            encoded = cookie.to_bytes(max(1, (cookie.bit_length() + 7) // 8), "big")
            payload_start = len(_COOKIE_PREFIX) + _COOKIE_SIGNATURE_SIZE
            if not encoded.startswith(_COOKIE_PREFIX) or len(encoded) <= payload_start:
                raise ValueError
            signature = encoded[len(_COOKIE_PREFIX) : payload_start]
            payload = encoded[payload_start:]
            expected = hmac.digest(self._cookie_secret, payload, hashlib.sha256)
            if not hmac.compare_digest(signature, expected):
                raise ValueError
            value = json.loads(payload)
            if (
                not isinstance(value, list)
                or len(value) != 5
                or isinstance(value[0], bool)
                or not isinstance(value[0], int)
                or value[0] < 0
                or not isinstance(value[1], str)
                or isinstance(value[2], bool)
                or not isinstance(value[2], int)
                or not isinstance(value[3], str)
                or not isinstance(value[4], bool)
            ):
                raise ValueError
            buffered = base64.b64decode(value[1], validate=True)
        except (AttributeError, OverflowError, ValueError, json.JSONDecodeError):
            raise io.UnsupportedOperation("can't restore arbitrary text position") from None
        return value[0], (buffered, value[2]), value[3], value[4]

    async def _fill(self) -> None:
        if self._eof:
            return
        data = await self.binary._read(self.binary._read_buffer_size)
        if data:
            self._text += self._decoder.decode(data, final=False)
        else:
            self._text += self._decoder.decode(b"", final=True)
            self._eof = True

    async def read(self, size: int | None = -1) -> str:
        """Read decoded text from the current cursor.

        Args:
            size: Maximum number of characters to read, or ``-1`` for all
                remaining text.

        Returns:
            Decoded text.
        """
        self.binary._check_operation()
        if size is None:
            size = -1
        if not isinstance(size, int):
            raise TypeError("size must be an integer")
        if size == 0:
            return ""
        if size < 0:
            while not self._eof:
                await self._fill()
            result, self._text = self._text, ""
            return result
        while len(self._text) < size and not self._eof:
            await self._fill()
        result, self._text = self._text[:size], self._text[size:]
        return result

    def _line_end(self) -> int | None:
        delimiter = self.newline
        if delimiter in (None, "\n"):
            index = self._text.find("\n")
            return index + 1 if index >= 0 else None
        if delimiter == "\r":
            index = self._text.find("\r")
            return index + 1 if index >= 0 else None
        if delimiter == "\r\n":
            index = self._text.find("\r\n")
            return index + 2 if index >= 0 else None
        for index, character in enumerate(self._text):
            if character == "\n":
                return index + 1
            if character == "\r":
                followed_by_lf = index + 1 < len(self._text) and self._text[index + 1] == "\n"
                return index + (2 if followed_by_lf else 1)
        return None

    async def readline(self, size: int | None = -1) -> str:
        """Read one decoded text line.

        Args:
            size: Maximum number of characters to read, or ``-1`` for an
                unbounded line.

        Returns:
            The next line according to the configured newline mode.
        """
        self.binary._check_operation()
        if size is None:
            size = -1
        if not isinstance(size, int):
            raise TypeError("size must be an integer")
        if size == 0:
            return ""
        while True:
            end = self._line_end()
            if end is not None or self._eof or (size >= 0 and len(self._text) >= size):
                take = len(self._text) if end is None else end
                if size >= 0:
                    take = min(take, size)
                result, self._text = self._text[:take], self._text[take:]
                return result
            await self._fill()

    async def seek(self, cookie: int, whence: int = io.SEEK_SET) -> int:
        """Restore a text cursor from a cookie returned by ``tell()``.

        Args:
            cookie: Text seek cookie, or zero for the beginning.
            whence: Standard ``io`` seek mode. Only limited text seeks are
                supported.

        Returns:
            The restored cookie.
        """
        self.binary._check_operation()
        if whence == io.SEEK_CUR:
            if cookie != 0:
                raise io.UnsupportedOperation("can't do nonzero cur-relative seeks")
            return self.tell()
        if whence == io.SEEK_END and cookie == 0:
            self.binary._seek(0, io.SEEK_END)
            self._decoder.reset()
            self._text = ""
            self._eof = True
            return self.tell()
        if whence == io.SEEK_END:
            raise io.UnsupportedOperation("can't do nonzero end-relative seeks")
        if whence != io.SEEK_SET:
            raise ValueError(f"invalid whence ({whence!r})")
        if cookie == 0:
            self.binary._seek(0)
            self._decoder.reset()
            self._text = ""
            self._eof = False
            return 0
        position, decoder_state, text, eof = self._decode_cookie(cookie)
        if position > self.binary.stat.size:
            raise io.UnsupportedOperation("can't restore arbitrary text position")
        try:
            candidate = self._new_decoder()
            candidate.setstate(decoder_state)
            if candidate.getstate() != decoder_state:
                raise ValueError
        except (OverflowError, TypeError, ValueError):
            raise io.UnsupportedOperation("can't restore invalid decoder state") from None
        self.binary._seek(position)
        self._decoder.setstate(decoder_state)
        self._text = text
        self._eof = eof
        return cookie

    async def close(self) -> None:
        """Close the underlying binary reader and clear seek-cookie secrets."""
        await self.binary.close()
        self._cookie_secret = b""

    def _clear_cookie_secret(self) -> None:
        self._cookie_secret = b""


__all__ = ["BlobReaderState", "BlobTextReaderState"]
