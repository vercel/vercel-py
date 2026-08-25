"""Transport-agnostic Blob stream state."""

from __future__ import annotations

import codecs
import io
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import Any

from vercel._internal.core.byte_stream import StagingByteFile
from vercel.blob.errors import BlobStreamError

from .models import Access, BlobStatResult
from .service import BlobService


class BinaryReaderCore:
    def __init__(self, *, service: BlobService, stat: BlobStatResult, data: bytes) -> None:
        self.service = service
        self.stat = stat
        self.data = data
        self.position = 0
        self.closed = False

    def check(self) -> None:
        self.service.ensure_open()
        if self.closed:
            raise ValueError("I/O operation on closed Blob stream")

    async def read(self, size: int | None = -1) -> bytes:
        self.check()
        if size is None or size < 0:
            result = self.data[self.position :]
            self.position = len(self.data)
            return result
        result = self.data[self.position : self.position + size]
        self.position += len(result)
        return result

    async def readinto(self, buffer: Any) -> int:
        view = memoryview(buffer)
        if view.readonly:
            raise TypeError("readinto() argument must be writable")
        data = await self.read(view.nbytes)
        view.cast("B")[: len(data)] = data
        return len(data)

    async def readline(self, size: int | None = -1) -> bytes:
        self.check()
        limit = (
            len(self.data)
            if size is None or size < 0
            else min(len(self.data), self.position + size)
        )
        newline = self.data.find(b"\n", self.position, limit)
        end = limit if newline < 0 else newline + 1
        result = self.data[self.position : end]
        self.position = end
        return result

    async def close(self) -> None:
        self.closed = True


class BinaryWriterCore:
    def __init__(
        self,
        *,
        service: BlobService,
        pathname: str,
        access: Access,
        content_type: str | None,
        cache_control_max_age: timedelta | None,
        staging: StagingByteFile,
        staging_owner: AbstractAsyncContextManager[StagingByteFile],
    ) -> None:
        self.service = service
        self.pathname = pathname
        self.access = access
        self.content_type = content_type
        self.cache_control_max_age = cache_control_max_age
        self.staging = staging
        self.staging_owner = staging_owner
        self.position = 0
        self.closed = False
        self.aborted = False
        self.broken: BaseException | None = None
        self._cleanup_attempted = False
        self._cleanup_complete = False

    @classmethod
    async def create(
        cls,
        *,
        service: BlobService,
        pathname: str,
        access: Access,
        content_type: str | None,
        cache_control_max_age: timedelta | None,
    ) -> BinaryWriterCore:
        staging_owner = service.staging_file_runtime.temporary_file()
        staging = await staging_owner.__aenter__()
        return cls(
            service=service,
            pathname=pathname,
            access=access,
            content_type=content_type,
            cache_control_max_age=cache_control_max_age,
            staging=staging,
            staging_owner=staging_owner,
        )

    def check(self) -> None:
        if self.broken is not None:
            raise self.broken
        if self.closed:
            raise ValueError("I/O operation on closed Blob stream")
        self.service.ensure_open()

    def _note_cleanup_failure(self, error: BaseException) -> None:
        primary = self.broken
        if primary is None:
            self.broken = error
            return
        add_note = getattr(primary, "add_note", None)
        if callable(add_note):
            add_note(f"staging cleanup also failed: {error!r}")

    async def cleanup_after_failure(self, *, retry: bool = False) -> None:
        if self._cleanup_complete or (self._cleanup_attempted and not retry):
            return
        self._cleanup_attempted = True
        primary = self.broken
        try:
            await self.staging_owner.__aexit__(
                type(primary) if primary is not None else None,
                primary,
                primary.__traceback__ if primary is not None else None,
            )
        except BaseException as error:
            self._note_cleanup_failure(error)
        else:
            self._cleanup_complete = True

    async def _break(self, error: BaseException) -> None:
        if self.broken is None:
            self.broken = error
        self.closed = True
        await self.cleanup_after_failure()

    async def write(self, data: Any) -> int:
        self.check()
        try:
            view = memoryview(data).cast("B")
        except TypeError as exc:
            raise TypeError("a bytes-like object is required") from exc
        payload = bytes(view)
        offset = 0
        try:
            while offset < len(payload):
                written = await self.staging.write(payload[offset:])
                if written <= 0:
                    raise BlobStreamError("Blob staging file write made no progress")
                offset += written
        except BaseException as error:
            await self._break(error)
            raise
        self.position += offset
        return offset

    async def flush(self) -> None:
        self.check()
        try:
            await self.staging.flush()
        except BaseException as error:
            await self._break(error)
            raise

    async def close(self, *, publish: bool = True) -> None:
        if self.closed:
            if self.broken is not None:
                raise self.broken
            return
        try:
            if publish and not self.aborted:
                await self.staging.flush()
                await self.staging.seek(0)
                body = await self.staging.read()
                await self.service.publish(
                    self.pathname,
                    body,
                    access=self.access,
                    content_type=self.content_type,
                    cache_control_max_age=self.cache_control_max_age,
                )
        except BaseException as error:
            await self._break(error)
            raise
        self.closed = True
        self._cleanup_attempted = True
        try:
            await self.staging_owner.__aexit__(None, None, None)
        except BaseException as error:
            self.broken = error
            raise
        else:
            self._cleanup_complete = True

    async def abort(self) -> None:
        if self.closed:
            return
        self.aborted = True
        self.closed = True
        error = RuntimeError("Blob writer aborted")
        try:
            await self.staging_owner.__aexit__(type(error), error, error.__traceback__)
        except BaseException as cleanup_error:
            self.broken = cleanup_error
            raise
        else:
            self._cleanup_complete = True


class TextReaderCore:
    def __init__(
        self,
        binary: BinaryReaderCore,
        *,
        encoding: str,
        errors: str,
        newline: str | None,
    ) -> None:
        self.binary = binary
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        decoder = codecs.getincrementaldecoder(encoding)(errors=errors)
        raw_text = decoder.decode(binary.data, final=True)
        newline_decoder = io.IncrementalNewlineDecoder(
            codecs.getincrementaldecoder(encoding)(errors=errors),
            translate=False,
        )
        newline_decoder.decode(binary.data, final=True)
        self.newlines = newline_decoder.newlines if newline in (None, "") else None
        self.text, self._byte_offsets = self._normalize(raw_text)
        self.position = 0

    def _normalize(self, raw_text: str) -> tuple[str, list[int]]:
        if self.newline is not None:
            return raw_text, [
                len(raw_text[:index].encode(self.encoding, self.errors))
                for index in range(len(raw_text) + 1)
            ]
        text: list[str] = []
        offsets = [0]
        index = 0
        while index < len(raw_text):
            character = raw_text[index]
            consumed = index + 1
            if character == "\r":
                if consumed < len(raw_text) and raw_text[consumed] == "\n":
                    consumed += 1
                character = "\n"
            text.append(character)
            offsets.append(len(raw_text[:consumed].encode(self.encoding, self.errors)))
            index = consumed
        return "".join(text), offsets

    def check(self) -> None:
        self.binary.check()

    def tell(self) -> int:
        self.check()
        return self._byte_offsets[self.position]

    def _advance(self, end: int) -> str:
        result = self.text[self.position : end]
        self.position = end
        self.binary.position = self.tell()
        return result

    def read(self, size: int | None = -1) -> str:
        self.check()
        if size is None or size < 0:
            return self._advance(len(self.text))
        return self._advance(min(len(self.text), self.position + size))

    def _line_end(self) -> int:
        limit = len(self.text)
        if self.newline in (None, "\n"):
            index = self.text.find("\n", self.position)
            return limit if index < 0 else index + 1
        if self.newline in ("\r", "\r\n"):
            index = self.text.find(self.newline, self.position)
            return limit if index < 0 else index + len(self.newline)
        for index in range(self.position, limit):
            character = self.text[index]
            if character == "\n":
                return index + 1
            if character == "\r":
                return index + 2 if self.text[index : index + 2] == "\r\n" else index + 1
        return limit

    def readline(self, size: int | None = -1) -> str:
        self.check()
        end = self._line_end()
        if size is not None and size >= 0:
            end = min(end, self.position + size)
        return self._advance(end)
