import pytest
from hypothesis import given, settings, strategies as st

from vercel.sandbox import SandboxUploadSizeMismatchError
from vercel.sandbox._internal.filesystem_write import _ExactSizeWriteTarget


class _RecordingUpload:
    def __init__(self) -> None:
        self.writes: list[bytes] = []
        self.finishes = 0
        self.write_error: BaseException | None = None

    async def write(self, data: bytes) -> None:
        if self.write_error is not None:
            raise self.write_error
        self.writes.append(data)

    async def flush(self) -> None:
        pass

    async def finish(self) -> None:
        self.finishes += 1

    async def abort(self) -> None:
        pass


@pytest.mark.asyncio
@settings(deadline=None)
@given(
    declared=st.integers(min_value=0, max_value=64),
    writes=st.lists(
        st.tuples(st.binary(max_size=16), st.booleans()),
        max_size=8,
    ),
)
async def test_exact_size_target_accounting(
    declared: int,
    writes: list[tuple[bytes, bool]],
) -> None:
    upload = _RecordingUpload()
    target = _ExactSizeWriteTarget(upload, name="data.bin", size=declared)
    forwarded: list[bytes] = []
    consumed = 0

    for chunk, inject_failure in writes:
        attempted = consumed + len(chunk)
        if attempted > declared:
            with pytest.raises(SandboxUploadSizeMismatchError) as overflow_info:
                await target.write(chunk)
            error = overflow_info.value
            assert (error.path, error.declared, error.consumed, error.early_end) == (
                "data.bin",
                declared,
                attempted,
                False,
            )
            assert upload.writes == forwarded
            break

        if inject_failure:
            write_error = RuntimeError("write failed")
            upload.write_error = write_error
            with pytest.raises(RuntimeError) as write_info:
                await target.write(chunk)
            assert write_info.value is write_error
            upload.write_error = None
            assert upload.writes == forwarded

        await target.write(chunk)
        forwarded.append(chunk)
        consumed = attempted

    assert upload.writes == forwarded
    if consumed == declared:
        await target.finish()
        assert upload.finishes == 1
    else:
        with pytest.raises(SandboxUploadSizeMismatchError) as underflow_info:
            await target.finish()
        error = underflow_info.value
        assert (error.path, error.declared, error.consumed, error.early_end) == (
            "data.bin",
            declared,
            consumed,
            True,
        )
        assert upload.finishes == 0
