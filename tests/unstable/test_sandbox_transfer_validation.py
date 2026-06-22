from datetime import timedelta

import pytest

from vercel._internal.unstable.sandbox.errors import (
    SandboxFilesystemTransferError,
    SandboxUploadSizeMismatchError,
)
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
from vercel._internal.unstable.sandbox.runtime_common import (
    _UploadFileEntry,
    _validate_chunk_size,
    _validate_transfer_size,
)
from vercel.unstable import sandbox


class TestFilesystemHandles:
    def test_concrete_handles_exported(self) -> None:
        assert sandbox.SandboxBinaryReader is not None
        assert sandbox.SandboxBinaryWriter is not None
        assert sandbox.SandboxTextReader is not None
        assert sandbox.SandboxTextWriter is not None
        assert sandbox.sync.SyncSandboxBinaryReader is not None
        assert sandbox.sync.SyncSandboxBinaryWriter is not None
        assert sandbox.sync.SyncSandboxTextReader is not None
        assert sandbox.sync.SyncSandboxTextWriter is not None


class TestValidateTransferSize:
    def test_valid_nonzero_int(self) -> None:
        assert _validate_transfer_size(1024) == 1024

    def test_valid_zero(self) -> None:
        assert _validate_transfer_size(0) == 0

    def test_valid_large_int(self) -> None:
        assert _validate_transfer_size(10**12) == 10**12

    def test_rejects_negative(self) -> None:
        with pytest.raises(ValueError, match="size must be >= 0"):
            _validate_transfer_size(-1)

    def test_rejects_bool_true(self) -> None:
        with pytest.raises(TypeError, match="size must be an integer >= 0"):
            _validate_transfer_size(True)

    def test_rejects_bool_false(self) -> None:
        with pytest.raises(TypeError, match="size must be an integer >= 0"):
            _validate_transfer_size(False)

    def test_rejects_float(self) -> None:
        with pytest.raises(TypeError, match="size must be an integer >= 0"):
            _validate_transfer_size(1.0)

    def test_rejects_string(self) -> None:
        with pytest.raises(TypeError, match="size must be an integer >= 0"):
            _validate_transfer_size("1024")

    def test_rejects_none(self) -> None:
        with pytest.raises(TypeError, match="size must be an integer >= 0"):
            _validate_transfer_size(None)


class TestValidateChunkSize:
    def test_valid_positive_int(self) -> None:
        assert _validate_chunk_size(4096) == 4096

    def test_valid_large_int(self) -> None:
        assert _validate_chunk_size(2**20) == 1048576

    def test_rejects_zero(self) -> None:
        with pytest.raises(ValueError, match="chunk_size must be positive"):
            _validate_chunk_size(0)

    def test_rejects_negative(self) -> None:
        with pytest.raises(ValueError, match="chunk_size must be positive"):
            _validate_chunk_size(-1)

    def test_rejects_bool_true(self) -> None:
        with pytest.raises(TypeError, match="chunk_size must be a positive integer"):
            _validate_chunk_size(True)

    def test_rejects_bool_false(self) -> None:
        with pytest.raises(TypeError, match="chunk_size must be a positive integer"):
            _validate_chunk_size(False)

    def test_rejects_float(self) -> None:
        with pytest.raises(TypeError, match="chunk_size must be a positive integer"):
            _validate_chunk_size(1.0)

    def test_rejects_string(self) -> None:
        with pytest.raises(TypeError, match="chunk_size must be a positive integer"):
            _validate_chunk_size("4096")

    def test_rejects_none(self) -> None:
        with pytest.raises(TypeError, match="chunk_size must be a positive integer"):
            _validate_chunk_size(None)


class TestUploadFileEntry:
    def test_minimal_entry(self) -> None:
        entry = _UploadFileEntry(path="workspace/file.txt", size=10, source=b"0123456789")
        assert entry.path == "workspace/file.txt"
        assert entry.size == 10
        assert entry.source == b"0123456789"
        assert entry.mode is None

    def test_entry_with_mode(self) -> None:
        entry = _UploadFileEntry(path="workspace/script.sh", size=5, source=b"#!/sh", mode=0o755)
        assert entry.mode == 0o755

    def test_entry_is_frozen(self) -> None:
        entry = _UploadFileEntry(path="workspace/file.txt", size=10, source=b"0123456789")
        assert entry.path == "workspace/file.txt"

    def test_zero_sized_entry(self) -> None:
        entry = _UploadFileEntry(path="workspace/empty.txt", size=0, source=b"")
        assert entry.size == 0
        assert entry.source == b""


class TestFileTransferTimeoutOption:
    def test_default_is_five_minutes(self) -> None:
        options = SandboxServiceOptions()
        assert options.file_transfer_timeout == timedelta(minutes=5)

    def test_custom_timeout(self) -> None:
        options = SandboxServiceOptions(file_transfer_timeout=timedelta(seconds=30))
        assert options.file_transfer_timeout == timedelta(seconds=30)

    def test_none_timeout_uses_default(self) -> None:
        options = SandboxServiceOptions(file_transfer_timeout=None)
        assert options.file_transfer_timeout == timedelta(minutes=5)

    def test_zero_timeout_not_replaced_by_default(self) -> None:
        options = SandboxServiceOptions(file_transfer_timeout=timedelta(0))
        assert options.file_transfer_timeout == timedelta(0)


class TestTransferErrors:
    def test_transfer_error_inheritance(self) -> None:
        assert issubclass(
            SandboxFilesystemTransferError,
            sandbox.SandboxFilesystemError,
        )

    def test_upload_size_mismatch_inheritance(self) -> None:
        assert issubclass(
            SandboxUploadSizeMismatchError,
            SandboxFilesystemTransferError,
        )

    def test_upload_size_mismatch_early_end(self) -> None:
        err = SandboxUploadSizeMismatchError(
            "workspace/file.txt", declared=100, consumed=50, early_end=True
        )
        assert err.path == "workspace/file.txt"
        assert err.declared == 100
        assert err.consumed == 50
        assert err.early_end is True
        assert "source ended early" in str(err)

    def test_upload_size_mismatch_trailing_data(self) -> None:
        err = SandboxUploadSizeMismatchError(
            "workspace/file.txt", declared=50, consumed=100, early_end=False
        )
        assert err.path == "workspace/file.txt"
        assert err.declared == 50
        assert err.consumed == 100
        assert err.early_end is False
        assert "source produced trailing data" in str(err)

    def test_transfer_errors_exported_in_public_api(self) -> None:
        assert sandbox.SandboxFilesystemTransferError is SandboxFilesystemTransferError
        assert sandbox.SandboxUploadSizeMismatchError is SandboxUploadSizeMismatchError

    def test_transfer_errors_exported_in_sync(self) -> None:
        assert sandbox.sync.SandboxFilesystemTransferError is SandboxFilesystemTransferError
        assert sandbox.sync.SandboxUploadSizeMismatchError is SandboxUploadSizeMismatchError
