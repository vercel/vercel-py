"""
E2E tests for Vercel Blob Storage functionality.

These tests verify the complete blob storage workflow including:
- Uploading files (put)
- Retrieving file metadata (head)
- Listing blobs
- Copying blobs
- Deleting blobs
- Creating folders
- Multipart uploads
"""

import asyncio
import os
import pytest

from vercel.blob import (
    put_async,
    head_async,
    list_objects_async,
    copy_async,
    delete_async,
    create_folder_async,
)
from vercel.blob import UploadProgressEvent


class TestBlobStorageE2E:
    """End-to-end tests for blob storage functionality."""

    @pytest.fixture
    def blob_token(self):
        """Get blob storage token from environment."""
        token = os.getenv("BLOB_READ_WRITE_TOKEN")
        if not token:
            pytest.skip("BLOB_READ_WRITE_TOKEN not set - skipping blob e2e tests")
        return token

    @pytest.fixture
    def test_prefix(self):
        """Generate a unique test prefix for this test run."""
        import time

        return f"e2e-test-{int(time.time())}"

    @pytest.fixture
    def test_data(self):
        """Sample test data for uploads."""
        return {
            "text": b"Hello, World! This is a test file for e2e testing.",
            "json": b'{"message": "test", "number": 42, "array": [1, 2, 3]}',
            "large": b"Large file content " * 1000,  # ~18KB
            "binary": b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f",
        }

    @pytest.fixture
    def uploaded_blobs(self):
        """Track uploaded blobs for cleanup."""
        return []

    @pytest.mark.asyncio
    async def test_blob_put_and_head(self, blob_token, test_prefix, test_data, uploaded_blobs):
        """Test basic blob upload and metadata retrieval."""
        pathname = f"{test_prefix}/test-file.txt"

        # Upload a text file
        result = await put_async(
            pathname,
            test_data["text"],
            access="public",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
        )

        uploaded_blobs.append(result.url)

        # Verify upload result
        assert result.pathname is not None
        assert result.url is not None
        assert result.downloadUrl is not None

        # Get file metadata
        metadata = await head_async(result.url, token=blob_token)

        # Verify metadata
        assert metadata.contentType == "text/plain"
        assert metadata.size == len(test_data["text"])
        assert metadata.pathname == result.pathname

    @pytest.mark.asyncio
    async def test_blob_list_operation(self, blob_token, test_prefix, test_data, uploaded_blobs):
        """Test blob listing functionality."""
        # Upload multiple files
        files = [
            ("file1.txt", test_data["text"], "text/plain"),
            ("file2.json", test_data["json"], "application/json"),
            ("subdir/file3.txt", test_data["text"], "text/plain"),
        ]

        uploaded_paths = []
        for filename, content, content_type in files:
            pathname = f"{test_prefix}/{filename}"
            result = await put_async(
                pathname,
                content,
                access="public",
                content_type=content_type,
                token=blob_token,
                add_random_suffix=True,
            )
            uploaded_blobs.append(result.url)
            uploaded_paths.append(result.pathname)

        # List blobs with prefix
        listing = await list_objects_async(prefix=f"{test_prefix}/", limit=10, token=blob_token)

        # Verify listing
        assert listing.blobs is not None
        assert len(listing.blobs) >= 3  # At least our 3 files

        # Check that our files are in the listing
        listed_paths = [blob_item.pathname for blob_item in listing.blobs]
        for path in uploaded_paths:
            assert path in listed_paths

    @pytest.mark.asyncio
    async def test_blob_copy_operation(self, blob_token, test_prefix, test_data, uploaded_blobs):
        """Test blob copying functionality."""
        # Upload original file
        original_path = f"{test_prefix}/original.txt"
        original_result = await put_async(
            original_path,
            test_data["text"],
            access="public",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
        )
        uploaded_blobs.append(original_result.url)

        # Copy the file
        copy_path = f"{test_prefix}/copy.txt"
        copy_result = await copy_async(
            original_result.pathname,
            copy_path,
            access="public",
            token=blob_token,
            allow_overwrite=True,
        )
        uploaded_blobs.append(copy_result.url)

        # Verify copy
        assert copy_result.pathname == copy_path
        assert copy_result.url is not None

        # Verify both files have same content
        original_metadata = await head_async(original_result.url, token=blob_token)
        copy_metadata = await head_async(copy_result.url, token=blob_token)

        assert original_metadata.size == copy_metadata.size
        assert original_metadata.contentType == copy_metadata.contentType

    @pytest.mark.asyncio
    async def test_blob_delete_operation(self, blob_token, test_prefix, test_data, uploaded_blobs):
        """Test blob deletion functionality."""
        # Upload a file
        pathname = f"{test_prefix}/to-delete.txt"
        result = await put_async(
            pathname,
            test_data["text"],
            access="public",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
        )

        # Verify file exists
        metadata = await head_async(result.url, token=blob_token)
        assert metadata is not None

        # Delete the file
        await delete_async([result.url], token=blob_token)

        # Verify file is deleted
        try:
            await head_async(result.url, token=blob_token)
            assert False, "File should have been deleted"
        except Exception as e:
            # Expected - file should not exist
            assert "not found" in str(e).lower() or "404" in str(e)

    @pytest.mark.asyncio
    async def test_blob_create_folder(self, blob_token, test_prefix, uploaded_blobs):
        """Test folder creation functionality."""
        folder_path = f"{test_prefix}/test-folder"

        # Create folder
        folder_result = await create_folder_async(folder_path, token=blob_token, overwrite=True)

        uploaded_blobs.append(folder_result.url)

        # Verify folder creation
        assert folder_result.pathname == folder_path
        assert folder_result.url is not None

        # Upload a file to the folder
        file_path = f"{folder_path}/file-in-folder.txt"
        file_result = await put_async(
            file_path,
            b"File in folder",
            access="public",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
        )
        uploaded_blobs.append(file_result.url)

        # Verify file was uploaded to folder
        assert file_result.pathname.startswith(folder_path)

    @pytest.mark.asyncio
    async def test_blob_multipart_upload(self, blob_token, test_prefix, test_data, uploaded_blobs):
        """Test multipart upload functionality."""
        pathname = f"{test_prefix}/multipart-file.txt"

        # Create a larger file for multipart upload
        large_content = test_data["large"] * 10  # ~180KB

        # Upload using multipart
        result = await put_async(
            pathname,
            large_content,
            access="public",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
            multipart=True,
        )

        uploaded_blobs.append(result.url)

        # Verify upload
        assert result.pathname is not None
        assert result.url is not None

        # Verify file metadata
        metadata = await head_async(result.url, token=blob_token)
        assert metadata.size == len(large_content)
        assert metadata.contentType == "text/plain"

    @pytest.mark.asyncio
    async def test_blob_upload_progress_callback(
        self, blob_token, test_prefix, test_data, uploaded_blobs
    ):
        """Test upload progress callback functionality."""
        pathname = f"{test_prefix}/progress-file.txt"

        progress_events = []

        def on_progress(event: UploadProgressEvent):
            progress_events.append(event)

        # Upload with progress callback
        result = await put_async(
            pathname,
            test_data["large"],
            access="public",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
            on_upload_progress=on_progress,
        )

        uploaded_blobs.append(result.url)

        # Verify progress events were received
        assert len(progress_events) > 0

        # Verify progress events are valid
        for event in progress_events:
            assert event.loaded >= 0
            assert event.total > 0
            assert event.percentage >= 0
            assert event.percentage <= 100

    @pytest.mark.asyncio
    async def test_blob_different_access_levels(
        self, blob_token, test_prefix, test_data, uploaded_blobs
    ):
        """Test different access levels for blob uploads."""
        # Test public access
        public_path = f"{test_prefix}/public-file.txt"
        public_result = await put_async(
            public_path,
            test_data["text"],
            access="public",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
        )
        uploaded_blobs.append(public_result.url)

        # Test private access
        private_path = f"{test_prefix}/private-file.txt"
        private_result = await put_async(
            private_path,
            test_data["text"],
            access="private",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
        )
        uploaded_blobs.append(private_result.url)

        # Verify both uploads succeeded
        assert public_result.url is not None
        assert private_result.url is not None

        # Verify metadata can be retrieved for both
        public_metadata = await head_async(public_result.url, token=blob_token)
        private_metadata = await head_async(private_result.url, token=blob_token)

        assert public_metadata is not None
        assert private_metadata is not None

    @pytest.mark.asyncio
    async def test_blob_content_type_detection(self, blob_token, test_prefix, uploaded_blobs):
        """Test automatic content type detection."""
        # Test different file types
        test_files = [
            ("test.txt", b"Plain text content", "text/plain"),
            ("test.json", b'{"key": "value"}', "application/json"),
            ("test.html", b"<html><body>Hello</body></html>", "text/html"),
        ]

        for filename, content, expected_type in test_files:
            pathname = f"{test_prefix}/{filename}"
            result = await put_async(
                pathname, content, access="public", token=blob_token, add_random_suffix=True
            )
            uploaded_blobs.append(result.url)

            # Verify content type
            metadata = await head_async(result.url, token=blob_token)
            assert metadata.contentType == expected_type

    @pytest.mark.asyncio
    async def test_blob_error_handling(self, blob_token, test_prefix):
        """Test blob error handling for invalid operations."""
        # Test uploading invalid data
        with pytest.raises(Exception):
            await put_async(
                f"{test_prefix}/invalid.txt",
                {"invalid": "dict"},  # Should fail - not bytes/string
                access="public",
                token=blob_token,
            )

        # Test accessing non-existent blob
        with pytest.raises(Exception):
            await head_async("https://example.com/non-existent-blob", token=blob_token)

    @pytest.mark.asyncio
    async def test_blob_concurrent_operations(
        self, blob_token, test_prefix, test_data, uploaded_blobs
    ):
        """Test concurrent blob operations."""

        async def upload_file(i: int):
            pathname = f"{test_prefix}/concurrent-{i}.txt"
            content = f"Concurrent file {i}: {test_data['text'].decode()}"
            result = await put_async(
                pathname,
                content.encode(),
                access="public",
                content_type="text/plain",
                token=blob_token,
                add_random_suffix=True,
            )
            return result

        # Upload multiple files concurrently
        results = await asyncio.gather(*[upload_file(i) for i in range(5)])

        # Verify all uploads succeeded
        for result in results:
            assert result.url is not None
            uploaded_blobs.append(result.url)

        # Verify all files can be accessed
        metadata_results = await asyncio.gather(
            *[head_async(result.url, token=blob_token) for result in results]
        )

        for metadata in metadata_results:
            assert metadata is not None
            assert metadata.contentType == "text/plain"
