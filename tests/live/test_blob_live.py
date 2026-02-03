"""Live API tests for Vercel Blob storage.

These tests make real API calls and require BLOB_READ_WRITE_TOKEN environment variable.
Run with: pytest tests/live/test_blob_live.py -v
"""

import pytest

from .conftest import requires_blob_credentials


@requires_blob_credentials
@pytest.mark.live
class TestBlobLive:
    """Live tests for Blob API operations."""

    def test_put_and_delete_lifecycle(self, blob_token, unique_blob_path, cleanup_registry):
        """Test complete blob put -> head -> delete lifecycle."""
        from vercel.blob import delete, head, put
        from vercel.blob.errors import BlobNotFoundError

        # Put a blob
        result = put(
            unique_blob_path,
            b"Hello, World! This is a test blob.",
            token=blob_token,
        )
        cleanup_registry.register("blob", result.url)

        # Verify the result
        assert result.url is not None
        assert result.pathname == unique_blob_path
        assert result.content_type is not None

        # Head to get metadata
        meta = head(result.url, token=blob_token)
        assert meta.size > 0
        assert meta.pathname == unique_blob_path

        # Delete the blob
        delete(result.url, token=blob_token)

        # Verify deletion - head should raise BlobNotFoundError
        with pytest.raises(BlobNotFoundError):
            head(result.url, token=blob_token)

    @pytest.mark.asyncio
    async def test_put_and_delete_async(self, blob_token, unique_blob_path, cleanup_registry):
        """Test async blob put -> head -> delete lifecycle."""
        from vercel.blob import delete_async, head_async, put_async
        from vercel.blob.errors import BlobNotFoundError

        # Put a blob
        result = await put_async(
            unique_blob_path,
            b"Hello, World! This is an async test blob.",
            token=blob_token,
        )
        cleanup_registry.register("blob", result.url)

        # Verify the result
        assert result.url is not None
        assert result.pathname == unique_blob_path

        # Head to get metadata
        meta = await head_async(result.url, token=blob_token)
        assert meta.size > 0

        # Delete the blob
        await delete_async(result.url, token=blob_token)

        # Verify deletion
        with pytest.raises(BlobNotFoundError):
            await head_async(result.url, token=blob_token)

    def test_list_objects(self, blob_token, unique_blob_path, cleanup_registry):
        """Test listing blobs with prefix filter."""
        from vercel.blob import delete, list_objects, put

        # Create a blob with a unique prefix
        prefix = unique_blob_path.rsplit("/", 1)[0] + "/"
        blob_path = f"{prefix}list-test.txt"

        result = put(blob_path, b"list test content", token=blob_token)
        cleanup_registry.register("blob", result.url)

        # List objects with the prefix
        listing = list_objects(prefix=prefix, token=blob_token)

        # Should find at least our blob
        assert len(listing.blobs) >= 1
        found = any(b.pathname == blob_path for b in listing.blobs)
        assert found, f"Expected to find {blob_path} in listing"

        # Cleanup
        delete(result.url, token=blob_token)

    def test_blob_client_class(self, blob_token, unique_blob_path, cleanup_registry):
        """Test BlobClient class-based interface."""
        from vercel.blob import BlobClient
        from vercel.blob.errors import BlobNotFoundError

        client = BlobClient(token=blob_token)

        # Put using client
        result = client.put(unique_blob_path, b"Client test content")
        cleanup_registry.register("blob", result.url)

        # Head using client
        meta = client.head(result.url)
        assert meta.size > 0

        # List using client
        listing = client.list_objects(limit=5)
        assert listing.blobs is not None

        # Delete using client
        client.delete(result.url)

        # Verify deletion
        with pytest.raises(BlobNotFoundError):
            client.head(result.url)

    @pytest.mark.asyncio
    async def test_async_blob_client_class(self, blob_token, unique_blob_path, cleanup_registry):
        """Test AsyncBlobClient class-based interface."""
        from vercel.blob import AsyncBlobClient
        from vercel.blob.errors import BlobNotFoundError

        client = AsyncBlobClient(token=blob_token)

        # Put using client
        result = await client.put(unique_blob_path, b"Async client test content")
        cleanup_registry.register("blob", result.url)

        # Head using client
        meta = await client.head(result.url)
        assert meta.size > 0

        # Delete using client
        await client.delete(result.url)

        # Verify deletion
        with pytest.raises(BlobNotFoundError):
            await client.head(result.url)

    def test_copy_operation(self, blob_token, unique_blob_path, cleanup_registry):
        """Test server-side copy operation."""
        from vercel.blob import copy, delete, head, put

        # Create source blob
        source_path = unique_blob_path
        source_result = put(source_path, b"Source content for copy", token=blob_token)
        cleanup_registry.register("blob", source_result.url)

        # Copy to new destination
        dest_path = source_path.replace(".txt", "-copy.txt")
        copy_result = copy(source_result.url, dest_path, token=blob_token)
        cleanup_registry.register("blob", copy_result.url)

        # Verify copy exists
        copy_meta = head(copy_result.url, token=blob_token)
        assert copy_meta.size > 0
        assert copy_meta.pathname == dest_path

        # Cleanup
        delete(source_result.url, token=blob_token)
        delete(copy_result.url, token=blob_token)

    def test_create_folder(self, blob_token, unique_test_name, cleanup_registry):
        """Test folder creation."""
        from vercel.blob import create_folder, delete, list_objects

        folder_path = f"test-folders/{unique_test_name}"

        result = create_folder(folder_path, token=blob_token)
        cleanup_registry.register("blob", result.url)

        # Verify folder was created
        assert result.pathname.endswith("/")

        # List should show the folder
        listing = list_objects(prefix=f"test-folders/{unique_test_name}", token=blob_token)
        # Verify the folder appears in the listing (as a folder or blob depending on mode)
        folder_urls = [b.url for b in listing.blobs] + [f.url for f in listing.folders]
        assert result.url in folder_urls, f"Created folder {result.url} not found in listing"

        # Cleanup
        delete(result.url, token=blob_token)

    def test_iter_objects(self, blob_token, unique_blob_path, cleanup_registry):
        """Test blob iteration."""
        from vercel.blob import delete, iter_objects, put

        # Create multiple blobs
        prefix = unique_blob_path.rsplit("/", 1)[0] + "/"
        urls = []

        for i in range(3):
            blob_path = f"{prefix}iter-test-{i}.txt"
            result = put(blob_path, f"Content {i}".encode(), token=blob_token)
            cleanup_registry.register("blob", result.url)
            urls.append(result.url)

        # Iterate over objects
        items = list(iter_objects(prefix=prefix, token=blob_token))

        # Should find our blobs
        assert len(items) >= 3

        # Cleanup
        for url in urls:
            delete(url, token=blob_token)
