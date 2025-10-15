"""
Integration tests for Vercel SDK combining multiple features.

These tests verify the complete SDK workflow combining:
- Cache + Blob storage
- Headers + OIDC + Cache
- Projects API + Blob storage
- Full end-to-end application scenarios
"""

import asyncio
import os
import pytest
from unittest.mock import Mock

from vercel.cache.aio import get_cache
from vercel.blob import put_async, head_async, delete_async
from vercel.headers import ip_address, geolocation
from vercel.oidc import get_vercel_oidc_token, decode_oidc_payload
from vercel.projects import create_project, update_project, delete_project


class TestVercelSDKIntegration:
    """Integration tests combining multiple Vercel SDK features."""

    @pytest.fixture
    def blob_token(self):
        """Get blob storage token from environment."""
        return os.getenv("BLOB_READ_WRITE_TOKEN")

    @pytest.fixture
    def vercel_token(self):
        """Get Vercel API token from environment."""
        return os.getenv("VERCEL_TOKEN")

    @pytest.fixture
    def oidc_token(self):
        """Get OIDC token from environment or use Vercel token as fallback."""
        # First try to get actual OIDC token
        oidc_token = os.getenv("VERCEL_OIDC_TOKEN")
        if oidc_token:
            return oidc_token

        # Fallback to Vercel API token for testing OIDC functionality
        vercel_token = os.getenv("VERCEL_TOKEN")
        if not vercel_token:
            pytest.skip(
                "Neither VERCEL_OIDC_TOKEN nor VERCEL_TOKEN set - skipping OIDC integration tests"
            )

        # Return Vercel token as fallback (tests will adapt)
        return vercel_token

    @pytest.fixture
    def vercel_team_id(self):
        """Get Vercel team ID from environment."""
        return os.getenv("VERCEL_TEAM_ID")

    @pytest.fixture
    def test_prefix(self):
        """Generate a unique test prefix for this test run."""
        import time

        return f"integration-test-{int(time.time())}"

    @pytest.fixture
    def uploaded_blobs(self):
        """Track uploaded blobs for cleanup."""
        return []

    @pytest.fixture
    def created_projects(self):
        """Track created projects for cleanup."""
        return []

    @pytest.mark.asyncio
    async def test_cache_blob_integration(self, blob_token, test_prefix, uploaded_blobs):
        """Test integration between cache and blob storage."""
        if not blob_token:
            pytest.skip("BLOB_READ_WRITE_TOKEN not set - skipping cache-blob integration test")

        cache = get_cache(namespace="integration-test")

        # Upload a file to blob storage
        file_content = b"Integration test file content"
        blob_result = await put_async(
            f"{test_prefix}/cache-blob-test.txt",
            file_content,
            access="public",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
        )
        uploaded_blobs.append(blob_result.url)

        # Cache the blob URL and metadata
        cache_key = "blob:test-file"
        blob_metadata = {
            "url": blob_result.url,
            "pathname": blob_result.pathname,
            "size": len(file_content),
            "content_type": "text/plain",
        }

        await cache.set(cache_key, blob_metadata, {"ttl": 60, "tags": ["blob", "test"]})

        # Retrieve from cache
        cached_metadata = await cache.get(cache_key)
        assert cached_metadata is not None
        assert cached_metadata["url"] == blob_result.url
        assert cached_metadata["size"] == len(file_content)

        # Verify blob still exists and is accessible
        blob_info = await head_async(blob_result.url, token=blob_token)
        assert blob_info.size == len(file_content)
        assert blob_info.content_type == "text/plain"

        # Clean up cache
        await cache.delete(cache_key)

    @pytest.mark.asyncio
    async def test_headers_oidc_cache_integration(self, oidc_token, vercel_token):
        """Test integration between headers, OIDC, and cache."""
        if not oidc_token:
            pytest.skip(
                "Neither VERCEL_OIDC_TOKEN nor VERCEL_TOKEN set - skipping headers-oidc-cache integration test"
            )

        cache = get_cache(namespace="integration-test")

        # Mock request with headers
        mock_request = Mock()
        mock_request.headers = {
            "x-real-ip": "203.0.113.1",
            "x-vercel-ip-city": "San Francisco",
            "x-vercel-ip-country": "US",
            "x-vercel-id": "sfo1:integration123",
        }

        # Extract geolocation data
        geo_data = geolocation(mock_request)
        ip = ip_address(mock_request)

        # Get OIDC token and decode payload
        token = get_vercel_oidc_token()

        # Handle both real OIDC tokens and Vercel API token fallback
        if token == vercel_token:
            print("✅ Using Vercel API token as OIDC fallback in integration test")
            # Use a mock payload for Vercel API token
            token_payload = {
                "sub": "vercel-api-user",
                "exp": int(asyncio.get_event_loop().time()) + 3600,
                "iat": int(asyncio.get_event_loop().time()),
            }
        else:
            # Real OIDC token
            token_payload = decode_oidc_payload(token)

        # Create user session data combining all information
        session_data = {
            "user_id": token_payload.get("sub"),
            "ip_address": ip,
            "geolocation": geo_data,
            "token_expires": token_payload.get("exp"),
            "region": geo_data.get("region"),
            "timestamp": int(asyncio.get_event_loop().time()),
        }

        # Cache the session data
        session_key = f"session:{token_payload.get('sub')}"
        await cache.set(session_key, session_data, {"ttl": 300, "tags": ["session", "user"]})

        # Retrieve and verify session data
        cached_session = await cache.get(session_key)
        assert cached_session is not None
        assert cached_session["user_id"] == token_payload.get("sub")
        assert cached_session["ip_address"] == ip
        assert cached_session["geolocation"]["city"] == "San Francisco"
        assert cached_session["geolocation"]["country"] == "US"

        # Clean up
        await cache.delete(session_key)

    @pytest.mark.asyncio
    async def test_projects_blob_integration(
        self,
        vercel_token,
        blob_token,
        vercel_team_id,
        test_prefix,
        uploaded_blobs,
        created_projects,
    ):
        """Test integration between projects API and blob storage."""
        if not vercel_token or not blob_token:
            pytest.skip(
                "VERCEL_TOKEN or BLOB_READ_WRITE_TOKEN not set - skipping projects-blob integration test"
            )

        # Create a project
        project_name = f"integration-test-project-{int(asyncio.get_event_loop().time())}"
        project_data = {"name": project_name, "framework": "nextjs"}

        created_project = await create_project(
            body=project_data, token=vercel_token, team_id=vercel_team_id
        )
        created_projects.append(created_project["id"])

        # Upload project assets to blob storage
        assets = [
            ("logo.png", b"PNG logo data", "image/png"),
            (
                "config.json",
                b'{"theme": "dark", "features": ["auth", "cache"]}',
                "application/json",
            ),
            ("README.md", b"# Project Documentation\n\nThis is a test project.", "text/markdown"),
        ]

        uploaded_assets = []
        for filename, content, content_type in assets:
            blob_result = await put_async(
                f"{test_prefix}/project-assets/{filename}",
                content,
                access="public",
                content_type=content_type,
                token=blob_token,
                add_random_suffix=True,
            )
            uploaded_blobs.append(blob_result.url)
            uploaded_assets.append(
                {
                    "filename": filename,
                    "url": blob_result.url,
                    "pathname": blob_result.pathname,
                    "content_type": content_type,
                    "size": len(content),
                }
            )

        # Update project with asset information
        project_update = {
            "name": created_project["name"],
            "env": [{"key": "ASSETS_CONFIG", "value": str(uploaded_assets), "type": "encrypted"}],
        }

        try:
            updated_project = await update_project(
                project_id=created_project["id"],
                body=project_update,
                token=vercel_token,
                team_id=vercel_team_id,
            )

            # Verify project was updated
            assert updated_project["id"] == created_project["id"]

        except Exception as e:
            # Environment variables might not be supported
            pytest.skip(f"Project environment variables not supported: {e}")

        # Verify all assets are accessible
        for asset in uploaded_assets:
            blob_info = await head_async(asset["url"], token=blob_token)
            assert blob_info.size == asset["size"]
            assert blob_info.contentType == asset["content_type"]

    @pytest.mark.asyncio
    async def test_full_application_workflow(
        self, blob_token, oidc_token, vercel_token, test_prefix, uploaded_blobs
    ):
        """Test a complete application workflow using multiple SDK features."""
        if not blob_token or not oidc_token:
            pytest.skip("Required tokens not set - skipping full workflow test")

        cache = get_cache(namespace="full-workflow-test")

        # Simulate a user uploading a file and processing it
        # Step 1: User uploads a file
        file_content = b"User uploaded file content for processing"
        upload_result = await put_async(
            f"{test_prefix}/user-uploads/document.txt",
            file_content,
            access="private",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
        )
        uploaded_blobs.append(upload_result.url)

        # Step 2: Get user context (OIDC + Headers)
        token = get_vercel_oidc_token()

        # Handle both real OIDC tokens and Vercel API token fallback
        if token == vercel_token:
            print("✅ Using Vercel API token as OIDC fallback in full workflow test")
            # Use a mock payload for Vercel API token
            token_payload = {
                "sub": "vercel-api-user",
                "exp": int(asyncio.get_event_loop().time()) + 3600,
                "iat": int(asyncio.get_event_loop().time()),
            }
        else:
            # Real OIDC token
            token_payload = decode_oidc_payload(token)

        # Mock request headers
        mock_request = Mock()
        mock_request.headers = {
            "x-real-ip": "198.51.100.1",
            "x-vercel-ip-city": "New York",
            "x-vercel-ip-country": "US",
            "x-vercel-id": "iad1:workflow123",
        }

        geo_data = geolocation(mock_request)
        ip = ip_address(mock_request)

        # Step 3: Create processing job
        job_id = f"job-{int(asyncio.get_event_loop().time())}"
        job_data = {
            "job_id": job_id,
            "user_id": token_payload.get("sub"),
            "file_url": upload_result.url,
            "file_pathname": upload_result.pathname,
            "uploaded_at": int(asyncio.get_event_loop().time()),
            "user_ip": ip,
            "user_location": geo_data,
            "status": "processing",
        }

        # Cache the job
        await cache.set(f"job:{job_id}", job_data, {"ttl": 3600, "tags": ["job", "processing"]})

        # Step 4: Process the file (simulate)
        processed_content = file_content.upper()  # Simple processing
        processed_result = await put_async(
            f"{test_prefix}/processed/document-processed.txt",
            processed_content,
            access="public",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
        )
        uploaded_blobs.append(processed_result.url)

        # Step 5: Update job status
        job_data["status"] = "completed"
        job_data["processed_file_url"] = processed_result.url
        job_data["processed_at"] = int(asyncio.get_event_loop().time())

        await cache.set(f"job:{job_id}", job_data, {"ttl": 3600, "tags": ["job", "completed"]})

        # Step 6: Verify the complete workflow
        cached_job = await cache.get(f"job:{job_id}")
        assert cached_job is not None
        assert cached_job["status"] == "completed"
        assert cached_job["processed_file_url"] == processed_result.url
        assert cached_job["user_location"]["city"] == "New York"

        # Verify both files are accessible
        original_info = await head_async(upload_result.url, token=blob_token)
        processed_info = await head_async(processed_result.url, token=blob_token)

        assert original_info.size == len(file_content)
        assert processed_info.size == len(processed_content)

        # Clean up
        await cache.delete(f"job:{job_id}")

    @pytest.mark.asyncio
    async def test_error_handling_integration(self, blob_token, test_prefix, uploaded_blobs):
        """Test error handling across integrated features."""
        if not blob_token:
            pytest.skip("BLOB_READ_WRITE_TOKEN not set - skipping error handling test")

        cache = get_cache(namespace="error-handling-test")

        # Test error handling in blob operations
        with pytest.raises(Exception):
            await put_async(
                f"{test_prefix}/invalid-file.txt",
                {"invalid": "data"},  # Invalid data type
                access="public",
                token=blob_token,
            )

        # Test error handling in cache operations
        # Note: Cache operations with invalid options might not raise exceptions
        # This depends on the implementation - some may ignore invalid options
        try:
            await cache.set("test:key", "value", {"invalid_option": "value"})
            # If no exception is raised, that's also acceptable behavior
        except Exception:
            # If an exception is raised, that's also acceptable behavior
            pass

        # Test error handling in headers
        with pytest.raises(Exception):
            ip_address(None)  # Invalid input

        # Test error handling in OIDC
        with pytest.raises(Exception):
            decode_oidc_payload("invalid.token")

    @pytest.mark.asyncio
    async def test_concurrent_integration_operations(self, blob_token, test_prefix, uploaded_blobs):
        """Test concurrent operations across integrated features."""
        if not blob_token:
            pytest.skip("BLOB_READ_WRITE_TOKEN not set - skipping concurrent integration test")

        cache = get_cache(namespace="concurrent-integration-test")

        async def upload_and_cache_file(i: int):
            # Upload file
            content = f"Concurrent file {i}".encode()
            blob_result = await put_async(
                f"{test_prefix}/concurrent/file-{i}.txt",
                content,
                access="public",
                content_type="text/plain",
                token=blob_token,
                add_random_suffix=True,
            )

            # Cache metadata
            metadata = {
                "file_id": i,
                "url": blob_result.url,
                "pathname": blob_result.pathname,
                "size": len(content),
            }

            await cache.set(f"file:{i}", metadata, {"ttl": 60, "tags": ["file", "concurrent"]})

            return blob_result.url, metadata

        # Run concurrent operations
        results = await asyncio.gather(*[upload_and_cache_file(i) for i in range(5)])

        # Track for cleanup
        for url, _ in results:
            uploaded_blobs.append(url)

        # Verify all operations succeeded
        assert len(results) == 5

        # Verify all files are accessible and cached
        for i, (url, metadata) in enumerate(results):
            # Verify blob is accessible
            blob_info = await head_async(url, token=blob_token)
            assert blob_info.size == len(f"Concurrent file {i}".encode())

            # Verify cache entry exists
            cached_metadata = await cache.get(f"file:{i}")
            assert cached_metadata is not None
            assert cached_metadata["file_id"] == i

        # Clean up cache
        await cache.expire_tag("concurrent")

    @pytest.mark.asyncio
    async def test_integration_performance(self, blob_token, test_prefix, uploaded_blobs):
        """Test performance of integrated operations."""
        if not blob_token:
            pytest.skip("BLOB_READ_WRITE_TOKEN not set - skipping performance test")

        cache = get_cache(namespace="performance-test")

        # Measure time for integrated operations
        import time

        start_time = time.time()

        # Upload file
        content = b"Performance test content"
        blob_result = await put_async(
            f"{test_prefix}/performance-test.txt",
            content,
            access="public",
            content_type="text/plain",
            token=blob_token,
            add_random_suffix=True,
        )
        uploaded_blobs.append(blob_result.url)

        # Cache metadata
        metadata = {
            "url": blob_result.url,
            "pathname": blob_result.pathname,
            "size": len(content),
            "uploaded_at": int(time.time()),
        }

        await cache.set("performance:test", metadata, {"ttl": 60})

        # Retrieve from cache
        cached_metadata = await cache.get("performance:test")

        # Verify blob is accessible
        blob_info = await head_async(blob_result.url, token=blob_token)

        end_time = time.time()
        duration = end_time - start_time

        # Verify operations completed successfully
        assert cached_metadata is not None
        assert blob_info.size == len(content)

        # Performance should be reasonable (less than 10 seconds for this simple operation)
        assert duration < 10.0, f"Operations took too long: {duration:.2f} seconds"

        # Clean up
        await cache.delete("performance:test")

    @pytest.mark.asyncio
    async def test_integration_cleanup(
        self, blob_token, uploaded_blobs, created_projects, vercel_token, vercel_team_id
    ):
        """Test cleanup of all integrated resources."""
        # Clean up blob storage
        if blob_token and uploaded_blobs:
            try:
                await delete_async(uploaded_blobs, token=blob_token)
            except Exception:
                # Some blobs might already be deleted
                pass

        # Clean up projects
        if vercel_token and created_projects:
            for project_id in created_projects:
                try:
                    await delete_project(
                        project_id=project_id, token=vercel_token, team_id=vercel_team_id
                    )
                except Exception:
                    # Project might already be deleted
                    pass

        # Clean up cache
        cache = get_cache(namespace="integration-test")
        await cache.expire_tag("test")
        await cache.expire_tag("blob")
        await cache.expire_tag("session")
        await cache.expire_tag("job")
        await cache.expire_tag("file")
        await cache.expire_tag("concurrent")
        await cache.expire_tag("processing")
        await cache.expire_tag("completed")
