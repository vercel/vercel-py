"""Fixtures for integration tests using respx mocking."""

import pytest

# API base URLs
BLOB_API_BASE = "https://blob.vercel-storage.com"
VERCEL_API_BASE = "https://api.vercel.com"
SANDBOX_API_BASE = "https://sandbox.vercel.com"


# =============================================================================
# Blob Module Mock Responses
# =============================================================================


@pytest.fixture
def mock_blob_put_response() -> dict:
    """Mock response for blob put operation."""
    return {
        "url": f"{BLOB_API_BASE}/test-abc123/test.txt",
        "downloadUrl": f"{BLOB_API_BASE}/test-abc123/test.txt?download=1",
        "pathname": "test.txt",
        "contentType": "text/plain",
        "contentDisposition": 'inline; filename="test.txt"',
    }


@pytest.fixture
def mock_blob_head_response() -> dict:
    """Mock response for blob head operation."""
    return {
        "url": f"{BLOB_API_BASE}/test-abc123/test.txt",
        "downloadUrl": f"{BLOB_API_BASE}/test-abc123/test.txt?download=1",
        "pathname": "test.txt",
        "contentType": "text/plain",
        "contentDisposition": 'inline; filename="test.txt"',
        "size": 13,
        "uploadedAt": "2024-01-15T10:30:00.000Z",
        "cacheControl": "max-age=31536000",
    }


@pytest.fixture
def mock_blob_list_response() -> dict:
    """Mock response for blob list operation."""
    return {
        "blobs": [
            {
                "url": f"{BLOB_API_BASE}/test-abc123/file1.txt",
                "downloadUrl": f"{BLOB_API_BASE}/test-abc123/file1.txt?download=1",
                "pathname": "file1.txt",
                "contentType": "text/plain",
                "contentDisposition": 'inline; filename="file1.txt"',
                "size": 100,
                "uploadedAt": "2024-01-15T10:30:00.000Z",
            },
            {
                "url": f"{BLOB_API_BASE}/test-abc123/file2.txt",
                "downloadUrl": f"{BLOB_API_BASE}/test-abc123/file2.txt?download=1",
                "pathname": "file2.txt",
                "contentType": "text/plain",
                "contentDisposition": 'inline; filename="file2.txt"',
                "size": 200,
                "uploadedAt": "2024-01-15T10:31:00.000Z",
            },
        ],
        "cursor": None,
        "hasMore": False,
        "folders": [],
    }


@pytest.fixture
def mock_blob_list_response_paginated() -> dict:
    """Mock response for paginated blob list operation."""
    return {
        "blobs": [
            {
                "url": f"{BLOB_API_BASE}/test-abc123/page1.txt",
                "downloadUrl": f"{BLOB_API_BASE}/test-abc123/page1.txt?download=1",
                "pathname": "page1.txt",
                "contentType": "text/plain",
                "contentDisposition": 'inline; filename="page1.txt"',
                "size": 50,
                "uploadedAt": "2024-01-15T10:30:00.000Z",
            },
        ],
        "cursor": "next_cursor_abc123",
        "hasMore": True,
        "folders": [],
    }


@pytest.fixture
def mock_blob_create_folder_response() -> dict:
    """Mock response for create folder operation."""
    return {
        "url": f"{BLOB_API_BASE}/test-abc123/my-folder/",
        "pathname": "my-folder/",
    }


@pytest.fixture
def mock_blob_copy_response() -> dict:
    """Mock response for blob copy operation."""
    return {
        "url": f"{BLOB_API_BASE}/test-abc123/copied.txt",
        "downloadUrl": f"{BLOB_API_BASE}/test-abc123/copied.txt?download=1",
        "pathname": "copied.txt",
        "contentType": "text/plain",
        "contentDisposition": 'inline; filename="copied.txt"',
    }


# =============================================================================
# Sandbox Module Mock Responses
# =============================================================================


@pytest.fixture
def mock_sandbox_create_response() -> dict:
    """Mock response for sandbox create operation.

    Matches the Sandbox model schema with all required fields.
    """
    return {
        "id": "sbx_test123456",
        "memory": 512,
        "vcpus": 1,
        "region": "iad1",
        "runtime": "nodejs20.x",
        "timeout": 300,
        "status": "running",
        "requestedAt": 1705320600000,
        "startedAt": 1705320601000,
        "requestedStopAt": None,
        "stoppedAt": None,
        "duration": None,
        "sourceSnapshotId": None,
        "snapshottedAt": None,
        "createdAt": 1705320600000,
        "cwd": "/app",
        "updatedAt": 1705320601000,
        "interactivePort": None,
    }


@pytest.fixture
def mock_sandbox_get_response() -> dict:
    """Mock response for sandbox get operation."""
    return {
        "id": "sbx_test123456",
        "memory": 512,
        "vcpus": 1,
        "region": "iad1",
        "runtime": "nodejs20.x",
        "timeout": 300,
        "status": "running",
        "requestedAt": 1705320600000,
        "startedAt": 1705320601000,
        "requestedStopAt": None,
        "stoppedAt": None,
        "duration": None,
        "sourceSnapshotId": None,
        "snapshottedAt": None,
        "createdAt": 1705320600000,
        "cwd": "/app",
        "updatedAt": 1705320601000,
        "interactivePort": None,
    }


@pytest.fixture
def mock_sandbox_command_response() -> dict:
    """Mock response for sandbox run_command operation."""
    return {
        "commandId": "cmd_test123",
        "exitCode": 0,
        "stdout": "Hello, World!\n",
        "stderr": "",
    }


@pytest.fixture
def mock_sandbox_command_detached_response() -> dict:
    """Mock response for sandbox run_command_detached operation."""
    return {
        "commandId": "cmd_detached_test123",
        "status": "running",
    }


@pytest.fixture
def mock_sandbox_read_file_content() -> bytes:
    """Mock content for sandbox read_file operation."""
    return b"file content from sandbox"


@pytest.fixture
def mock_sandbox_snapshot_response() -> dict:
    """Mock response for sandbox snapshot operation.

    Matches the Snapshot model schema with all required fields.
    """
    return {
        "id": "snap_test123456",
        "sourceSandboxId": "sbx_test123456",
        "region": "iad1",
        "status": "created",
        "sizeBytes": 1024000,
        "expiresAt": 1705924600000,
        "createdAt": 1705320600000,
        "updatedAt": 1705320600000,
    }


# =============================================================================
# Cache Module Mock Responses
# =============================================================================


@pytest.fixture
def mock_cache_get_response() -> dict:
    """Mock response for cache get operation."""
    return {
        "value": "cached_value_123",
        "status": "HIT",
    }


@pytest.fixture
def mock_cache_set_response() -> dict:
    """Mock response for cache set operation."""
    return {
        "status": "OK",
    }


@pytest.fixture
def mock_cache_delete_response() -> dict:
    """Mock response for cache delete operation."""
    return {
        "status": "OK",
    }


# =============================================================================
# OIDC Module Mock Responses
# =============================================================================


@pytest.fixture
def mock_oidc_token() -> str:
    """Mock OIDC JWT token for testing.

    This is a valid JWT structure with a test payload.
    Header: {"alg": "RS256", "typ": "JWT"}
    Payload: {"sub": "test_subject", "aud": "vercel",
              "iss": "https://oidc.vercel.com", "exp": 9999999999}
    """
    # This is a properly formatted JWT (though not cryptographically valid)
    # Base64url encoded: header.payload.signature
    header = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"
    # fmt: off
    payload = "eyJzdWIiOiJ0ZXN0X3N1YmplY3QiLCJhdWQiOiJ2ZXJjZWwiLCJpc3MiOiJodHRwczovL29pZGMudmVyY2VsLmNvbSIsImV4cCI6OTk5OTk5OTk5OX0"  # noqa: E501
    # fmt: on
    signature = "test_signature"
    return f"{header}.{payload}.{signature}"


@pytest.fixture
def mock_oidc_token_payload() -> dict:
    """Mock decoded OIDC token payload."""
    return {
        "sub": "test_subject",
        "aud": "vercel",
        "iss": "https://oidc.vercel.com",
        "exp": 9999999999,
    }


# =============================================================================
# Projects Module Mock Responses (for reference/compatibility)
# =============================================================================


@pytest.fixture
def mock_project_data() -> dict:
    """Mock project data based on actual API response structure."""
    return {
        "id": "prj_abc123456789",
        "name": "test-project",
        "accountId": "team_test123456789",
        "createdAt": 1705320600000,
        "updatedAt": 1705320600000,
        "framework": None,
        "devCommand": None,
        "installCommand": None,
        "buildCommand": None,
        "outputDirectory": None,
        "rootDirectory": None,
        "nodeVersion": "20.x",
        "serverlessFunctionRegion": None,
        "sourceFilesOutsideRootDirectory": False,
        "speedInsights": None,
        "webAnalytics": None,
        "autoAssignCustomDomains": True,
        "autoAssignCustomDomainsUpdatedBy": None,
        "gitForkProtection": True,
        "directoryListing": False,
        "skewProtectionBoundaryAt": None,
        "skewProtectionMaxAge": None,
    }


@pytest.fixture
def mock_projects_list_response(mock_project_data: dict) -> dict:
    """Mock projects list response with pagination."""
    return {
        "projects": [mock_project_data],
        "pagination": {
            "count": 1,
            "next": None,
            "prev": None,
        },
    }


# =============================================================================
# Error Response Fixtures
# =============================================================================


@pytest.fixture
def mock_error_not_found() -> dict:
    """Mock 404 Not Found error response."""
    return {
        "error": {
            "code": "not_found",
            "message": "The requested resource was not found.",
        }
    }


@pytest.fixture
def mock_error_unauthorized() -> dict:
    """Mock 401 Unauthorized error response."""
    return {
        "error": {
            "code": "unauthorized",
            "message": "Authentication required.",
        }
    }


@pytest.fixture
def mock_error_forbidden() -> dict:
    """Mock 403 Forbidden error response."""
    return {
        "error": {
            "code": "forbidden",
            "message": "You do not have permission to access this resource.",
        }
    }


@pytest.fixture
def mock_error_bad_request() -> dict:
    """Mock 400 Bad Request error response."""
    return {
        "error": {
            "code": "bad_request",
            "message": "The request was invalid.",
        }
    }


@pytest.fixture
def mock_error_server_error() -> dict:
    """Mock 500 Internal Server Error response."""
    return {
        "error": {
            "code": "internal_server_error",
            "message": "An unexpected error occurred.",
        }
    }
