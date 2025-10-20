# E2E Tests for Vercel Python SDK

This directory contains comprehensive end-to-end tests for the Vercel Python SDK, covering all major workflows and integrations.

## Test Structure

### E2E Tests (`tests/e2e/`)
- **`test_cache_e2e.py`** - Runtime cache functionality (set/get/delete/expire_tag)
- **`test_blob_e2e.py`** - Blob storage operations (put/head/list/copy/delete)
- **`test_oidc_e2e.py`** - OIDC token functionality
- **`test_headers_e2e.py`** - Headers and geolocation functionality
- **`test_projects_e2e.py`** - Projects API operations
- **`conftest.py`** - Test configuration and utilities

### Integration Tests (`tests/integration/`)
- **`test_integration_e2e.py`** - Tests combining multiple SDK features

## Environment Setup

### Required Environment Variables

The e2e tests require the following environment variables to be set:

```bash
# Blob Storage
BLOB_READ_WRITE_TOKEN=your_blob_token_here

# Vercel API
VERCEL_TOKEN=your_vercel_token_here
VERCEL_PROJECT_ID=your_project_id_here
VERCEL_TEAM_ID=your_team_id_here

# OIDC
VERCEL_OIDC_TOKEN=your_oidc_token_here
```

### GitHub Actions Secrets

For running e2e tests in GitHub Actions, set these secrets in your repository:

- `BLOB_READ_WRITE_TOKEN`
- `VERCEL_TOKEN`
- `VERCEL_PROJECT_ID`
- `VERCEL_TEAM_ID`
- `VERCEL_OIDC_TOKEN`

## Running Tests

### Using the Test Runner

```bash
# Run all tests
python run_e2e_tests.py

# Run specific test types
python run_e2e_tests.py --test-type e2e
python run_e2e_tests.py --test-type integration
python run_e2e_tests.py --test-type examples

# Run tests matching a pattern
python run_e2e_tests.py --test-type e2e --pattern "cache"

# Check environment configuration
python run_e2e_tests.py --check-env
```

### Using pytest directly

```bash
# Run all e2e tests
pytest tests/e2e/ -v

# Run integration tests
pytest tests/integration/ -v

# Run specific test file
pytest tests/e2e/test_cache_e2e.py -v

# Run tests matching a pattern
pytest tests/e2e/ -k "cache" -v
```

## Test Features

### Cache Tests
- Basic cache operations (set/get/delete)
- TTL expiration
- Tag-based invalidation
- Namespace isolation
- Concurrent operations
- Fallback to in-memory cache when runtime cache is unavailable

**Note**: Vercel uses HTTP caching headers and Data Cache for production caching. These tests validate the in-memory cache implementation and ensure the SDK works correctly in all environments.

### Blob Storage Tests
- File upload and download
- Metadata retrieval
- File listing and copying
- Folder creation
- Multipart uploads
- Progress callbacks
- Different access levels
- Error handling

### OIDC Tests
- Token retrieval and validation
- Token payload decoding
- Token refresh functionality
- Error handling
- Concurrent access

### Headers Tests
- IP address extraction
- Geolocation data extraction
- Flag emoji generation
- URL decoding
- Request context management
- Framework integration

### Projects API Tests
- Project listing and creation
- Project updates and deletion
- Pagination
- Team scoping
- Error handling
- Concurrent operations

### Integration Tests
- Cache + Blob storage workflows
- Headers + OIDC + Cache workflows
- Projects API + Blob storage workflows
- Full application scenarios
- Error handling across features
- Performance testing

## Test Configuration

The tests use a configuration system that:

- Automatically skips tests when required tokens are not available
- Provides unique test prefixes to avoid conflicts
- Tracks resources for cleanup
- Supports conditional test execution

## Cleanup

Tests automatically clean up resources they create:

- Blob storage files are deleted
- Projects are removed
- Cache entries are expired
- Temporary data is cleaned up

## Continuous Integration

The e2e tests are integrated into the GitHub Actions workflow:

- Run on pull requests and pushes to main
- Skip gracefully when secrets are not available
- Include timeout protection
- Provide detailed output for debugging

## Troubleshooting

### Common Issues

1. **Tests skipped**: Check that required environment variables are set
2. **Timeout errors**: Increase timeout values for slow operations
3. **Cleanup failures**: Some resources might already be deleted
4. **Token expiration**: Refresh tokens before running tests

### Debug Mode

Enable debug logging by setting:

```bash
export SUSPENSE_CACHE_DEBUG=true
```

### Local Development

For local development, you can run individual test files:

```bash
# Test cache functionality
pytest tests/e2e/test_cache_e2e.py::TestRuntimeCacheE2E::test_cache_set_get_basic -v

# Test blob storage
pytest tests/e2e/test_blob_e2e.py::TestBlobStorageE2E::test_blob_put_and_head -v
```

## Contributing

When adding new e2e tests:

1. Follow the existing test structure
2. Use the configuration system for environment setup
3. Include proper cleanup in teardown
4. Add appropriate skip conditions
5. Test both success and error scenarios
6. Include performance considerations for slow operations
