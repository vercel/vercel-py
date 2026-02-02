"""Tests for the core client with namespaced sub-clients."""

import httpx
import pytest
import respx

from vercel._core import (
    AsyncVercelClient,
    ClientConfig,
    VercelClient,
)


@pytest.fixture
def api_mock():
    """Mock API for testing."""
    with respx.mock(assert_all_called=False, base_url="https://api.vercel.com") as mock:
        # GET /v10/projects
        mock.get("/v10/projects").mock(
            return_value=httpx.Response(
                200, json={"projects": [{"id": "1"}, {"id": "2"}], "pagination": {}}
            )
        )

        # POST /v11/projects
        mock.post("/v11/projects").mock(
            return_value=httpx.Response(200, json={"id": "new", "name": "Created"})
        )

        # PATCH /v9/projects/:id
        mock.patch("/v9/projects/123").mock(
            return_value=httpx.Response(200, json={"id": "123", "name": "Updated"})
        )

        # DELETE /v9/projects/:id
        mock.delete("/v9/projects/123").mock(return_value=httpx.Response(204))

        # POST /v13/deployments
        mock.post("/v13/deployments").mock(
            return_value=httpx.Response(200, json={"id": "dpl_1", "url": "test.vercel.app"})
        )

        # POST /v2/files
        mock.post("/v2/files").mock(
            return_value=httpx.Response(200, json={"url": "file-url"})
        )

        yield mock


class TestSyncClient:
    """Test synchronous VercelClient with namespaced API."""

    def test_projects_list(self, api_mock):
        """Test client.projects.list()."""
        with VercelClient(access_token="test-token") as client:
            response = client.projects.list()

        assert "projects" in response
        assert len(response["projects"]) == 2

    def test_projects_list_with_team(self, api_mock):
        """Test client.projects.list() with team_id."""
        with VercelClient(access_token="test-token") as client:
            response = client.projects.list(team_id="team_123")

        assert "projects" in response
        request = api_mock.calls[0].request
        assert "teamId=team_123" in str(request.url)

    def test_projects_create(self, api_mock):
        """Test client.projects.create()."""
        with VercelClient(access_token="test-token") as client:
            response = client.projects.create(body={"name": "my-project"})

        assert response["id"] == "new"
        assert response["name"] == "Created"

    def test_projects_update(self, api_mock):
        """Test client.projects.update()."""
        with VercelClient(access_token="test-token") as client:
            response = client.projects.update("123", body={"name": "Updated"})

        assert response["id"] == "123"
        assert response["name"] == "Updated"

    def test_projects_delete(self, api_mock):
        """Test client.projects.delete()."""
        with VercelClient(access_token="test-token") as client:
            result = client.projects.delete("123")

        assert result is None

    def test_deployments_create(self, api_mock):
        """Test client.deployments.create()."""
        with VercelClient(access_token="test-token") as client:
            response = client.deployments.create(body={"name": "my-deployment"})

        assert response["id"] == "dpl_1"
        assert response["url"] == "test.vercel.app"

    def test_deployments_upload_file(self, api_mock):
        """Test client.deployments.upload_file()."""
        with VercelClient(access_token="test-token") as client:
            response = client.deployments.upload_file(
                content=b"hello world",
                content_length=11,
                x_vercel_digest="abc123",
            )

        assert response["url"] == "file-url"


class TestAsyncClient:
    """Test asynchronous AsyncVercelClient with namespaced API."""

    @pytest.mark.asyncio
    async def test_projects_list(self, api_mock):
        """Test await client.projects.list()."""
        async with AsyncVercelClient(access_token="test-token") as client:
            response = await client.projects.list()

        assert "projects" in response
        assert len(response["projects"]) == 2

    @pytest.mark.asyncio
    async def test_projects_list_with_team(self, api_mock):
        """Test await client.projects.list() with team_id."""
        async with AsyncVercelClient(access_token="test-token") as client:
            response = await client.projects.list(team_id="team_123")

        assert "projects" in response
        request = api_mock.calls[0].request
        assert "teamId=team_123" in str(request.url)

    @pytest.mark.asyncio
    async def test_projects_create(self, api_mock):
        """Test await client.projects.create()."""
        async with AsyncVercelClient(access_token="test-token") as client:
            response = await client.projects.create(body={"name": "my-project"})

        assert response["id"] == "new"
        assert response["name"] == "Created"

    @pytest.mark.asyncio
    async def test_projects_update(self, api_mock):
        """Test await client.projects.update()."""
        async with AsyncVercelClient(access_token="test-token") as client:
            response = await client.projects.update("123", body={"name": "Updated"})

        assert response["id"] == "123"
        assert response["name"] == "Updated"

    @pytest.mark.asyncio
    async def test_projects_delete(self, api_mock):
        """Test await client.projects.delete()."""
        async with AsyncVercelClient(access_token="test-token") as client:
            result = await client.projects.delete("123")

        assert result is None

    @pytest.mark.asyncio
    async def test_deployments_create(self, api_mock):
        """Test await client.deployments.create()."""
        async with AsyncVercelClient(access_token="test-token") as client:
            response = await client.deployments.create(body={"name": "my-deployment"})

        assert response["id"] == "dpl_1"
        assert response["url"] == "test.vercel.app"

    @pytest.mark.asyncio
    async def test_deployments_upload_file(self, api_mock):
        """Test await client.deployments.upload_file()."""
        async with AsyncVercelClient(access_token="test-token") as client:
            response = await client.deployments.upload_file(
                content=b"hello world",
                content_length=11,
                x_vercel_digest="abc123",
            )

        assert response["url"] == "file-url"


class TestClientConfig:
    """Test ClientConfig."""

    def test_resolve_token_from_config(self):
        """Test token resolution from config."""
        config = ClientConfig(access_token="my-token")
        assert config.resolve_token() == "my-token"

    def test_resolve_token_missing(self, monkeypatch):
        """Test token resolution fails when missing."""
        monkeypatch.delenv("VERCEL_TOKEN", raising=False)
        config = ClientConfig()
        with pytest.raises(RuntimeError, match="Missing Vercel API token"):
            config.resolve_token()

    def test_resolve_token_from_env(self, monkeypatch):
        """Test token resolution from environment."""
        monkeypatch.setenv("VERCEL_TOKEN", "env-token")
        config = ClientConfig()
        assert config.resolve_token() == "env-token"

    def test_build_url(self):
        """Test URL building."""
        config = ClientConfig(base_url="https://api.vercel.com")
        assert config.build_url("/v10/projects") == "https://api.vercel.com/v10/projects"


class TestCodeSharing:
    """Tests demonstrating that business logic is truly shared."""

    def test_same_config_structure(self):
        """Both clients accept the same config structure."""
        sync_client = VercelClient(
            access_token="test",
            base_url="https://api.vercel.com",
            timeout=30.0,
            default_team_id="team_123",
        )
        async_client = AsyncVercelClient(
            access_token="test",
            base_url="https://api.vercel.com",
            timeout=30.0,
            default_team_id="team_123",
        )

        assert sync_client._config.access_token == async_client._config.access_token
        assert sync_client._config.base_url == async_client._config.base_url
        assert sync_client._config.timeout == async_client._config.timeout
        assert sync_client._config.default_team_id == async_client._config.default_team_id

        sync_client.close()

    def test_sub_clients_share_transport(self):
        """Sub-clients share the same transport instance."""
        with VercelClient(access_token="test") as client:
            assert client.projects._transport is client.deployments._transport
            assert client.projects._transport is client._transport


class TestErrorHandling:
    """Test error handling."""

    def test_sync_client_error_response(self):
        """Test sync client handles error responses."""
        with respx.mock(base_url="https://api.vercel.com") as mock:
            mock.get("/v10/projects").mock(
                return_value=httpx.Response(401, json={"error": "Unauthorized"})
            )

            with pytest.raises(RuntimeError, match="Failed to list projects"):
                with VercelClient(access_token="test-token") as client:
                    client.projects.list()

    @pytest.mark.asyncio
    async def test_async_client_error_response(self):
        """Test async client handles error responses."""
        with respx.mock(base_url="https://api.vercel.com") as mock:
            mock.get("/v10/projects").mock(
                return_value=httpx.Response(401, json={"error": "Unauthorized"})
            )

            with pytest.raises(RuntimeError, match="Failed to list projects"):
                async with AsyncVercelClient(access_token="test-token") as client:
                    await client.projects.list()

    def test_deployments_create_body_validation(self):
        """Test deployment body validation is shared."""
        with VercelClient(access_token="test-token") as client:
            with pytest.raises(ValueError, match="body must be a dict"):
                client.deployments.create(body="not a dict")  # type: ignore

    @pytest.mark.asyncio
    async def test_async_deployments_create_body_validation(self):
        """Test async deployment body validation is shared."""
        async with AsyncVercelClient(access_token="test-token") as client:
            with pytest.raises(ValueError, match="body must be a dict"):
                await client.deployments.create(body="not a dict")  # type: ignore
