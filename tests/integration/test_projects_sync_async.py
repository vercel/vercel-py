"""
Integration tests for Vercel Projects API sync/async functionality.

These tests verify that both sync and async versions of the projects API
work correctly and produce consistent results. They validate different
parts of the requests including parameters, body content, and error handling.
"""

import os
import pytest
from unittest.mock import patch, MagicMock

# Import both sync and async functions
from vercel.projects import get_projects, create_project, update_project, delete_project
from vercel.projects.projects import (
    get_projects_async,
    create_project_async,
    update_project_async,
    delete_project_async,
)


class TestProjectsAPI:
    """Test suite for Projects API sync/async functionality."""

    @pytest.fixture
    def mock_token(self):
        """Mock Vercel token for testing."""
        return "test_token_123"

    @pytest.fixture
    def mock_project_data(self):
        """Mock project data for testing based on actual API response."""
        return {
            "accountId": "team_7HmsszwpwmzzJZViREX6dLD0",
            "autoExposeSystemEnvs": True,
            "autoAssignCustomDomains": True,
            "autoAssignCustomDomainsUpdatedBy": "system",
            "buildCommand": None,
            "createdAt": 1640995200000,
            "devCommand": None,
            "directoryListing": False,
            "gitForkProtection": True,
            "gitLFS": False,
            "id": "prj_test123",
            "installCommand": None,
            "lastRollbackTarget": None,
            "lastAliasRequest": None,
            "name": "test-project",
            "nodeVersion": "22.x",
            "outputDirectory": None,
            "productionDeploymentsFastLane": True,
            "publicSource": None,
            "defaultResourceConfig": {
                "fluid": True,
                "functionDefaultRegions": ["iad1"],
                "functionDefaultTimeout": 300,
                "functionDefaultMemoryType": "standard",
                "functionZeroConfigFailover": False,
                "elasticConcurrencyEnabled": False,
            },
            "resourceConfig": {
                "fluid": True,
                "functionDefaultRegions": ["iad1"],
            },
            "rootDirectory": None,
            "serverlessFunctionRegion": "iad1",
            "skewProtectionMaxAge": 86400,
            "sourceFilesOutsideRootDirectory": True,
            "enableAffectedProjectsDeployments": True,
            "ssoProtection": {
                "deploymentType": "all_except_custom_domains",
            },
            "updatedAt": 1640995200000,
            "live": False,
            "gitComments": {
                "onCommit": False,
                "onPullRequest": True,
            },
            "gitProviderOptions": {
                "createDeployments": "enabled",
            },
            "oidcTokenConfig": {
                "enabled": True,
                "issuerMode": "team",
            },
            "latestDeployments": [],
            "targets": {},
            "deploymentExpiration": {
                "expirationDays": 180,
                "expirationDaysProduction": 365,
                "expirationDaysCanceled": 30,
                "expirationDaysErrored": 90,
                "deploymentsToKeep": 10,
            },
            "features": {
                "webAnalytics": False,
            },
        }

    @pytest.fixture
    def mock_projects_response(self):
        """Mock projects list response based on actual API structure."""
        return {
            "projects": [
                {
                    "accountId": "team_7HmsszwpwmzzJZViREX6dLD0",
                    "autoExposeSystemEnvs": True,
                    "autoAssignCustomDomains": True,
                    "autoAssignCustomDomainsUpdatedBy": "system",
                    "buildCommand": None,
                    "createdAt": 1640995200000,
                    "devCommand": None,
                    "directoryListing": False,
                    "framework": "nextjs",
                    "gitForkProtection": True,
                    "gitLFS": False,
                    "id": "prj_test123",
                    "installCommand": None,
                    "lastRollbackTarget": None,
                    "lastAliasRequest": None,
                    "name": "test-project",
                    "nodeVersion": "22.x",
                    "outputDirectory": None,
                    "productionDeploymentsFastLane": True,
                    "publicSource": None,
                    "defaultResourceConfig": {
                        "fluid": True,
                        "functionDefaultRegions": ["iad1"],
                        "functionDefaultTimeout": 300,
                        "functionDefaultMemoryType": "standard",
                        "functionZeroConfigFailover": False,
                        "elasticConcurrencyEnabled": False,
                    },
                    "resourceConfig": {
                        "fluid": True,
                        "functionDefaultRegions": ["iad1"],
                    },
                    "rootDirectory": None,
                    "skewProtectionMaxAge": 86400,
                    "sourceFilesOutsideRootDirectory": True,
                    "enableAffectedProjectsDeployments": True,
                    "ssoProtection": {
                        "deploymentType": "all_except_custom_domains",
                    },
                    "updatedAt": 1640995200000,
                    "live": False,
                    "gitComments": {
                        "onCommit": False,
                        "onPullRequest": True,
                    },
                    "gitProviderOptions": {
                        "createDeployments": "enabled",
                    },
                    "oidcTokenConfig": {
                        "enabled": True,
                        "issuerMode": "team",
                    },
                    "latestDeployments": [],
                    "targets": {},
                    "deploymentExpiration": {
                        "expirationDays": 180,
                        "expirationDaysProduction": 365,
                        "expirationDaysCanceled": 30,
                        "expirationDaysErrored": 90,
                        "deploymentsToKeep": 10,
                    },
                    "features": {
                        "webAnalytics": False,
                    },
                },
            ],
            "pagination": {
                "count": 1,
                "next": None,
                "prev": None,
            },
        }

    def test_get_projects_sync(self, mock_token, mock_projects_response):
        """Test sync get_projects function with comprehensive output validation."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_projects_response
            mock_request.return_value = mock_response

            result = get_projects(token=mock_token)

            # Validate response structure and content
            assert isinstance(result, dict)
            assert "projects" in result
            assert "pagination" in result

            # Validate projects array structure
            projects = result["projects"]
            assert isinstance(projects, list)
            assert len(projects) == 1

            # Validate individual project structure
            for project in projects:
                assert isinstance(project, dict)
                # Validate core required fields
                assert "id" in project
                assert "name" in project
                assert "accountId" in project
                assert "createdAt" in project
                assert "updatedAt" in project
                assert "framework" in project

                # Validate data types
                assert isinstance(project["id"], str)
                assert isinstance(project["name"], str)
                assert isinstance(project["accountId"], str)
                assert isinstance(project["createdAt"], int)
                assert isinstance(project["updatedAt"], int)
                assert project["framework"] is None or isinstance(project["framework"], str)

                # Validate project ID format (starts with prj_)
                assert project["id"].startswith("prj_")

                # Validate account ID format (starts with team_)
                assert project["accountId"].startswith("team_")

                # Validate timestamp values are reasonable (after 2020)
                assert project["createdAt"] > 1577836800000  # Jan 1, 2020
                assert project["updatedAt"] > 1577836800000  # Jan 1, 2020

                # Validate optional but common fields
                if "nodeVersion" in project:
                    assert isinstance(project["nodeVersion"], str)
                if "gitForkProtection" in project:
                    assert isinstance(project["gitForkProtection"], bool)
                if "live" in project:
                    assert isinstance(project["live"], bool)
                if "autoExposeSystemEnvs" in project:
                    assert isinstance(project["autoExposeSystemEnvs"], bool)

            # Validate pagination structure
            pagination = result["pagination"]
            assert isinstance(pagination, dict)
            assert "count" in pagination
            assert "next" in pagination
            assert "prev" in pagination
            assert pagination["count"] == 1
            assert pagination["next"] is None
            assert pagination["prev"] is None

            # Validate pagination data types
            assert isinstance(pagination["count"], int)
            assert pagination["next"] is None or isinstance(pagination["next"], int)
            assert pagination["prev"] is None or isinstance(pagination["prev"], int)

            # Validate request was made correctly
            mock_request.assert_called_once()
            call_args = mock_request.call_args

            # Validate HTTP method and path
            assert call_args[0][0] == "GET"  # method
            assert call_args[0][1] == "/v10/projects"  # path

            # Validate token parameter
            assert call_args[1]["token"] == mock_token

            # Validate no body for GET request (json parameter not passed when None)
            assert "json" not in call_args[1] or call_args[1]["json"] is None

    @pytest.mark.asyncio
    async def test_get_projects_async(self, mock_token, mock_projects_response):
        """Test async get_projects_async function with request validation."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_projects_response
            mock_request.return_value = mock_response

            result = await get_projects_async(token=mock_token)

            # Validate response
            assert result == mock_projects_response

            # Validate request was made correctly
            mock_request.assert_called_once()
            call_args = mock_request.call_args

            # Validate HTTP method and path
            assert call_args[0][0] == "GET"  # method
            assert call_args[0][1] == "/v10/projects"  # path

            # Validate token parameter
            assert call_args[1]["token"] == mock_token

            # Validate no body for GET request (json parameter not passed when None)
            assert "json" not in call_args[1] or call_args[1]["json"] is None

    def test_create_project_sync(self, mock_token, mock_project_data):
        """Test sync create_project function with comprehensive output validation."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.json.return_value = mock_project_data
            mock_request.return_value = mock_response

            project_body = {"name": "test-project", "framework": "nextjs"}
            result = create_project(body=project_body, token=mock_token)

            # Validate response structure and content
            assert isinstance(result, dict)

            # Validate core required fields
            assert "id" in result
            assert "name" in result
            assert "accountId" in result
            assert "createdAt" in result
            assert "updatedAt" in result

            # Validate data types
            assert isinstance(result["id"], str)
            assert isinstance(result["name"], str)
            assert isinstance(result["accountId"], str)
            assert isinstance(result["createdAt"], int)
            assert isinstance(result["updatedAt"], int)

            # Validate ID formats
            assert result["id"].startswith("prj_")
            assert result["accountId"].startswith("team_")

            # Validate timestamp values are reasonable
            assert result["createdAt"] > 1577836800000  # Jan 1, 2020
            assert result["updatedAt"] > 1577836800000  # Jan 1, 2020

            # Validate optional but common fields
            if "nodeVersion" in result:
                assert isinstance(result["nodeVersion"], str)
            if "gitForkProtection" in result:
                assert isinstance(result["gitForkProtection"], bool)
            if "live" in result:
                assert isinstance(result["live"], bool)
            if "autoExposeSystemEnvs" in result:
                assert isinstance(result["autoExposeSystemEnvs"], bool)
            if "defaultResourceConfig" in result:
                assert isinstance(result["defaultResourceConfig"], dict)
                assert "fluid" in result["defaultResourceConfig"]
                assert isinstance(result["defaultResourceConfig"]["fluid"], bool)

            # Validate specific values from mock
            assert result["id"] == "prj_test123"
            assert result["name"] == "test-project"
            assert result["accountId"] == "team_7HmsszwpwmzzJZViREX6dLD0"
            assert result["createdAt"] == 1640995200000
            assert result["updatedAt"] == 1640995200000

            # Validate request was made correctly
            mock_request.assert_called_once()
            call_args = mock_request.call_args

            # Validate HTTP method and path
            assert call_args[0][0] == "POST"  # method
            assert call_args[0][1] == "/v11/projects"  # path

            # Validate token parameter
            assert call_args[1]["token"] == mock_token

            # Validate request body
            assert call_args[1]["json"] == project_body

    @pytest.mark.asyncio
    async def test_create_project_async(self, mock_token, mock_project_data):
        """Test async create_project_async function with request validation."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.json.return_value = mock_project_data
            mock_request.return_value = mock_response

            project_body = {"name": "test-project", "framework": "nextjs"}
            result = await create_project_async(body=project_body, token=mock_token)

            # Validate response
            assert result == mock_project_data

            # Validate request was made correctly
            mock_request.assert_called_once()
            call_args = mock_request.call_args

            # Validate HTTP method and path
            assert call_args[0][0] == "POST"  # method
            assert call_args[0][1] == "/v11/projects"  # path

            # Validate token parameter
            assert call_args[1]["token"] == mock_token

            # Validate request body
            assert call_args[1]["json"] == project_body

    def test_update_project_sync(self, mock_token, mock_project_data):
        """Test sync update_project function with request validation."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_project_data
            mock_request.return_value = mock_response

            project_id = "test_project_123"
            update_body = {"framework": "nextjs", "buildCommand": "npm run build"}
            result = update_project(project_id, body=update_body, token=mock_token)

            # Validate response
            assert result == mock_project_data

            # Validate request was made correctly
            mock_request.assert_called_once()
            call_args = mock_request.call_args

            # Validate HTTP method and path
            assert call_args[0][0] == "PATCH"  # method
            assert call_args[0][1] == f"/v9/projects/{project_id}"  # path

            # Validate token parameter
            assert call_args[1]["token"] == mock_token

            # Validate request body
            assert call_args[1]["json"] == update_body

    @pytest.mark.asyncio
    async def test_update_project_async(self, mock_token, mock_project_data):
        """Test async update_project_async function with request validation."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_project_data
            mock_request.return_value = mock_response

            project_id = "test_project_123"
            update_body = {"framework": "nextjs", "buildCommand": "npm run build"}
            result = await update_project_async(project_id, body=update_body, token=mock_token)

            # Validate response
            assert result == mock_project_data

            # Validate request was made correctly
            mock_request.assert_called_once()
            call_args = mock_request.call_args

            # Validate HTTP method and path
            assert call_args[0][0] == "PATCH"  # method
            assert call_args[0][1] == f"/v9/projects/{project_id}"  # path

            # Validate token parameter
            assert call_args[1]["token"] == mock_token

            # Validate request body
            assert call_args[1]["json"] == update_body

    def test_delete_project_sync(self, mock_token):
        """Test sync delete_project function with request validation."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 204
            mock_request.return_value = mock_response

            project_id = "test_project_123"
            result = delete_project(project_id, token=mock_token)

            # Validate response
            assert result is None

            # Validate request was made correctly
            mock_request.assert_called_once()
            call_args = mock_request.call_args

            # Validate HTTP method and path
            assert call_args[0][0] == "DELETE"  # method
            assert call_args[0][1] == f"/v9/projects/{project_id}"  # path

            # Validate token parameter
            assert call_args[1]["token"] == mock_token

            # Validate no body for DELETE request (json parameter not passed when None)
            assert "json" not in call_args[1] or call_args[1]["json"] is None

    @pytest.mark.asyncio
    async def test_delete_project_async(self, mock_token):
        """Test async delete_project_async function with request validation."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 204
            mock_request.return_value = mock_response

            project_id = "test_project_123"
            result = await delete_project_async(project_id, token=mock_token)

            # Validate response
            assert result is None

            # Validate request was made correctly
            mock_request.assert_called_once()
            call_args = mock_request.call_args

            # Validate HTTP method and path
            assert call_args[0][0] == "DELETE"  # method
            assert call_args[0][1] == f"/v9/projects/{project_id}"  # path

            # Validate token parameter
            assert call_args[1]["token"] == mock_token

            # Validate no body for DELETE request (json parameter not passed when None)
            assert "json" not in call_args[1] or call_args[1]["json"] is None

    def test_get_projects_with_team_id_sync(self, mock_token):
        """Test sync get_projects with team_id parameter validation."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"projects": []}
            mock_request.return_value = mock_response

            team_id = "team_123"
            get_projects(token=mock_token, team_id=team_id)

            # Validate request was made with correct params
            call_args = mock_request.call_args
            params = call_args[1]["params"]
            assert params["teamId"] == team_id

    @pytest.mark.asyncio
    async def test_get_projects_with_team_id_async(self, mock_token):
        """Test async get_projects_async with team_id parameter validation."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"projects": []}
            mock_request.return_value = mock_response

            team_id = "team_123"
            await get_projects_async(token=mock_token, team_id=team_id)

            # Validate request was made with correct params
            call_args = mock_request.call_args
            params = call_args[1]["params"]
            assert params["teamId"] == team_id

    def test_get_projects_with_query_params_sync(self, mock_token):
        """Test sync get_projects with query parameters validation."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"projects": []}
            mock_request.return_value = mock_response

            query_params = {"search": "test", "limit": 10}
            get_projects(token=mock_token, query=query_params)

            # Validate request was made with correct params
            call_args = mock_request.call_args
            params = call_args[1]["params"]
            assert params["search"] == "test"
            assert params["limit"] == 10

    @pytest.mark.asyncio
    async def test_get_projects_with_query_params_async(self, mock_token):
        """Test async get_projects_async with query parameters validation."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"projects": []}
            mock_request.return_value = mock_response

            query_params = {"search": "test", "limit": 10}
            await get_projects_async(token=mock_token, query=query_params)

            # Validate request was made with correct params
            call_args = mock_request.call_args
            params = call_args[1]["params"]
            assert params["search"] == "test"
            assert params["limit"] == 10

    def test_create_project_with_team_id_sync(self, mock_token, mock_project_data):
        """Test sync create_project with team_id parameter validation."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.json.return_value = mock_project_data
            mock_request.return_value = mock_response

            project_body = {"name": "test-project"}
            team_id = "team_123"
            create_project(body=project_body, token=mock_token, team_id=team_id)

            # Validate request was made with correct params
            call_args = mock_request.call_args
            params = call_args[1]["params"]
            assert params["teamId"] == team_id

    @pytest.mark.asyncio
    async def test_create_project_with_team_id_async(self, mock_token, mock_project_data):
        """Test async create_project_async with team_id parameter validation."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.json.return_value = mock_project_data
            mock_request.return_value = mock_response

            project_body = {"name": "test-project"}
            team_id = "team_123"
            await create_project_async(body=project_body, token=mock_token, team_id=team_id)

            # Validate request was made with correct params
            call_args = mock_request.call_args
            params = call_args[1]["params"]
            assert params["teamId"] == team_id

    def test_error_handling_sync(self, mock_token):
        """Test sync error handling with comprehensive output validation."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 400
            mock_response.reason_phrase = "Bad Request"
            mock_response.json.return_value = {"error": "Invalid request"}
            mock_request.return_value = mock_response

            # Validate that the correct exception is raised
            with pytest.raises(RuntimeError) as exc_info:
                get_projects(token=mock_token)

            # Validate error message content
            error_message = str(exc_info.value)
            assert "Failed to get projects" in error_message
            assert "400" in error_message
            assert "Bad Request" in error_message
            assert "Invalid request" in error_message

            # Validate that the request was still made
            mock_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling_async(self, mock_token):
        """Test async error handling with detailed validation."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 400
            mock_response.reason_phrase = "Bad Request"
            mock_response.json.return_value = {"error": "Invalid request"}
            mock_request.return_value = mock_response

            with pytest.raises(RuntimeError, match="Failed to get projects"):
                await get_projects_async(token=mock_token)

    def test_missing_token_error_sync(self):
        """Test sync functions raise error when token is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(RuntimeError, match="Missing Vercel API token"):
                get_projects()

    @pytest.mark.asyncio
    async def test_missing_token_error_async(self):
        """Test async functions raise error when token is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(RuntimeError, match="Missing Vercel API token"):
                await get_projects_async()

    def test_timeout_parameter_sync(self, mock_token):
        """Test sync functions accept timeout parameter."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"projects": []}
            mock_request.return_value = mock_response

            get_projects(token=mock_token, timeout=120.0)

            # Validate timeout was passed correctly
            call_args = mock_request.call_args
            assert call_args[1]["timeout"] == 120.0

    @pytest.mark.asyncio
    async def test_timeout_parameter_async(self, mock_token):
        """Test async functions accept timeout parameter."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"projects": []}
            mock_request.return_value = mock_response

            await get_projects_async(token=mock_token, timeout=120.0)

            # Validate timeout was passed correctly
            call_args = mock_request.call_args
            assert call_args[1]["timeout"] == 120.0

    def test_base_url_parameter_sync(self, mock_token):
        """Test sync functions accept base_url parameter."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"projects": []}
            mock_request.return_value = mock_response

            custom_base_url = "https://custom-api.example.com"
            get_projects(token=mock_token, base_url=custom_base_url)

            # Validate base_url was passed correctly
            call_args = mock_request.call_args
            assert call_args[1]["base_url"] == custom_base_url

    @pytest.mark.asyncio
    async def test_base_url_parameter_async(self, mock_token):
        """Test async functions accept base_url parameter."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"projects": []}
            mock_request.return_value = mock_response

            custom_base_url = "https://custom-api.example.com"
            await get_projects_async(token=mock_token, base_url=custom_base_url)

            # Validate base_url was passed correctly
            call_args = mock_request.call_args
            assert call_args[1]["base_url"] == custom_base_url


class TestConsistency:
    """Test that sync and async versions produce consistent results."""

    @pytest.mark.asyncio
    async def test_sync_async_consistency(self):
        """Test that sync and async versions produce the same results."""
        mock_response_data = {
            "projects": [{"id": "proj_1", "name": "test"}],
            "pagination": {"count": 1},
        }

        # Mock both sync and async request functions
        with (
            patch("vercel.projects.projects._request") as mock_sync_request,
            patch("vercel.projects.projects._request_async") as mock_async_request,
        ):
            # Setup mock responses
            mock_sync_response = MagicMock()
            mock_sync_response.status_code = 200
            mock_sync_response.json.return_value = mock_response_data
            mock_sync_request.return_value = mock_sync_response

            mock_async_response = MagicMock()
            mock_async_response.status_code = 200
            mock_async_response.json.return_value = mock_response_data
            mock_async_request.return_value = mock_async_response

            # Call both versions
            sync_result = get_projects(token="test_token")
            async_result = await get_projects_async(token="test_token")

            # Results should be identical
            assert sync_result == async_result
            assert sync_result == mock_response_data
            assert async_result == mock_response_data


if __name__ == "__main__":
    pytest.main([__file__])
