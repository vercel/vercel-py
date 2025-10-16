"""
Integration tests for Vercel Projects API sync/async functionality.

These tests verify that both sync and async versions of the projects API
work correctly and produce consistent results.
"""

import asyncio
import os
import pytest
from datetime import datetime
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
        """Mock project data for testing."""
        return {
            "id": "test_project_123",
            "name": "test-project",
            "framework": "nextjs",
            "createdAt": 1640995200000,
            "updatedAt": 1640995200000,
        }
    
    @pytest.fixture
    def mock_projects_response(self):
        """Mock projects list response."""
        return {
            "projects": [
                {
                    "id": "proj_1",
                    "name": "project-1",
                    "framework": "nextjs",
                },
                {
                    "id": "proj_2", 
                    "name": "project-2",
                    "framework": "react",
                },
            ],
            "pagination": {
                "count": 2,
                "next": None,
                "prev": None,
            },
        }
    
    def test_get_projects_sync(self, mock_token, mock_projects_response):
        """Test sync get_projects function."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_projects_response
            mock_request.return_value = mock_response
            
            result = get_projects(token=mock_token)
            
            assert result == mock_projects_response
            mock_request.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_projects_async(self, mock_token, mock_projects_response):
        """Test async get_projects_async function."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_projects_response
            mock_request.return_value = mock_response
            
            result = await get_projects_async(token=mock_token)
            
            assert result == mock_projects_response
            mock_request.assert_called_once()
    
    def test_create_project_sync(self, mock_token, mock_project_data):
        """Test sync create_project function."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.json.return_value = mock_project_data
            mock_request.return_value = mock_response
            
            project_body = {"name": "test-project", "framework": "nextjs"}
            result = create_project(body=project_body, token=mock_token)
            
            assert result == mock_project_data
            mock_request.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_project_async(self, mock_token, mock_project_data):
        """Test async create_project_async function."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.json.return_value = mock_project_data
            mock_request.return_value = mock_response
            
            project_body = {"name": "test-project", "framework": "nextjs"}
            result = await create_project_async(body=project_body, token=mock_token)
            
            assert result == mock_project_data
            mock_request.assert_called_once()
    
    def test_update_project_sync(self, mock_token, mock_project_data):
        """Test sync update_project function."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_project_data
            mock_request.return_value = mock_response
            
            project_id = "test_project_123"
            update_body = {"framework": "nextjs", "buildCommand": "npm run build"}
            result = update_project(project_id, body=update_body, token=mock_token)
            
            assert result == mock_project_data
            mock_request.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_project_async(self, mock_token, mock_project_data):
        """Test async update_project_async function."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_project_data
            mock_request.return_value = mock_response
            
            project_id = "test_project_123"
            update_body = {"framework": "nextjs", "buildCommand": "npm run build"}
            result = await update_project_async(project_id, body=update_body, token=mock_token)
            
            assert result == mock_project_data
            mock_request.assert_called_once()
    
    def test_delete_project_sync(self, mock_token):
        """Test sync delete_project function."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 204
            mock_request.return_value = mock_response
            
            project_id = "test_project_123"
            result = delete_project(project_id, token=mock_token)
            
            assert result is None
            mock_request.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_project_async(self, mock_token):
        """Test async delete_project_async function."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 204
            mock_request.return_value = mock_response
            
            project_id = "test_project_123"
            result = await delete_project_async(project_id, token=mock_token)
            
            assert result is None
            mock_request.assert_called_once()
    
    def test_error_handling_sync(self, mock_token):
        """Test sync error handling."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 400
            mock_response.reason_phrase = "Bad Request"
            mock_response.json.return_value = {"error": "Invalid request"}
            mock_request.return_value = mock_response
            
            with pytest.raises(RuntimeError, match="Failed to get projects"):
                get_projects(token=mock_token)
    
    @pytest.mark.asyncio
    async def test_error_handling_async(self, mock_token):
        """Test async error handling."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 400
            mock_response.reason_phrase = "Bad Request"
            mock_response.json.return_value = {"error": "Invalid request"}
            mock_request.return_value = mock_response
            
            with pytest.raises(RuntimeError, match="Failed to get projects"):
                await get_projects_async(token=mock_token)
    
    def test_team_id_parameter_sync(self, mock_token):
        """Test that team_id parameter is passed correctly in sync functions."""
        with patch("vercel.projects.projects._request") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"projects": []}
            mock_request.return_value = mock_response
            
            team_id = "team_123"
            get_projects(token=mock_token, team_id=team_id)
            
            # Verify the request was made with correct params
            call_args = mock_request.call_args
            assert call_args[1]["params"]["teamId"] == team_id
    
    @pytest.mark.asyncio
    async def test_team_id_parameter_async(self, mock_token):
        """Test that team_id parameter is passed correctly in async functions."""
        with patch("vercel.projects.projects._request_async") as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"projects": []}
            mock_request.return_value = mock_response
            
            team_id = "team_123"
            await get_projects_async(token=mock_token, team_id=team_id)
            
            # Verify the request was made with correct params
            call_args = mock_request.call_args
            assert call_args[1]["params"]["teamId"] == team_id


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
        with patch("vercel.projects.projects._request") as mock_sync_request, \
             patch("vercel.projects.projects._request_async") as mock_async_request:
            
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
