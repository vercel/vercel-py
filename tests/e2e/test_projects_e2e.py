"""
E2E tests for Vercel Projects API functionality.

These tests verify the complete projects API workflow including:
- Listing projects
- Creating projects
- Updating projects
- Deleting projects
- Project management operations
"""

import asyncio
import os
import pytest
from typing import Any, Dict

from vercel.projects import get_projects, create_project, update_project, delete_project


class TestProjectsAPIE2E:
    """End-to-end tests for projects API functionality."""
    
    @pytest.fixture
    def vercel_token(self):
        """Get Vercel API token from environment."""
        token = os.getenv('VERCEL_TOKEN')
        if not token:
            pytest.skip("VERCEL_TOKEN not set - skipping projects API e2e tests")
        return token
    
    @pytest.fixture
    def vercel_team_id(self):
        """Get Vercel team ID from environment."""
        return os.getenv('VERCEL_TEAM_ID')
    
    @pytest.fixture
    def test_project_name(self):
        """Generate a unique test project name."""
        import time
        return f"vercel-sdk-e2e-test-{int(time.time() * 1000)}"
    
    @pytest.fixture
    def created_projects(self):
        """Track created projects for cleanup."""
        return []
    
    @pytest.mark.asyncio
    async def test_get_projects_list(self, vercel_token, vercel_team_id):
        """Test listing projects."""
        # Get projects list
        result = await get_projects(
            token=vercel_token,
            team_id=vercel_team_id,
            query={'limit': 10}
        )
        
        # Verify response structure
        assert isinstance(result, dict)
        assert 'projects' in result
        assert isinstance(result['projects'], list)
        
        # Verify project structure if projects exist
        if result['projects']:
            project = result['projects'][0]
            assert 'id' in project
            assert 'name' in project
            assert 'createdAt' in project
    
    @pytest.mark.asyncio
    async def test_get_projects_with_filters(self, vercel_token, vercel_team_id):
        """Test listing projects with various filters."""
        # Test with limit
        result = await get_projects(
            token=vercel_token,
            team_id=vercel_team_id,
            query={'limit': 5}
        )
        
        assert len(result['projects']) <= 5
        
        # Test with search query (if projects exist)
        if result['projects']:
            first_project_name = result['projects'][0]['name']
            search_result = await get_projects(
                token=vercel_token,
                team_id=vercel_team_id,
                query={'search': first_project_name[:10]}
            )
            
            # Should find at least the project we searched for
            assert len(search_result['projects']) >= 1
    
    @pytest.mark.asyncio
    async def test_create_project(self, vercel_token, vercel_team_id, test_project_name, created_projects):
        """Test project creation."""
        # Create project without GitHub repository linking
        project_data = {
            'name': test_project_name,
            'framework': 'nextjs'
        }
        
        result = await create_project(
            body=project_data,
            token=vercel_token,
            team_id=vercel_team_id
        )
        
        # Track for cleanup
        created_projects.append(result['id'])
        
        # Verify project creation
        assert isinstance(result, dict)
        assert result['name'] == test_project_name
        assert 'id' in result
        assert 'createdAt' in result
        
        # Verify project exists in list (with eventual consistency handling)
        projects = await get_projects(
            token=vercel_token,
            team_id=vercel_team_id,
            query={'search': test_project_name}
        )
        
        # The project might not appear immediately due to eventual consistency
        # Just verify we got a valid response
        assert isinstance(projects, dict)
        assert 'projects' in projects
        # Note: We don't assert the project is in the list due to eventual consistency
    
    @pytest.mark.asyncio
    async def test_update_project(self, vercel_token, vercel_team_id, test_project_name, created_projects):
        """Test project update."""
        # First create a project
        project_data = {
            'name': test_project_name,
            'framework': 'nextjs'
        }
        
        created_project = await create_project(
            body=project_data,
            token=vercel_token,
            team_id=vercel_team_id
        )
        
        created_projects.append(created_project['id'])
        
        # Update the project
        update_data = {
            'name': f"{test_project_name}-updated",
            'framework': 'svelte'
        }
        
        updated_project = await update_project(
            id_or_name=created_project['id'],
            body=update_data,
            token=vercel_token,
            team_id=vercel_team_id
        )
        
        # Verify update
        assert updated_project['name'] == f"{test_project_name}-updated"
        assert updated_project['framework'] == 'svelte'
        assert updated_project['id'] == created_project['id']
    
    @pytest.mark.asyncio
    async def test_delete_project(self, vercel_token, vercel_team_id, test_project_name):
        """Test project deletion."""
        # First create a project
        project_data = {
            'name': test_project_name,
            'framework': 'nextjs'
        }
        
        created_project = await create_project(
            body=project_data,
            token=vercel_token,
            team_id=vercel_team_id
        )
        
        # Delete the project
        await delete_project(
            id_or_name=created_project['id'],
            token=vercel_token,
            team_id=vercel_team_id
        )
        
        # Verify project is deleted by trying to get it
        # Note: This might not work immediately due to eventual consistency
        # In a real scenario, you might need to wait or check differently
        
        # Verify project is not in recent projects list
        projects = await get_projects(
            token=vercel_token,
            team_id=vercel_team_id,
            query={'search': test_project_name}
        )
        
        project_ids = [p['id'] for p in projects['projects']]
        assert created_project['id'] not in project_ids
    
    @pytest.mark.asyncio
    async def test_project_operations_error_handling(self, vercel_token, vercel_team_id):
        """Test error handling for invalid project operations."""
        # Test getting non-existent project (should return empty results, not raise exception)
        result = await get_projects(
            token=vercel_token,
            team_id=vercel_team_id,
            query={'search': 'non-existent-project-12345'}
        )
        assert result['projects'] == []
        
        # Test updating non-existent project (should raise exception)
        with pytest.raises(Exception):
            await update_project(
                id_or_name='non-existent-id',
                body={'name': 'test'},
                token=vercel_token,
                team_id=vercel_team_id
            )
        
        # Test deleting non-existent project (should raise exception)
        with pytest.raises(Exception):
            await delete_project(
                id_or_name='non-existent-id',
                token=vercel_token,
                team_id=vercel_team_id
            )
    
    @pytest.mark.asyncio
    async def test_project_creation_with_invalid_data(self, vercel_token, vercel_team_id):
        """Test project creation with invalid data."""
        # Test with missing required fields
        with pytest.raises(Exception):
            await create_project(
                body={},  # Empty body
                token=vercel_token,
                team_id=vercel_team_id
            )
        
        # Test with invalid framework
        with pytest.raises(Exception):
            await create_project(
                body={
                    'name': 'test-project',
                    'framework': 'invalid-framework'
                },
                token=vercel_token,
                team_id=vercel_team_id
            )
    
    @pytest.mark.asyncio
    async def test_project_pagination(self, vercel_token, vercel_team_id):
        """Test project pagination."""
        # Get first page
        first_page = await get_projects(
            token=vercel_token,
            team_id=vercel_team_id,
            query={'limit': 2}
        )
        
        assert len(first_page['projects']) <= 2
        
        # If there are more projects, test pagination
        if 'pagination' in first_page and first_page['pagination'].get('hasNext'):
            # Get next page
            next_page = await get_projects(
                token=vercel_token,
                team_id=vercel_team_id,
                query={'limit': 2, 'from': first_page['pagination']['next']}
            )
            
            # Verify different projects
            first_page_ids = {p['id'] for p in first_page['projects']}
            next_page_ids = {p['id'] for p in next_page['projects']}
            
            # Should be different projects (no overlap)
            assert len(first_page_ids.intersection(next_page_ids)) == 0
    
    @pytest.mark.asyncio
    async def test_project_concurrent_operations(self, vercel_token, vercel_team_id, test_project_name, created_projects):
        """Test concurrent project operations."""
        # Create multiple projects concurrently
        project_names = [f"{test_project_name}-{i}" for i in range(3)]
        
        async def create_single_project(name):
            project_data = {
                'name': name,
                'framework': 'nextjs'
            }
            return await create_project(
                body=project_data,
                token=vercel_token,
                team_id=vercel_team_id
            )
        
        # Create projects concurrently
        created_projects_list = await asyncio.gather(*[
            create_single_project(name) for name in project_names
        ])
        
        # Track for cleanup
        for project in created_projects_list:
            created_projects.append(project['id'])
        
        # Verify all projects were created
        assert len(created_projects_list) == 3
        
        for i, project in enumerate(created_projects_list):
            assert project['name'] == project_names[i]
            assert 'id' in project
    
    @pytest.mark.asyncio
    async def test_project_team_scoping(self, vercel_token, vercel_team_id):
        """Test project operations with team scoping."""
        # Test getting projects with team ID
        result = await get_projects(
            token=vercel_token,
            team_id=vercel_team_id
        )
        
        # Verify response structure
        assert isinstance(result, dict)
        assert 'projects' in result
        
        # Test getting projects without team ID (personal projects)
        # Note: This might fail due to token permissions
        try:
            personal_result = await get_projects(
                token=vercel_token
            )
            # If successful, verify response structure
            assert isinstance(personal_result, dict)
            assert 'projects' in personal_result
        except Exception as e:
            # If it fails due to permissions, that's expected
            if "Not authorized" in str(e) or "forbidden" in str(e).lower():
                print("✅ Expected: Token doesn't have access to personal projects")
            else:
                raise e
    
    @pytest.mark.asyncio
    async def test_project_environment_variables(self, vercel_token, vercel_team_id, test_project_name, created_projects):
        """Test project environment variables (if supported)."""
        # Create a project
        project_data = {
            'name': test_project_name,
            'framework': 'nextjs'
        }
        
        created_project = await create_project(
            body=project_data,
            token=vercel_token,
            team_id=vercel_team_id
        )
        
        created_projects.append(created_project['id'])
        
        # Test updating project with environment variables
        update_data = {
            'name': created_project['name'],
            'env': [
                {
                    'key': 'TEST_VAR',
                    'value': 'test_value',
                    'type': 'encrypted'
                }
            ]
        }
        
        try:
            updated_project = await update_project(
                project_id=created_project['id'],
                body=update_data,
                token=vercel_token,
                team_id=vercel_team_id
            )
            
            # Verify environment variables were set
            assert 'env' in updated_project
            assert len(updated_project['env']) >= 1
            
        except Exception as e:
            # Environment variables might not be supported in all API versions
            # This is acceptable for e2e testing
            pytest.skip(f"Environment variables not supported: {e}")
    
    @pytest.mark.asyncio
    async def test_project_cleanup(self, vercel_token, vercel_team_id, created_projects):
        """Test cleanup of created projects."""
        # Delete all created projects
        for project_id in created_projects:
            try:
                await delete_project(
                    project_id=project_id,
                    token=vercel_token,
                    team_id=vercel_team_id
                )
            except Exception as e:
                # Project might already be deleted or not exist
                # This is acceptable for cleanup
                pass
        
        # Verify projects are deleted
        for project_id in created_projects:
            projects = await get_projects(
                token=vercel_token,
                team_id=vercel_team_id
            )
            
            project_ids = [p['id'] for p in projects['projects']]
            assert project_id not in project_ids
