"""
Real API integration tests for Vercel Projects module.

These tests make actual API calls to Vercel and validate the real responses.
They require VERCEL_TOKEN and VERCEL_TEAM_ID environment variables.
"""

import os
import pytest
import time

# Import the actual functions (not mocked)
from vercel.projects import get_projects, create_project, update_project, delete_project
from vercel.projects.projects import get_projects_async, create_project_async, delete_project_async


@pytest.mark.skipif(
    not os.getenv("VERCEL_TOKEN") or not os.getenv("VERCEL_TEAM_ID"),
    reason="Requires VERCEL_TOKEN and VERCEL_TEAM_ID environment variables",
)
class TestProjectsRealAPI:
    """Test suite for Projects API using real Vercel API calls."""

    @pytest.fixture
    def team_id(self):
        """Get team ID from environment."""
        return os.getenv("VERCEL_TEAM_ID")

    @pytest.fixture
    def test_project_name(self):
        """Generate unique test project name."""
        timestamp = int(time.time())
        return f"vercel-py-test-{timestamp}"

    def test_get_projects_real_api(self, team_id):
        """Test get_projects with real API and validate actual response structure."""
        result = get_projects(team_id=team_id)

        # Validate response is a dict
        assert isinstance(result, dict), f"Expected dict, got {type(result)}"

        # Validate top-level structure
        assert "projects" in result, "Response missing 'projects' key"
        assert "pagination" in result, "Response missing 'pagination' key"

        # Validate projects array
        projects = result["projects"]
        assert isinstance(projects, list), f"Expected list, got {type(projects)}"

        # Validate first project structure if projects exist
        if len(projects) > 0:
            project = projects[0]
            assert isinstance(project, dict), f"Expected dict, got {type(project)}"

            # Validate core required fields exist
            required_fields = ["id", "name", "accountId", "createdAt", "updatedAt"]
            for field in required_fields:
                assert field in project, f"Missing required field: {field}"

            # Validate data types
            assert isinstance(project["id"], str), f"Expected string, got {type(project['id'])}"
            assert isinstance(project["name"], str), f"Expected string, got {type(project['name'])}"
            assert isinstance(project["accountId"], str), (
                f"Expected string, got {type(project['accountId'])}"
            )
            assert isinstance(project["createdAt"], int), (
                f"Expected int, got {type(project['createdAt'])}"
            )
            assert isinstance(project["updatedAt"], int), (
                f"Expected int, got {type(project['updatedAt'])}"
            )

            # Validate ID formats
            assert project["id"].startswith("prj_"), (
                f"Project ID should start with 'prj_', got: {project['id']}"
            )
            assert project["accountId"].startswith("team_"), (
                f"Account ID should start with 'team_', got: {project['accountId']}"
            )

            # Validate timestamps are reasonable (after 2020)
            assert project["createdAt"] > 1577836800000, (
                f"Created timestamp too old: {project['createdAt']}"
            )
            assert project["updatedAt"] > 1577836800000, (
                f"Updated timestamp too old: {project['updatedAt']}"
            )

        # Validate pagination structure
        pagination = result["pagination"]
        assert isinstance(pagination, dict), f"Expected dict, got {type(pagination)}"
        assert "count" in pagination, "Pagination missing 'count'"
        assert "next" in pagination, "Pagination missing 'next'"
        assert "prev" in pagination, "Pagination missing 'prev'"
        assert isinstance(pagination["count"], int), (
            f"Expected int, got {type(pagination['count'])}"
        )

        print(f"✅ Real API test passed: Found {len(projects)} projects")

    def test_create_project_real_api(self, team_id, test_project_name):
        """Test create_project with real API and validate actual response."""
        project_body = {"name": test_project_name, "framework": "nextjs"}

        result = create_project(body=project_body, team_id=team_id)

        # Validate response structure
        assert isinstance(result, dict), f"Expected dict, got {type(result)}"

        # Validate core fields
        assert "id" in result, "Response missing 'id'"
        assert "name" in result, "Response missing 'name'"
        assert "accountId" in result, "Response missing 'accountId'"
        assert "createdAt" in result, "Response missing 'createdAt'"
        assert "updatedAt" in result, "Response missing 'updatedAt'"

        # Validate data types
        assert isinstance(result["id"], str), f"Expected string, got {type(result['id'])}"
        assert isinstance(result["name"], str), f"Expected string, got {type(result['name'])}"
        assert isinstance(result["accountId"], str), (
            f"Expected string, got {type(result['accountId'])}"
        )
        assert isinstance(result["createdAt"], int), (
            f"Expected int, got {type(result['createdAt'])}"
        )
        assert isinstance(result["updatedAt"], int), (
            f"Expected int, got {type(result['updatedAt'])}"
        )

        # Validate values match what we sent
        assert result["name"] == test_project_name, (
            f"Expected {test_project_name}, got {result['name']}"
        )
        assert result["accountId"] == team_id, f"Expected {team_id}, got {result['accountId']}"

        # Validate ID format
        assert result["id"].startswith("prj_"), (
            f"Project ID should start with 'prj_', got: {result['id']}"
        )

        # Validate timestamps are recent (within last minute)
        current_time = int(time.time() * 1000)
        assert result["createdAt"] > current_time - 60000, (
            f"Created timestamp too old: {result['createdAt']}"
        )
        assert result["updatedAt"] > current_time - 60000, (
            f"Updated timestamp too old: {result['updatedAt']}"
        )

        print(f"✅ Real API test passed: Created project {result['name']} with ID {result['id']}")

        # Return project ID for cleanup
        return result["id"]

    def test_update_project_real_api(self, team_id, test_project_name):
        """Test update_project with real API and validate actual response."""
        # First create a project
        project_body = {"name": test_project_name, "framework": "nextjs"}
        created_project = create_project(body=project_body, team_id=team_id)
        project_id = created_project["id"]

        try:
            # Update the project
            update_body = {"framework": "svelte"}
            result = update_project(project_id, body=update_body, team_id=team_id)

            # Validate response structure
            assert isinstance(result, dict), f"Expected dict, got {type(result)}"

            # Validate core fields
            assert "id" in result, "Response missing 'id'"
            assert "name" in result, "Response missing 'name'"
            assert "accountId" in result, "Response missing 'accountId'"
            assert "updatedAt" in result, "Response missing 'updatedAt'"

            # Validate values
            assert result["id"] == project_id, f"Expected {project_id}, got {result['id']}"
            assert result["name"] == test_project_name, (
                f"Expected {test_project_name}, got {result['name']}"
            )
            assert result["accountId"] == team_id, f"Expected {team_id}, got {result['accountId']}"

            # Validate updatedAt is newer than createdAt
            assert result["updatedAt"] >= created_project["createdAt"], (
                "UpdatedAt should be >= createdAt"
            )

            print(f"✅ Real API test passed: Updated project {result['name']}")

        finally:
            # Clean up - delete the project
            delete_project(project_id, team_id=team_id)

    def test_delete_project_real_api(self, team_id, test_project_name):
        """Test delete_project with real API."""
        # First create a project
        project_body = {"name": test_project_name, "framework": "nextjs"}
        created_project = create_project(body=project_body, team_id=team_id)
        project_id = created_project["id"]

        # Delete the project
        delete_project(project_id, team_id=team_id)
        print(f"✅ Real API test passed: Deleted project {project_id}")

        # Verify project is actually deleted by checking it's not in the list
        try:
            projects_list = get_projects(team_id=team_id)
            project_ids = [p["id"] for p in projects_list["projects"]]
            assert project_id not in project_ids, (
                f"Project {project_id} still exists after deletion"
            )
            print(f"✅ Verified project {project_id} was deleted")
        except Exception as e:
            pytest.fail(f"Failed to verify project deletion: {e}")

    @pytest.mark.asyncio
    async def test_get_projects_async_real_api(self, team_id):
        """Test get_projects_async with real API and validate actual response."""
        result = await get_projects_async(team_id=team_id)

        # Same validation as sync version
        assert isinstance(result, dict), f"Expected dict, got {type(result)}"
        assert "projects" in result, "Response missing 'projects' key"
        assert "pagination" in result, "Response missing 'pagination' key"

        projects = result["projects"]
        assert isinstance(projects, list), f"Expected list, got {type(projects)}"

        # Validate first project structure if projects exist
        if len(projects) > 0:
            project = projects[0]
            assert isinstance(project, dict), f"Expected dict, got {type(project)}"

            # Validate core fields
            required_fields = ["id", "name", "accountId", "createdAt", "updatedAt"]
            for field in required_fields:
                assert field in project, f"Missing required field: {field}"

            # Validate data types
            assert isinstance(project["id"], str), f"Expected string, got {type(project['id'])}"
            assert isinstance(project["name"], str), f"Expected string, got {type(project['name'])}"
            assert isinstance(project["accountId"], str), (
                f"Expected string, got {type(project['accountId'])}"
            )
            assert isinstance(project["createdAt"], int), (
                f"Expected int, got {type(project['createdAt'])}"
            )
            assert isinstance(project["updatedAt"], int), (
                f"Expected int, got {type(project['updatedAt'])}"
            )

            # Validate ID formats
            assert project["id"].startswith("prj_"), (
                f"Project ID should start with 'prj_', got: {project['id']}"
            )
            assert project["accountId"].startswith("team_"), (
                f"Account ID should start with 'team_', got: {project['accountId']}"
            )

        print(f"✅ Real async API test passed: Found {len(projects)} projects")

    @pytest.mark.asyncio
    async def test_create_project_async_real_api(self, team_id, test_project_name):
        """Test create_project_async with real API and validate actual response."""
        project_body = {"name": f"{test_project_name}-async", "framework": "nextjs"}

        result = await create_project_async(body=project_body, team_id=team_id)

        # Same validation as sync version
        assert isinstance(result, dict), f"Expected dict, got {type(result)}"

        # Validate core fields
        assert "id" in result, "Response missing 'id'"
        assert "name" in result, "Response missing 'name'"
        assert "accountId" in result, "Response missing 'accountId'"
        assert "createdAt" in result, "Response missing 'createdAt'"
        assert "updatedAt" in result, "Response missing 'updatedAt'"

        # Validate data types
        assert isinstance(result["id"], str), f"Expected string, got {type(result['id'])}"
        assert isinstance(result["name"], str), f"Expected string, got {type(result['name'])}"
        assert isinstance(result["accountId"], str), (
            f"Expected string, got {type(result['accountId'])}"
        )
        assert isinstance(result["createdAt"], int), (
            f"Expected int, got {type(result['createdAt'])}"
        )
        assert isinstance(result["updatedAt"], int), (
            f"Expected int, got {type(result['updatedAt'])}"
        )

        # Validate values
        assert result["name"] == f"{test_project_name}-async", (
            f"Expected {test_project_name}-async, got {result['name']}"
        )
        assert result["accountId"] == team_id, f"Expected {team_id}, got {result['accountId']}"

        # Validate ID format
        assert result["id"].startswith("prj_"), (
            f"Project ID should start with 'prj_', got: {result['id']}"
        )

        print(
            f"✅ Real async API test passed: Created project {result['name']} with ID {result['id']}"
        )

        # Clean up - delete the project
        await delete_project_async(result["id"], team_id=team_id)

    def test_error_handling_real_api(self, team_id):
        """Test error handling with real API."""
        # Test with invalid project ID
        with pytest.raises(RuntimeError) as exc_info:
            delete_project("prj_invalid", team_id=team_id)

        error_message = str(exc_info.value)
        assert "Failed to delete project" in error_message, (
            f"Expected 'Failed to delete project' in: {error_message}"
        )

        print(f"✅ Real API error handling test passed: {error_message}")

    def test_full_crud_workflow_real_api(self, team_id, test_project_name):
        """Test complete CRUD workflow with real API."""
        project_body = {"name": test_project_name, "framework": "nextjs"}
        project_id = None

        try:
            # CREATE
            created = create_project(body=project_body, team_id=team_id)
            project_id = created["id"]
            print(f"✅ Created project: {created['name']} ({project_id})")

            # READ - verify project exists in list
            projects = get_projects(team_id=team_id)
            project_names = [p["name"] for p in projects["projects"]]
            assert test_project_name in project_names, (
                f"Project {test_project_name} not found in list"
            )
            print("✅ Verified project exists in list")

            # UPDATE
            update_body = {"framework": "svelte"}
            updated = update_project(project_id, body=update_body, team_id=team_id)
            assert updated["id"] == project_id, "Project ID changed after update"
            print("✅ Updated project framework")

            # DELETE
            delete_project(project_id, team_id=team_id)
            print("✅ Deleted project")

            # VERIFY DELETION - project should not be in list anymore
            projects_after_delete = get_projects(team_id=team_id)
            project_names_after = [p["name"] for p in projects_after_delete["projects"]]
            assert test_project_name not in project_names_after, (
                f"Project {test_project_name} still exists after deletion"
            )
            print("✅ Verified project was deleted")

            print("🎉 Full CRUD workflow test passed!")

        except Exception as e:
            # Clean up on error
            if project_id:
                try:
                    delete_project(project_id, team_id=team_id)
                except Exception:
                    pass
            raise e
