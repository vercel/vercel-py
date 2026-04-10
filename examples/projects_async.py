#!/usr/bin/env python3
"""
Async example demonstrating Vercel Projects API usage.

This example shows how to use the async versions of the projects API functions.
It performs a full CRUD cycle: list projects, create a project, update it, and delete it.

Requirements:
- VERCEL_TOKEN environment variable set
- Optional: VERCEL_TEAM_ID for team-scoped operations

Usage:
    python examples/projects_async.py
"""

import asyncio
import os
import uuid

from dotenv import load_dotenv

from vercel.projects.aio import create_project, delete_project, get_projects, update_project

load_dotenv()


async def main() -> None:
    """Demonstrate async projects API usage."""
    print("🚀 Vercel Projects API - Async Example")
    print("=" * 50)

    # Check if we have a token
    token = os.getenv("VERCEL_TOKEN")
    if not token:
        print("❌ Error: VERCEL_TOKEN environment variable is required")
        print("   Set it with: export VERCEL_TOKEN=your_token_here")
        return

    team_id = os.getenv("VERCEL_TEAM_ID")
    if team_id:
        print(f"📋 Using team ID: {team_id}")
    else:
        print("📋 Using personal account")

    project_id = None
    project_name = None

    try:
        # 1. List existing projects
        print("\n1️⃣ Listing existing projects...")
        projects_response = await get_projects(team_id=team_id)
        projects = projects_response.get("projects", [])
        print(f"   Found {len(projects)} existing projects")

        if projects:
            print("   Recent projects:")
            for project in projects[:3]:  # Show first 3
                print(f"   - {project.get('name', 'Unknown')} ({project.get('id', 'No ID')})")

        # 2. Create a new test project
        print("\n2️⃣ Creating a new test project...")
        test_project_name = f"vercel-py-async-test-{str(uuid.uuid4())}"

        create_response = await create_project(
            body={
                "name": test_project_name,
                "framework": "nextjs",
                "publicSource": False,
            },
            team_id=team_id,
        )

        project_id = create_response.get("id")
        project_name = create_response.get("name")
        print(f"   ✅ Created project: {project_name} (ID: {project_id})")

        # 3. Update the project
        print("\n3️⃣ Updating the project...")
        update_response = await update_project(
            project_id,
            body={
                "framework": "nextjs",
                "buildCommand": "npm run build",
                "outputDirectory": ".next",
                "installCommand": "npm install",
            },
            team_id=team_id,
        )

        updated_framework = update_response.get("framework")
        print(f"   ✅ Updated project framework to: {updated_framework}")

        # 4. Get projects again to verify our project is there
        print("\n4️⃣ Verifying project appears in list...")
        projects_response = await get_projects(team_id=team_id)
        projects = projects_response.get("projects", [])

        our_project = next((p for p in projects if p.get("id") == project_id), None)
        if our_project:
            print(f"   ✅ Found our project: {our_project.get('name')}")
        else:
            print("   ❌ Could not find our project in the list")

        print("\n🎉 All operations completed successfully!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("   Make sure your VERCEL_TOKEN is valid and has the necessary permissions")

    finally:
        # 5. Clean up - delete the test project if it was created
        if project_id:
            print("\n5️⃣ Cleaning up - deleting test project...")
            try:
                await delete_project(project_id, team_id=team_id)
                print(f"   ✅ Deleted project: {project_name}")
            except Exception as cleanup_error:
                print(f"   ⚠️  Failed to delete project: {cleanup_error}")


if __name__ == "__main__":
    asyncio.run(main())
