#!/usr/bin/env python3
"""
Comparison example showing both sync and async Vercel Projects API usage.

This example demonstrates the differences between sync and async approaches
by performing the same operations with both methods and comparing performance.

Requirements:
- VERCEL_TOKEN environment variable set
- Optional: VERCEL_TEAM_ID for team-scoped operations

Usage:
    python examples/projects_comparison.py
"""

import asyncio
import os
import time
from datetime import datetime
from dotenv import load_dotenv

from vercel.projects import get_projects, create_project, update_project, delete_project
from vercel.projects.aio import get_projects as get_projects_async, create_project as create_project_async, update_project as update_project_async, delete_project as delete_project_async

load_dotenv()


async def async_operations(team_id: str | None = None) -> tuple[str, float]:
    """Perform async operations and return project ID and duration."""
    start_time = time.time()
    project_id = None

    try:
        # Create project
        test_project_name = f"vercel-py-async-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        create_response = await create_project_async(
            body={
                "name": test_project_name,
                "framework": "nextjs",
                "publicSource": False,
            },
            team_id=team_id,
        )
        project_id = create_response.get("id")

        # Update project
        await update_project_async(
            project_id,
            body={
                "framework": "nextjs",
                "buildCommand": "npm run build",
            },
            team_id=team_id,
        )

        # List projects
        await get_projects_async(team_id=team_id)
    finally:
        # Delete project if it was created
        if project_id:
            try:
                await delete_project_async(project_id, team_id=team_id)
            except Exception:
                # Silently ignore cleanup errors
                pass

    duration = time.time() - start_time
    return project_id, duration


def sync_operations(team_id: str | None = None) -> tuple[str, float]:
    """Perform sync operations and return project ID and duration."""
    start_time = time.time()
    project_id = None

    try:
        # Create project
        test_project_name = f"vercel-py-sync-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        create_response = create_project(
            body={
                "name": test_project_name,
                "framework": "nextjs",
                "publicSource": False,
            },
            team_id=team_id,
        )
        project_id = create_response.get("id")

        # Update project
        update_project(
            project_id,
            body={
                "framework": "nextjs",
                "buildCommand": "npm run build",
            },
            team_id=team_id,
        )

        # List projects
        get_projects(team_id=team_id)
    finally:
        # Delete project if it was created
        if project_id:
            try:
                delete_project(project_id, team_id=team_id)
            except Exception:
                # Silently ignore cleanup errors
                pass

    duration = time.time() - start_time
    return project_id, duration


async def main() -> None:
    """Compare sync vs async performance."""
    print("🚀 Vercel Projects API - Sync vs Async Comparison")
    print("=" * 60)

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

    try:
        print("\n🔄 Running async operations...")
        async_project_id, async_duration = await async_operations(team_id)
        print(f"   ✅ Async operations completed in {async_duration:.2f} seconds")

        print("\n🔄 Running sync operations...")
        sync_project_id, sync_duration = sync_operations(team_id)
        print(f"   ✅ Sync operations completed in {sync_duration:.2f} seconds")

        print("\n📊 Performance Comparison:")
        print(f"   Async duration: {async_duration:.2f}s")
        print(f"   Sync duration:  {sync_duration:.2f}s")

        if async_duration < sync_duration:
            improvement = ((sync_duration - async_duration) / sync_duration) * 100
            print(f"   🚀 Async is {improvement:.1f}% faster!")
        elif sync_duration < async_duration:
            improvement = ((async_duration - sync_duration) / async_duration) * 100
            print(f"   🐌 Sync is {improvement:.1f}% faster")
        else:
            print("   ⚖️  Both approaches performed similarly")

        print("\n💡 Notes:")
        print("   - Async operations can run concurrently when possible")
        print("   - Sync operations are simpler but block the thread")
        print("   - Performance differences depend on network latency and API response times")
        print("   - For single operations, the difference may be minimal")
        print("   - Async shines when making multiple concurrent requests")

        print("\n🎉 Comparison completed successfully!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("   Make sure your VERCEL_TOKEN is valid and has the necessary permissions")
        return


if __name__ == "__main__":
    asyncio.run(main())
