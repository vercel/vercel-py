"""
Example usage of the Vercel Deployments API.

This example demonstrates all the deployment management functions:
- get_deployment: Get a single deployment by ID or URL
- get_deployments: List deployments with filtering
- delete_deployment: Delete a deployment
- cancel_deployment: Cancel a building deployment
- list_deployment_files: List files in a deployment
"""

import os
from vercel.deployments import (
    get_deployment,
    get_deployments,
    delete_deployment,
    cancel_deployment,
    list_deployment_files,
)

# Set your Vercel API token
# Either set VERCEL_TOKEN environment variable or pass token parameter
VERCEL_TOKEN = os.getenv("VERCEL_TOKEN")


def example_get_deployment():
    """Get a single deployment by ID or URL."""
    deployment_id = "dpl_ABC123"  # Replace with your deployment ID

    result = get_deployment(
        id_or_url=deployment_id,
        token=VERCEL_TOKEN,
        with_git_repo_info=True,  # Include git information
    )

    print(f"Deployment: {result.get('name')}")
    print(f"Status: {result.get('readyState')}")
    print(f"URL: {result.get('url')}")
    return result


def example_list_deployments():
    """List deployments with filtering."""
    result = get_deployments(
        token=VERCEL_TOKEN,
        limit=10,  # Get up to 10 deployments
        state="READY",  # Filter by state: BUILDING, ERROR, READY, QUEUED, CANCELED
        target="production",  # Filter by target: production, staging, preview
    )

    deployments = result.get("deployments", [])
    print(f"Found {len(deployments)} deployments")

    for deployment in deployments:
        print(
            f"- {deployment.get('uid')}: {deployment.get('name')} ({deployment.get('readyState')})"
        )

    return result


def example_list_deployments_by_project():
    """List deployments for a specific project."""
    project_id = "prj_ABC123"  # Replace with your project ID

    result = get_deployments(
        token=VERCEL_TOKEN,
        project_id=project_id,
        limit=20,
    )

    deployments = result.get("deployments", [])
    print(f"Found {len(deployments)} deployments for project {project_id}")

    return result


def example_list_deployments_by_branch():
    """List deployments filtered by Git branch."""
    result = get_deployments(
        token=VERCEL_TOKEN,
        branch="main",  # Filter by Git branch
        limit=10,
    )

    deployments = result.get("deployments", [])
    print(f"Found {len(deployments)} deployments from main branch")

    return result


def example_list_deployment_files():
    """List files in a deployment."""
    deployment_id = "dpl_ABC123"  # Replace with your deployment ID

    files = list_deployment_files(
        id=deployment_id,
        token=VERCEL_TOKEN,
    )

    print(f"Found {len(files)} files in deployment")

    for file in files[:5]:  # Show first 5 files
        print(f"- {file.get('name')} ({file.get('type')})")

    return files


def example_cancel_deployment():
    """Cancel a deployment that is currently building."""
    deployment_id = "dpl_ABC123"  # Replace with your deployment ID

    result = cancel_deployment(
        id=deployment_id,
        token=VERCEL_TOKEN,
    )

    print(f"Canceled deployment: {result.get('uid')}")
    print(f"State: {result.get('state')}")

    return result


def example_delete_deployment():
    """Delete a deployment."""
    deployment_id = "dpl_ABC123"  # Replace with your deployment ID

    result = delete_deployment(
        id=deployment_id,
        token=VERCEL_TOKEN,
    )

    print(f"Deleted deployment: {result.get('uid')}")
    print(f"State: {result.get('state')}")

    return result


def example_delete_deployment_by_url():
    """Delete a deployment by URL instead of ID."""
    deployment_url = "my-app-abc123.vercel.app"  # Replace with deployment URL

    # When deleting by URL, you still need to provide an ID (can be placeholder)
    # and pass the URL as a query parameter
    result = delete_deployment(
        id="placeholder",  # Will be ignored when url is provided
        url=deployment_url,
        token=VERCEL_TOKEN,
    )

    print(f"Deleted deployment by URL: {deployment_url}")

    return result


# Async examples
async def async_examples():
    """Async versions of the deployment functions."""
    from vercel.deployments.aio import (
        get_deployment,
        get_deployments,
        delete_deployment,
        cancel_deployment,
        list_deployment_files,
    )

    # Get a single deployment (async)
    deployment = await get_deployment(
        id_or_url="dpl_ABC123",
        token=VERCEL_TOKEN,
    )
    print(f"Async: Got deployment {deployment.get('uid')}")

    # List deployments (async)
    result = await get_deployments(
        token=VERCEL_TOKEN,
        limit=5,
    )
    print(f"Async: Found {len(result.get('deployments', []))} deployments")

    # List deployment files (async)
    files = await list_deployment_files(
        id="dpl_ABC123",
        token=VERCEL_TOKEN,
    )
    print(f"Async: Found {len(files)} files")


def main():
    """Run examples (uncomment the ones you want to try)."""
    print("Vercel Deployments API Examples\n")

    # List deployments
    # example_list_deployments()

    # Get a specific deployment
    # example_get_deployment()

    # List deployments by project
    # example_list_deployments_by_project()

    # List deployments by branch
    # example_list_deployments_by_branch()

    # List files in a deployment
    # example_list_deployment_files()

    # Cancel a building deployment
    # example_cancel_deployment()

    # Delete a deployment
    # example_delete_deployment()

    # Delete a deployment by URL
    # example_delete_deployment_by_url()

    print("\nUncomment the examples you want to run!")


if __name__ == "__main__":
    main()

    # To run async examples:
    # import asyncio
    # asyncio.run(async_examples())
