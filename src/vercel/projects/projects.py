"""Backward compatibility shim for vercel.projects.projects imports."""

# Re-export sync functions
from . import create_project, delete_project, get_projects, update_project

# Re-export async functions with their original names
from .aio import (
    create_project as create_project_async,
    delete_project as delete_project_async,
    get_projects as get_projects_async,
    update_project as update_project_async,
)

__all__ = [
    "get_projects",
    "create_project",
    "update_project",
    "delete_project",
    "get_projects_async",
    "create_project_async",
    "update_project_async",
    "delete_project_async",
]
