"""Backward compatibility shim for vercel.deployments.deployments imports."""

# Re-export sync functions
from . import create_deployment, upload_file

# Re-export async functions with their original names
from .aio import (
    create_deployment as create_deployment_async,
    upload_file as upload_file_async,
)

__all__ = [
    "create_deployment",
    "upload_file",
    "create_deployment_async",
    "upload_file_async",
]
