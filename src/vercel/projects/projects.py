# Re-exported for backwards compatibility: these symbols were previously defined
# directly in this module and may be imported by name by downstream consumers.
from ._async.projects import (  # noqa: F401
    _request as _request_async,
    create_project as create_project_async,
    delete_project as delete_project_async,
    get_projects as get_projects_async,
    update_project as update_project_async,
)
from ._sync.projects import (  # noqa: F401
    DEFAULT_API_BASE_URL,
    DEFAULT_TIMEOUT,
    _request,
    _require_token,
    create_project,
    delete_project,
    get_projects,
    update_project,
)

__all__ = [
    "get_projects",
    "create_project",
    "update_project",
    "delete_project",
]
