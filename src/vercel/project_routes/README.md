# Vercel Project Routes

Manage project-level routing rules through the Vercel API. All operations are
available through synchronous and asynchronous clients.

```python
from vercel.client import Vercel
from vercel.project_routes import RewriteRoute

vercel = Vercel(access_token="...")

staged = vercel.project_routes.add_route(
    project_id="prj_123",
    team_id="team_123",
    route=RewriteRoute(
        name="Rewrite /old to /new",
        source="/old",
        destination="/new",
        source_syntax="equals",
    ),
)

vercel.project_routes.update_route_versions(
    project_id="prj_123",
    team_id="team_123",
    version_id=staged["version"]["id"],
    action="promote",
)
```

The client also provides `get_routes`, `stage_routes`, `delete_routes`,
`edit_route`, `generate_route`, and `get_route_versions`. Use `AsyncVercel` for
the equivalent asynchronous API.
