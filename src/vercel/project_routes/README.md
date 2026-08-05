# Vercel Project Routes

Manage project-level routing rules through the Vercel API. All operations are
available through synchronous and asynchronous clients.

Changes always stage first. Publish them by promoting the staged version.

## Adding routes

Author routes with `RewriteRoute`, `RedirectRoute`, or `SetStatusRoute`:

```python
from vercel.client import Vercel
from vercel.project_routes import RedirectRoute, RewriteRoute, SetStatusRoute

vercel = Vercel(access_token="...")

added = vercel.project_routes.add_route(
    project_id="prj_123",
    route=RewriteRoute(
        name="Rewrite /old to /new",
        source="/old",
        destination="/new",
        source_syntax="equals",
    ),
)

vercel.project_routes.add_route(
    project_id="prj_123",
    route=RedirectRoute(name="Old blog", source="/blog/(.*)", destination="/posts/$1", status=301),
)

vercel.project_routes.add_route(
    project_id="prj_123",
    route=SetStatusRoute(name="Retire legacy API", source="/api/v1/.*", status=410),
)
```

`placement="start"`, `"end"`, `"before"`, or `"after"` positions the route;
the last two take a `reference_id`. Anything the dataclasses do not cover
(conditions, transforms, headers) uses a raw `RouteInput` mapping, in the same
shape as `routes` in `vercel.json`:

```python
vercel.project_routes.add_route(
    project_id="prj_123",
    route={
        "name": "Beta users only",
        "srcSyntax": "path-to-regexp",
        "route": {
            "src": "/beta/:path*",
            "dest": "/app/:path*",
            "has": [{"type": "cookie", "key": "beta", "value": "1"}],
        },
    },
)
```

## Inspecting and editing

Results are typed dataclasses; the routing rule itself stays a mapping:

```python
result = vercel.project_routes.get_routes(project_id="prj_123")
for route in result.routes:
    print(route.name, route.route_type, route.route["src"], route.staged)

rewrites = vercel.project_routes.get_routes(
    project_id="prj_123", search="legacy", route_type="rewrite"
)

edited = vercel.project_routes.edit_route(
    project_id="prj_123",
    route_id=added.route.id,
    route=RewriteRoute(name="Rewrite /old to /new", source="/old", destination="/v2/new"),
)
vercel.project_routes.edit_route(
    project_id="prj_123", route_id=added.route.id, restore=True
)

deleted = vercel.project_routes.delete_routes(
    project_id="prj_123", route_ids=[added.route.id]
)
```

Fetched routes round-trip back into bulk staging:

```python
keep = [r for r in result.routes if r.enabled]
version = vercel.project_routes.stage_routes(
    project_id="prj_123", routes=keep, overwrite=True
)
```

## Publishing

Preview staged changes with `diff`, then promote, discard, or roll back:

```python
pending = vercel.project_routes.get_routes(project_id="prj_123", diff="only")

versions = vercel.project_routes.get_route_versions(project_id="prj_123")
vercel.project_routes.update_route_version(
    project_id="prj_123", version_id=versions[0].id, action="promote"
)
```

## Generating routes

`generate_route` turns natural language into a route suggestion. Feed the
result back as `current_route` to refine it:

```python
suggestion = vercel.project_routes.generate_route(
    project_id="prj_123",
    prompt="Redirect /docs to docs.example.com, preserving the path",
)
refined = vercel.project_routes.generate_route(
    project_id="prj_123",
    prompt="Only when the beta cookie is not set",
    current_route=suggestion,
)
```

## Errors

Failed requests raise `ProjectRoutesError` with `status_code`, the API error
`code` (for example `routes_limit_exceeded`), and the parsed `response_body`.

Use `AsyncVercel` for the equivalent asynchronous API, or the module-level
functions in `vercel.project_routes.aio`.
