# Proxy

`vercel.proxy` provides Python routing middleware that runs before Vercel's
cache and application routing.

## Configuration

Point Vercel at an exported `Proxy` object:

`vercel.json`
```json
{
    "proxy": "proxy.py"
}
```

The proxy function has no dependencies installed by default. To add dependencies
to your proxy, add a `proxy` dependency group in `pyproject.toml`

```toml
[dependency-groups]
proxy = ["vercel-proxy"]
```


`proxy.py`:

```python
from vercel.proxy import Proxy, Request, redirect, rewrite

proxy = Proxy()


@proxy.middleware("http")
async def authenticate(request: Request, call_next):
    if request.cookies.get("session") is None:
        return redirect("/login")

    if request.url.path == "/about":
        return rewrite("/about-2")

    response = await call_next(request)
    response.headers["x-authenticated"] = "true"
    return response
```

`call_next()` advances through the remaining Python proxy middleware and
routes. It returns a synthetic routing response; it does not invoke, await, or
contain the eventual CDN or application response.

Returning `None` continues Vercel routing unchanged. Use
`continue_routing()` when the continuation needs response headers or a
complete replacement set of request headers:

```python
from vercel.proxy import continue_routing

return continue_routing(
    headers={"x-authenticated": "true"},
    request_headers={
        **request.headers,
        "x-user-id": "user_123",
    },
)
```

`request_headers` is the complete set forwarded after the proxy, not a patch.

## Route selected logic

Route paths use template syntax where `{name}` captures a single path segment
and `{name:path}` captures the remainder of the path. The first route whose
path, method, and conditions all match is selected. With no `methods`
argument, a route matches every HTTP method.

```python
from vercel.proxy import Proxy, Request, Route, redirect, rewrite
from vercel.proxy.matchers import cookie, header, host, query


async def dashboard(request: Request):
    if request.cookies.get("session") is None:
        return redirect("/login")
    return None


proxy = Proxy(
    routes=[
        Route(
            "/dashboard/{path:path}",
            dashboard,
            has=[host("{tenant}.example.com"), cookie("session")],
            missing=[header("x-blocked")],
        ),
        Route.rewrite(
            "/legacy/{path:path}",
            "/new/{path}",
            has=[query("migrate", "1")],
        ),
        Route.redirect("/docs", "/documentation", status_code=308),
    ]
)
```

`header()`, `cookie()`, and `query()` match presence when no value is supplied
and use exact value matching otherwise. `host()` supports path-style captures;
captured hostname values are added to `request.path_params`.

For a rewrite that needs arbitrary Python logic, use a normal route handler:

```python
Route(
    "/legacy/{path:path}",
    lambda request: rewrite(f"/new/{request.path_params['path']}"),
)
```

Purely static rewrites are generally more efficient in Vercel routing
configuration because they do not need to start Python.
