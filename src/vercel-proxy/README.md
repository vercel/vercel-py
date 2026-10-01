# vercel-proxy

`vercel.proxy` lets you write Vercel middleware in Python. It runs before each
request reaches your app and decides whether to let it continue, rewrite it,
redirect it or answer it directly.

```python
from vercel.proxy import (
    ContinueResponse,
    Proxy,
    RedirectResponse,
    Request,
    Response,
    RewriteResponse,
)

proxy = Proxy()


@proxy.route("/old-blog/{slug}")
def old_blog(request: Request) -> Response:
    return RedirectResponse(f"/blog/{request.path_params['slug']}", status_code=308)


@proxy.route("/dashboard/{path:path}")
def dashboard(request: Request) -> Response:
    if "session" not in request.cookies:
        return RedirectResponse("/login")
    return ContinueResponse(request_headers={"x-user": request.cookies["session"]})


@proxy.route("/api/{path:path}", host="{tenant}.example.com")
async def tenant_api(request: Request) -> Response:
    tenant = request.path_params["tenant"]
    return RewriteResponse(f"https://{tenant}.api.example.com/{request.path_params['path']}")
```

## Routes

`@proxy.route(path)` registers a handler for requests whose path matches. Each
request goes to the first matching route, in the order the routes were
registered.

A path can capture parts of the URL with placeholders. The captured values are
passed to the handler in `request.path_params`.

```python
@proxy.route("/users/{user_id:int}")
def user(request: Request) -> Response:
    return RewriteResponse(f"/profiles/{request.path_params['user_id']}")
```

The following placeholders are available:

- `{name}` captures one path segment as a string.
- `{name:type}` also converts the value. The type can be `str`, `int`, `float`
  or `uuid`.
- `{name:path}` captures any part of the path, including `/`.

Trailing slashes are optional by default. Pass `strict=True` to `Proxy()` to
require exact matches.

A route can also be limited to certain methods or hosts.

```python
@proxy.route("/admin", methods=["GET", "POST"], host="admin.example.com")
def admin(request: Request) -> Response:
    return RewriteResponse("/internal/admin")
```

- `methods` lists the HTTP methods the route accepts. Omit it to accept every
  method.
- `host` is the hostname the route accepts. It can capture values like the
  path. Omit it to accept every host.

Handlers can be sync or async. Sync handlers run in a worker thread.

## Fallback

Requests that match no route go to the fallback. By default they continue
unchanged.

Pass a fallback to the constructor to change it. It can be a response, which
is returned for every unmatched request, or a handler.

```python
proxy = Proxy(fallback=JSONResponse({"error": "not found"}, status_code=404))
```

The fallback can also be set with the `@proxy.fallback` decorator. It replaces
any fallback passed to the constructor.

```python
@proxy.fallback
def fallback(request: Request) -> Response:
    return RewriteResponse("/404")
```

## Requests

`Request` is a Starlette [`Request`](https://starlette.dev/requests/). Use `method`, `url`, `headers`,
`query_params`, `cookies`, `path_params` and `client` to decide how to route
it.

WebSocket upgrades are routed like `GET` requests, and the destination accepts
the socket. Check `request.headers.get("upgrade")` to tell them apart.

## Responses

A handler returns a `ContinueResponse` or `RewriteResponse` to let the request
continue, or another response to answer it directly.

`ContinueResponse()` continues to the original URL. `RewriteResponse(url)`
serves the request from `url` without changing the URL the client sees.

For both, `request_headers` is a dictionary of headers to replace on the
forwarded request. A `None` value removes the header.

```python
return ContinueResponse(request_headers={"x-region": "eu", "cookie": None})
```

`Response`, `JSONResponse`, `PlainTextResponse`, `HTMLResponse` and
`RedirectResponse` answer the request directly. They are the Starlette
[responses](https://starlette.dev/responses/) of the same name.

Every response has `headers`, `set_cookie` and `delete_cookie`, which work like
Starlette's.

```python
response = ContinueResponse()
response.set_cookie("bucket", "b", max_age=86400)
return response
```
