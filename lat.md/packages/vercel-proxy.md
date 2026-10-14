# vercel-proxy

Python routing middleware that runs in front of an app and decides whether each request continues, is rewritten or redirected, or is answered directly.

Lives in [[src/vercel-proxy]] and is exposed as [[src/vercel-proxy/vercel/proxy/__init__.py|vercel.proxy]]. It depends only on Starlette, and it is the only service package without sync and async variants: handlers may be either.

## Deployment model

The proxy runs as a separate entrypoint from the app, configured in `vercel.json`, with its own minimal dependency set.

The project points `proxy.entrypoint` in `vercel.json` at a `module:attribute`, optionally with path matchers. The proxy gets only the packages listed in the `vercel.proxy` dependency group of `pyproject.toml`. Proxy code therefore cannot assume the app's dependencies are importable.

## Routing

A `Proxy` is an ASGI app that sends each request to the first matching route, in registration order, falling back when nothing matches.

[[src/vercel-proxy/vercel/proxy/proxy.py#Proxy]] matches paths with typed placeholders (`str`, `int`, `float`, `uuid`, `path`), and can also match on HTTP methods and host patterns. Matching first tries the path exactly as sent, then with the trailing slash toggled, unless `strict=True`. The fallback continues unmatched requests unchanged by default, or can be a fixed response or a handler.

Sync handlers run in a worker thread. WebSocket upgrades are routed like bodiless `GET` requests and answered as plain HTTP through the ASGI WebSocket denial-response extension, which the server must support; the destination accepts the socket. Captured path values are decoded, so handlers must quote them before building URLs.

## Responses

Response types map onto the platform's middleware protocol, which forwards or answers the request based on reserved headers.

[[src/vercel-proxy/vercel/proxy/response.py#ContinueResponse]] lets the request through, optionally replacing or removing forwarded request headers. [[src/vercel-proxy/vercel/proxy/response.py#RewriteResponse]] serves it from another URL without changing what the client sees. [[src/vercel-proxy/vercel/proxy/response.py#RedirectResponse]], [[src/vercel-proxy/vercel/proxy/response.py#JSONResponse]], and the other Starlette-style responses answer the request directly, so it never reaches the app.

These responses are encoded as `x-middleware-*` headers, so user code may not set headers with that prefix. Header names are validated, and background tasks are unsupported.
