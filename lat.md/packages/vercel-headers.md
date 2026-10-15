# vercel-headers

Context-local store for the current request's Vercel headers, plus IP and geolocation helpers that read them.

Lives in [[src/vercel-headers]] and is exposed as [[src/vercel-headers/vercel/headers/__init__.py|vercel.headers]]. It has no dependencies, so every other package can depend on it.

## Header store

One context variable holds the request headers, so a framework registers them once and every helper sees them.

[[src/vercel-headers/vercel/headers/__init__.py#set_headers]] and [[src/vercel-headers/vercel/headers/__init__.py#get_headers]] read and write the store. Because it is a `ContextVar`, concurrent requests on different tasks or threads never see each other's headers. [[vercel-oidc]] reads `x-vercel-oidc-token` from it, and [[vercel-cache]] exposes the same store through its own context helpers.

[[src/vercel-headers/vercel/headers/__init__.py#headers_from_asgi_scope]] and [[src/vercel-headers/vercel/headers/__init__.py#headers_from_wsgi_environ]] convert framework-native requests into the mapping the store expects.

## Geolocation and IP

Request metadata helpers decode the headers Vercel's edge adds, matching the TypeScript SDK's names and behavior.

[[src/vercel-headers/vercel/headers/__init__.py#geolocation]] returns city, country, flag emoji, region, coordinates, and postal code. The city is URL-decoded, and the Vercel region is derived from the `x-vercel-id` request ID. [[src/vercel-headers/vercel/headers/__init__.py#ip_address]] reads `x-real-ip`. Both accept a request object or a plain header mapping.
