# Internal Core

`vercel.internal.core` provides the shared runtime used by Vercel Python service
packages. It is installed as their dependency and is not intended for direct
installation or direct end-user imports.

The distribution owns the lightweight root `vercel` package, including the
public `vercel.session(...)` context and shared Vercel error hierarchy.
