import fastapi

from vercel.headers import set_headers

from .. import runtime, world as w


class FastAPIRequestAdapter(w.HTTPRequest):
    def __init__(self, request: fastapi.Request):
        self._request = request

    def get_header(self, name: str) -> str | None:
        return self._request.headers.get(name)

    async def get_body(self) -> bytes:
        return await self._request.body()


def make_fastapi_response(response: w.HTTPResponse) -> fastapi.Response:
    return fastapi.Response(
        content=response.body,
        status_code=response.status,
        headers=response.headers,
    )


def with_workflow(app: fastapi.FastAPI) -> fastapi.FastAPI:
    @app.middleware("http")
    async def set_vercel_headers(request: fastapi.Request, call_next):
        set_headers(request.headers)
        return await call_next(request)

    router = fastapi.APIRouter(prefix="/.well-known/workflow/v1", tags=["Workflow"])

    workflow_entrypoint = runtime.workflow_entrypoint()

    @router.post("/flow")
    async def flow(request: fastapi.Request):
        response = await workflow_entrypoint(FastAPIRequestAdapter(request))
        return make_fastapi_response(response)

    step_entrypoint = runtime.step_entrypoint()

    @router.post("/step")
    async def step(request: fastapi.Request):
        response = await step_entrypoint(FastAPIRequestAdapter(request))
        return make_fastapi_response(response)

    app.include_router(router)
    return app
