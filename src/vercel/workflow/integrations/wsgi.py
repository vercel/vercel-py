from multiprocessing.process import name
from typing import Any, Callable, Iterable, TypeAlias, Protocol
from types import TracebackType

from .. import runtime, world as w

_ExcInfo: TypeAlias = tuple[type[BaseException], BaseException, TracebackType]
_OptExcInfo: TypeAlias = _ExcInfo | tuple[None, None, None]

class StartResponse(Protocol):
    """start_response() callable as defined in PEP 3333"""
    def __call__(
            self,
            status: str,
            headers: list[tuple[str, str]],
            exc_info: _OptExcInfo | None = ...,
            /,
    ) -> Callable[[bytes], object]: ...

WSGIEnvironment: TypeAlias = dict[str, Any]
WSGIApplication: TypeAlias = Callable[[WSGIEnvironment, StartResponse], Iterable[bytes]]

workflow_entrypoint = runtime.workflow_entrypoint()
step_entrypoint = runtime.step_entrypoint()

class WSGIRequest(w.HTTPRequest):
    body: bytes

    def __init__(self, env: WSGIEnvironment) -> None:
        self.env = env

    def get_header(self, name: str) -> str | None:
        key = "HTTP_" + name.upper().replace("-", "_")
        if key == "HTTP_CONTENT_TYPE":
            return self.env["CONTENT_TYPE"]
        elif key == "HTTP_CONTENT_LENGTH":
            return self.env["CONTENT_LENGTH"]
        else:
            return self.env.get(key)

    def get_body(self) -> bytes:
        if not hasattr(self, "body"):
            self.body = self.env["wsgi.input"].read()
        return self.body


def entrypoint(env: WSGIEnvironment, start_response: StartResponse) -> Iterable[bytes]:
    if env["REQUEST_METHOD"].lower() == "post":
        if env["PATH_INFO"] == "/.well-known/workflow/v1/flow":
            response = workflow_entrypoint(WSGIRequest(env))
        elif env["PATH_INFO"] == "/.well-known/workflow/v1/step":
            response = step_entrypoint(WSGIRequest(env))
        else:
            response = w.HTTPResponse.json({"error": "Not found"}, status=404)
    else:
        response = w.HTTPResponse.json({"error": "Not found"}, status=404)

    start_response(str(response.status), list(response.headers.items()), None)
    yield response.body
