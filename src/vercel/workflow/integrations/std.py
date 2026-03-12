import asyncio
from http.server import BaseHTTPRequestHandler

from .. import runtime, world as w

workflow_entrypoint = runtime.workflow_entrypoint()
step_entrypoint = runtime.step_entrypoint()


class Request(w.HTTPRequest):
    body: bytes

    def __init__(self, http_handler: BaseHTTPRequestHandler) -> None:
        self._http_handler = http_handler

    def get_header(self, name: str) -> str | None:
        return self._http_handler.headers.get(name)

    async def get_body(self) -> bytes:
        if not hasattr(self, "body"):
            content_length = int(self._http_handler.headers.get("Content-Length", 0))
            self.body = self._http_handler.rfile.read(content_length)
        return self.body


class Handler:
    def __init__(self, http_handler: BaseHTTPRequestHandler) -> None:
        self._http_handler = http_handler

    def __call__(self) -> None:
        if self._http_handler.command.lower() == "post":
            if self._http_handler.path.startswith("/.well-known/workflow/v1/flow"):
                response = asyncio.run(workflow_entrypoint(Request(self._http_handler)))
            elif self._http_handler.path.startswith("/.well-known/workflow/v1/step"):
                response = asyncio.run(step_entrypoint(Request(self._http_handler)))
            else:
                response = w.HTTPResponse.json({"error": "Not found"}, status=404)
        else:
            response = w.HTTPResponse.json({"error": "Not found"}, status=404)

        self._http_handler.send_response(response.status)
        for key, value in response.headers.items():
            self._http_handler.send_header(key, value)
        self._http_handler.end_headers()
        self._http_handler.wfile.write(response.body)
        self._http_handler.wfile.flush()
