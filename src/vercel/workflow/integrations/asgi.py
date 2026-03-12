from typing import Any, Callable, Awaitable

from .. import runtime, world as w

type Scope = dict[str, Any]
type Receive = Callable[[], Awaitable[dict[str, Any]]]
type Send = Callable[[dict[str, Any]], Awaitable[None]]

workflow_entrypoint = runtime.workflow_entrypoint()
step_entrypoint = runtime.step_entrypoint()


class ASGIRequest(w.HTTPRequest):
    body: bytes

    def __init__(self, scope: Scope, receive: Receive):
        self.scope = scope
        self.receive = receive

    def get_header(self, name: str) -> str | None:
        get_key = name.lower().encode("latin-1")
        for key, value in self.scope["headers"]:
            if isinstance(key, str):
                key = key.lower().encode("latin-1")
            if key == get_key:
                if isinstance(value, (list, tuple)):
                    single_value = value[0]
                else:
                    single_value = value
                if isinstance(single_value, bytes):
                    return value.decode("latin-1")
                else:
                    return single_value
        return None

    async def get_body(self) -> bytes:
        if not hasattr(self, "body"):
            chunks: list[bytes] = []
            while True:
                message = await self.receive()
                if message["type"] == "http.request":
                    body = message.get("body", b"")
                    if body:
                        chunks.append(body)
                    if not message.get("more_body", False):
                        break
                elif message["type"] == "http.disconnect":  # pragma: no branch
                    raise EOFError()
            self.body = b"".join(chunks)
        return self.body


async def send_response(send: Send, response: w.HTTPResponse) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": response.status,
            "headers": [(k.encode("latin-1"), v.encode("latin-1")) for k, v in response.headers.items()],
        }
    )
    await send({"type": "http.response.body", "body": response.body})


async def entrypoint(scope: Scope, receive: Receive, send: Send) -> None:
    if scope["method"].lower() == "post":
        if scope["path"] == "/.well-known/workflow/v1/flow":
            response = await workflow_entrypoint(ASGIRequest(scope, receive))
        elif scope["path"] == "/.well-known/workflow/v1/step":
            response = await step_entrypoint(ASGIRequest(scope, receive))
        else:
            response = w.HTTPResponse.json({"error": "Not found"}, status=404)
    else:
        response = w.HTTPResponse.json({"error": "Not found"}, status=404)
    await send_response(send, response)
