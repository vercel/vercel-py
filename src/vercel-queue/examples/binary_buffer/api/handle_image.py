from __future__ import annotations

from vercel.queue import Message, asgi_app, subscribe


@subscribe(topic="images", consumer_group=f"api/{__name__}.py")
def handle_image(message: Message[bytes]) -> None:
    print("Received image bytes", message.payload.hex())


app = asgi_app()
