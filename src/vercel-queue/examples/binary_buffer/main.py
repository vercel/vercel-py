from __future__ import annotations

from vercel.queue import Message, subscribe


@subscribe(topic="images")
def handle_image(message: Message[bytes]) -> None:
    print("Received image bytes", message.payload.hex())
