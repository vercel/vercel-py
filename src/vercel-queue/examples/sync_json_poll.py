from __future__ import annotations

from vercel.queue import Topic
from vercel.queue.sync import QueueClient


def main() -> None:
    events = Topic[dict[str, str]]("events")

    queue = QueueClient(region="iad1")
    queue.send("events", {"type": "user.created", "user_id": "usr_123"})

    for delivery in queue.poll(events, "analytics"):
        with delivery as message:
            print(message.payload)


if __name__ == "__main__":
    main()
