from __future__ import annotations

import asyncio

from vercel.queue import QueueClient, Topic


async def main() -> None:
    orders = Topic[dict[str, str]]("orders")

    queue = QueueClient(region="iad1")
    message_id = await queue.send(
        "orders",
        {"order_id": "ord_123", "action": "fulfill"},
        idempotency_key="ord_123",
    )

    async for delivery in queue.poll(orders, "fulfillment", lease_duration=120):
        async with delivery as message:
            print(message_id, message.payload, message.metadata.delivery_count)


if __name__ == "__main__":
    asyncio.run(main())
