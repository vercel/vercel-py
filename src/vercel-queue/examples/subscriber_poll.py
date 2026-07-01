from __future__ import annotations

import sys

if sys.version_info < (3, 11):
    raise SystemExit("subscriber_poll.py requires Python 3.11 or newer")

import asyncio

from vercel.queue import QueueClient, poll_and_handle, subscribe


async def main() -> None:
    processed = asyncio.Event()

    @subscribe(topic="orders", consumer_group="fulfillment")
    async def fulfill_order(order: dict[str, str]) -> None:
        print("fulfilled", order["order_id"])
        processed.set()

    queue = QueueClient(region="iad1")
    await queue.send(
        "orders",
        {"order_id": "ord_123", "action": "fulfill"},
        idempotency_key="subscriber-poll-ord-123",
    )

    async with asyncio.TaskGroup() as task_group:
        poller = task_group.create_task(poll_and_handle(fulfill_order, interval=1.0))
        try:
            await asyncio.wait_for(processed.wait(), timeout=5)
        finally:
            poller.cancel()


if __name__ == "__main__":
    asyncio.run(main())
