from __future__ import annotations

from contextlib import suppress

import trio

from vercel.queue import QueueClient, poll_and_handle, subscribe


async def main() -> None:
    processed = trio.Event()

    @subscribe(topic="orders", consumer_group="fulfillment")
    async def fulfill_order(order: dict[str, str]) -> None:
        print("fulfilled", order["order_id"])
        processed.set()

    queue = QueueClient(region="iad1")
    await queue.send(
        "orders",
        {"order_id": "ord_123", "action": "fulfill"},
        idempotency_key="subscriber-poll-trio-ord-123",
    )

    async def poll() -> None:
        with suppress(trio.Cancelled):
            await poll_and_handle(fulfill_order, interval=1.0)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(poll)
        with trio.fail_after(5):
            await processed.wait()
        nursery.cancel_scope.cancel()


if __name__ == "__main__":
    trio.run(main)
