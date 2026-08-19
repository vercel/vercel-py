from __future__ import annotations

from pydantic import BaseModel

from vercel.queue import subscribe


class Order(BaseModel):
    order_id: str
    total_cents: int


@subscribe(topic="typed-orders")
async def handle_order(order: Order) -> None:
    print("Billing order", order.order_id, order.total_cents)
