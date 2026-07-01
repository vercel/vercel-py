from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from urllib.parse import parse_qs, urlencode


@dataclass(frozen=True)
class Invoice:
    invoice_id: str
    customer_id: str
    total_cents: int


class InvoiceFormTransport:
    content_type = "application/x-www-form-urlencoded"

    def serialize(self, value: Invoice) -> bytes:
        return urlencode({
            "invoice_id": value.invoice_id,
            "customer_id": value.customer_id,
            "total_cents": str(value.total_cents),
        }).encode("utf-8")

    async def deserialize(
        self,
        payload: AsyncIterator[bytes],
        *,
        content_type: str,
    ) -> Invoice:
        body = bytearray()
        async for chunk in payload:
            body.extend(chunk)

        parsed = parse_qs(body.decode("utf-8"), strict_parsing=True)
        return Invoice(
            invoice_id=_single(parsed, "invoice_id"),
            customer_id=_single(parsed, "customer_id"),
            total_cents=int(_single(parsed, "total_cents")),
        )


def _single(values: dict[str, list[str]], key: str) -> str:
    value = values[key]
    if len(value) != 1:
        raise ValueError(f"expected one {key} value")
    return value[0]
