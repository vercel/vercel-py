from __future__ import annotations

from invoice_transport import Invoice, InvoiceFormTransport

from vercel.queue import Topic
from vercel.queue.sync import QueueClient

invoice_events = Topic[Invoice]("invoices", transport=InvoiceFormTransport())


def main() -> None:
    invoice = Invoice(
        invoice_id="inv_123",
        customer_id="cus_456",
        total_cents=4200,
    )
    queue = QueueClient(region="iad1")
    message_id = queue.send(invoice_events, invoice)
    if message_id is not None:
        print(message_id)


if __name__ == "__main__":
    main()
