from __future__ import annotations

from invoice_transport import Invoice, InvoiceFormTransport

from vercel.queue import Topic, asgi_app, subscribe

invoice_events = Topic[Invoice]("invoices", transport=InvoiceFormTransport())


@subscribe(topic=invoice_events, consumer_group=f"api/{__name__}.py")
async def record_invoice(invoice: Invoice) -> None:
    print("Recorded invoice", invoice.invoice_id, invoice.customer_id, invoice.total_cents)


app = asgi_app()
