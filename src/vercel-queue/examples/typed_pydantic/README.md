# Typed Pydantic Queue app

Deploy this directory as a Vercel app:

```bash
cd src/vercel-queue/examples/typed_pydantic
vc link
vc deploy
```

Send a message from an environment that has Vercel Queue credentials. Use
`--region iad1` for local sends outside Vercel:

```bash
uv run --project ../.. python -m vercel.queue send --region iad1 --topic typed-orders --json '{"order_id":"ord_123","total_cents":2500}'
```

Check the function logs:

```bash
vc logs <deployment-url>
```

Expected output includes:

```text
Billing order ord_123 2500
```

The push subscriber receives a Pydantic model and validates the JSON payload.
