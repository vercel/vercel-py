# Custom Transport Queue app

Deploy this directory as a Vercel app:

```bash
cd src/vercel-queue/examples/custom_transport
vc link
vc deploy
```

Send a message from an environment that has Vercel Queue credentials. The
producer script uses the same `Topic` and custom transport as the subscriber:

```bash
uv run --project . python send_invoice.py
```

Check the function logs:

```bash
vc logs <deployment-url>
```

Expected output includes:

```text
Recorded invoice inv_123 cus_456 4200
```

The custom transport serializes an `Invoice` dataclass as
`application/x-www-form-urlencoded` bytes and deserializes the same format for
the push subscriber.
